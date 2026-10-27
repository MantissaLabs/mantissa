#![allow(clippy::unwrap_used)]

use crate::common;
use crate::common::privileged_networking::{
    PrivilegedTestGuard, create_privileged_node, privileged_networking_enabled,
};
use crate::{cleanup_wireguard_interface, require_wireguard_kernel_support, test_peer_value};
use futures::TryStreamExt;
use mantissa::network::wireguard::{MANTISSA_WIREGUARD_IFNAME, ensure_wireguard_underlay};
use mantissa::topology::peers::WireGuardPeerValue;
use mantissa_store::uuid_key::UuidKey;
use rtnetlink::packet_route::{
    AddressFamily,
    address::AddressHeaderFlags,
    route::{RouteAttribute, RouteHeader, RouteMessage, RouteProtocol, RouteType},
    rule::RuleMessage,
};
use rtnetlink::{Handle, IpVersion, LinkUnspec, LinkVeth, RouteMessageBuilder};
use std::collections::HashSet;
use std::fs::File;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::fd::AsRawFd;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

/// Opens a netlink socket in the calling thread's namespace and drives its replies asynchronously.
fn netlink_handle() -> std::io::Result<Handle> {
    let (connection, handle, _) = rtnetlink::new_connection()?;
    tokio::spawn(connection);
    Ok(handle)
}

/// Resolves an interface index while draining the reply stream before later netlink operations.
async fn link_index(handle: &Handle, name: &str) -> Result<Option<u32>, rtnetlink::Error> {
    let links: Vec<_> = handle
        .link()
        .get()
        .match_name(name.to_string())
        .execute()
        .try_collect()
        .await?;
    Ok(links.first().map(|link| link.header.index))
}

/// Finds exact main-table host routes without treating a broader subnet route as a match.
async fn host_routes(
    handle: &Handle,
    address: IpAddr,
) -> Result<Vec<RouteMessage>, rtnetlink::Error> {
    let mut query = RouteMessage::default();
    query.header.address_family = if address.is_ipv4() {
        AddressFamily::Inet
    } else {
        AddressFamily::Inet6
    };
    let routes: Vec<_> = handle.route().get(query).execute().try_collect().await?;

    Ok(routes
        .into_iter()
        .filter(|route| {
            let table = route
                .attributes
                .iter()
                .find_map(|attribute| match attribute {
                    RouteAttribute::Table(table) => Some(*table),
                    _ => None,
                })
                .unwrap_or(u32::from(route.header.table));
            table == u32::from(RouteHeader::RT_TABLE_MAIN)
                && route.header.destination_prefix_length
                    == if address.is_ipv4() { 32 } else { 128 }
                && route
                    .attributes
                    .contains(&RouteAttribute::Destination(address.into()))
        })
        .collect())
}

/// Removes the test's physical link and endpoint routes, including routes left by a failed apply.
struct UnderlayLink {
    name: String,
}

impl Drop for UnderlayLink {
    /// Uses a separate reactor so cleanup finishes even while unwinding the test's async runtime.
    fn drop(&mut self) {
        let name = self.name.clone();
        let worker = thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async {
                let Ok(handle) = netlink_handle() else {
                    return;
                };
                for address in ["192.0.2.10", "198.51.100.10", "fd00:feed::10"] {
                    if let Ok(routes) = host_routes(&handle, address.parse().unwrap()).await {
                        for route in routes {
                            let _ = handle.route().del(route).execute().await;
                        }
                    }
                }
                if let Ok(Some(index)) = link_index(&handle, &name).await {
                    let _ = handle.link().del(index).execute().await;
                }
            });
        });
        let _ = worker.join();
    }
}

/// Assigns fixture addresses and enables their veth endpoint without waiting for IPv6 DAD timers.
async fn configure_link(handle: &Handle, name: &str, addresses: &[(&str, u8)]) -> u32 {
    let index = link_index(handle, name)
        .await
        .unwrap()
        .expect("fixture veth exists");

    for (address, prefix) in addresses {
        let address: IpAddr = address.parse().unwrap();
        let mut request = handle.address().add(index, address, *prefix);
        if address.is_ipv6() {
            request.message_mut().header.flags |= AddressHeaderFlags::Nodad;
        }
        request.execute().await.expect("assign fixture address");
    }

    handle
        .link()
        .set(LinkUnspec::new_with_index(index).up().build())
        .execute()
        .await
        .expect("enable fixture veth");
    index
}

/// Creates remote sockets and netlink state in their own namespace to exercise physical routing.
async fn underlay_listeners(handle: &Handle) -> (UnderlayLink, [TcpListener; 3]) {
    let suffix = &Uuid::new_v4().simple().to_string()[..8];
    let link = UnderlayLink {
        name: format!("wg-h-{suffix}"),
    };
    let peer_name = format!("wg-p-{suffix}");
    let worker_peer_name = peer_name.clone();
    let (namespace_tx, namespace_rx) = tokio::sync::oneshot::channel();
    let (configure_tx, configure_rx) = mpsc::channel();

    // Open the namespace descriptor and reactor after unshare. Netlink sockets and
    // listeners must belong to the peer namespace, while the host runtime stays put.
    let worker = thread::spawn(move || {
        assert_eq!(unsafe { libc::unshare(libc::CLONE_NEWNET) }, 0);
        namespace_tx
            .send(File::open("/proc/thread-self/ns/net").unwrap())
            .unwrap();
        configure_rx.recv().unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let peer_handle = netlink_handle().unwrap();
                configure_link(
                    &peer_handle,
                    &worker_peer_name,
                    &[
                        ("192.0.2.10", 24),
                        ("198.51.100.10", 32),
                        ("fd00:feed::10", 64),
                    ],
                )
                .await;
                ["192.0.2.10:0", "198.51.100.10:0", "[fd00:feed::10]:0"].map(|address| {
                    let listener =
                        std::net::TcpListener::bind(address).expect("remote TCP listener");
                    listener.set_nonblocking(true).unwrap();
                    listener
                })
            })
    });

    let peer_namespace = namespace_rx.await.unwrap();
    handle
        .link()
        .add(LinkVeth::new(&link.name, &peer_name).build())
        .execute()
        .await
        .expect("create fixture veth pair");
    let peer_index = link_index(handle, &peer_name).await.unwrap().unwrap();
    handle
        .link()
        .set(
            LinkUnspec::new_with_index(peer_index)
                .setns_by_fd(peer_namespace.as_raw_fd())
                .build(),
        )
        .execute()
        .await
        .expect("move remote veth into peer namespace");
    let index = configure_link(
        handle,
        &link.name,
        &[("192.0.2.1", 24), ("fd00:feed::1", 64)],
    )
    .await;
    handle
        .route()
        .add(
            RouteMessageBuilder::<Ipv4Addr>::new()
                .destination_prefix("198.51.100.10".parse().unwrap(), 32)
                .gateway("192.0.2.10".parse().unwrap())
                .output_interface(index)
                .build(),
        )
        .execute()
        .await
        .expect("install existing gateway route");
    configure_tx.send(()).unwrap();

    let listeners =
        tokio::task::spawn_blocking(move || worker.join().expect("configure peer namespace"))
            .await
            .unwrap()
            .map(|listener| TcpListener::from_std(listener).unwrap());
    (link, listeners)
}

/// Captures the selected endpoint route while excluding volatile kernel cache counters.
async fn endpoint_route(handle: &Handle, address: IpAddr) -> RouteMessage {
    let query = RouteMessageBuilder::<IpAddr>::new()
        .destination_prefix(address, if address.is_ipv4() { 32 } else { 128 })
        .unwrap()
        .table_id(u32::from(RouteHeader::RT_TABLE_UNSPEC))
        .protocol(RouteProtocol::Unspec)
        .kind(RouteType::Unspec)
        .build();
    let mut replies: Vec<_> = handle
        .route()
        .get(query)
        .execute()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(replies.len(), 1, "endpoint lookup should return one route");
    let mut route = replies.pop().unwrap();
    route
        .attributes
        .retain(|attribute| !matches!(attribute, RouteAttribute::CacheInfo(_)));
    route
}

/// Checks the exact IPv6 host route so a default route cannot hide a missing tunnel route.
async fn has_interface_route(handle: &Handle, address: Ipv6Addr, index: u32) -> bool {
    host_routes(handle, address.into())
        .await
        .unwrap()
        .iter()
        .any(|route| route.attributes.contains(&RouteAttribute::Oif(index)))
}

/// Snapshots both policy rule lists to verify that WireGuard leaves host routing policy intact.
async fn policy_rules(handle: &Handle) -> [Vec<RuleMessage>; 2] {
    let ipv4 = handle.rule().get(IpVersion::V4).execute().try_collect();
    let ipv6 = handle.rule().get(IpVersion::V6).execute().try_collect();
    let (ipv4, ipv6) = tokio::try_join!(ipv4, ipv6).unwrap();
    [ipv4, ipv6]
}

/// Opens a new connection after each routing change so a cached TCP path cannot hide a regression.
async fn assert_endpoint_connects(listener: &TcpListener) {
    let address = listener.local_addr().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        client.expect("connect to peer over existing host route");
        accepted.expect("accept connection in peer namespace");
    })
    .await
    .expect("fresh peer TCP connection should survive WireGuard reconciliation");
}

local_test!(
    wireguard_preserves_endpoint_routes_and_repairs_tunnel_routes,
    {
        if !privileged_networking_enabled("WireGuard routing") {
            return;
        }

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = true;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = false;
            config.network.nodeport.enabled = false;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });
        require_wireguard_kernel_support().await;
        cleanup_wireguard_interface().await;

        let mut node = create_privileged_node().await;
        node.stop_runtime_background_tasks_for_test().await;
        let handle = netlink_handle().unwrap();
        let (_underlay, listeners) = underlay_listeners(&handle).await;
        let remote_peer_id = Uuid::new_v4();
        let desired_peers = HashSet::from([remote_peer_id]);
        let remote_tunnel = mantissa_net::wireguard::wireguard_tunnel_ipv6(remote_peer_id);
        let rules = policy_rules(&handle).await;
        let mut previous = None;

        for listener in &listeners {
            let endpoint = listener.local_addr().unwrap();
            let route_before = endpoint_route(&handle, endpoint.ip()).await;
            assert_endpoint_connects(listener).await;

            node.peers
                .upsert(
                    &UuidKey::from(remote_peer_id),
                    test_peer_value(
                        &endpoint.to_string(),
                        WireGuardPeerValue {
                            public_key: [7; 32],
                            port: endpoint.port(),
                            enabled: true,
                        },
                    ),
                )
                .await
                .expect("publish peer's physical endpoint");
            let state =
                ensure_wireguard_underlay(&node.registry, node.id, &desired_peers, previous)
                    .await
                    .expect("configure WireGuard with existing endpoint routing");
            assert_endpoint_connects(listener).await;
            assert_eq!(endpoint_route(&handle, endpoint.ip()).await, route_before);
            let index = link_index(&handle, MANTISSA_WIREGUARD_IFNAME)
                .await
                .unwrap()
                .unwrap();
            assert!(has_interface_route(&handle, remote_tunnel, index).await);

            let kernel_peer = {
                use defguard_wireguard_rs::{Kernel, WGApi, WireguardInterfaceApi};
                let host = WGApi::<Kernel>::new(MANTISSA_WIREGUARD_IFNAME)
                    .unwrap()
                    .read_interface_data()
                    .expect("read configured WireGuard peers");
                host.peers
                    .get(&defguard_wireguard_rs::key::Key::new([7; 32]))
                    .expect("peer's encryption configuration remains present")
                    .clone()
            };
            assert_eq!(kernel_peer.endpoint, Some(endpoint));
            previous = Some(state);
        }

        // Repair missing routes without forcing an interface refresh, and prune retired
        // tunnel addresses while preserving unrelated routes on the same interface.
        let stale_tunnel = mantissa_net::wireguard::wireguard_tunnel_ipv6(Uuid::new_v4());
        let foreign_route: Ipv6Addr = "fd00:beef::1".parse().unwrap();
        let index = link_index(&handle, MANTISSA_WIREGUARD_IFNAME)
            .await
            .unwrap()
            .unwrap();
        for route in host_routes(&handle, remote_tunnel.into()).await.unwrap() {
            handle
                .route()
                .del(route)
                .execute()
                .await
                .expect("remove required tunnel route");
        }
        for address in [stale_tunnel, foreign_route] {
            handle
                .route()
                .add(
                    RouteMessageBuilder::<Ipv6Addr>::new()
                        .destination_prefix(address, 128)
                        .output_interface(index)
                        .build(),
                )
                .execute()
                .await
                .expect("seed stale and unrelated interface routes");
        }
        let state = ensure_wireguard_underlay(&node.registry, node.id, &desired_peers, previous)
            .await
            .expect("repair and prune only Mantissa tunnel routes");
        assert!(has_interface_route(&handle, remote_tunnel, index).await);
        assert!(
            host_routes(&handle, stale_tunnel.into())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(has_interface_route(&handle, foreign_route, index).await);

        ensure_wireguard_underlay(&node.registry, node.id, &HashSet::new(), Some(state))
            .await
            .expect("remove routes for peers outside the desired scope");
        assert!(
            host_routes(&handle, remote_tunnel.into())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(policy_rules(&handle).await, rules);

        cleanup_wireguard_interface().await;
    }
);
