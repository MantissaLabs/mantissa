use anyhow::{Context, Result, anyhow};
use futures::{StreamExt, TryStreamExt};
use libc;
use rtnetlink::packet_core::{
    NLM_F_ACK, NLM_F_CREATE, NLM_F_REPLACE, NLM_F_REQUEST, NetlinkMessage, NetlinkPayload,
};
use rtnetlink::packet_route::neighbour::{
    NeighbourAddress, NeighbourAttribute, NeighbourFlags, NeighbourMessage, NeighbourState,
};
use rtnetlink::packet_route::{AddressFamily, RouteNetlinkMessage};
use rtnetlink::{Handle, LinkUnspec, LinkVeth};
use std::fs::File;
use std::net::{IpAddr, Ipv4Addr};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::spawn_blocking;
use tracing::debug;
use uuid::Uuid;

use super::{container_iface_name, host_iface_name};
use nix::sched::{CloneFlags, setns};

#[derive(Clone)]
pub struct AttachmentProvisioner {
    handle: Handle,
}

impl Default for AttachmentProvisioner {
    fn default() -> Self {
        Self::new().expect("attachment provisioner initialization")
    }
}

impl AttachmentProvisioner {
    pub fn new() -> Result<Self> {
        let (connection, handle, _) =
            rtnetlink::new_connection().context("failed to open rtnetlink connection")?;
        tokio::spawn(connection);
        Ok(Self { handle })
    }

    pub async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        let host_if = host_iface_name(attachment_id);
        Ok(self.link_index(&host_if).await?.is_some())
    }

    pub async fn ensure_attachment(
        &self,
        _network_id: Uuid,
        bridge_name: &str,
        mtu: u32,
        attachment_id: Uuid,
        container_pid: i32,
        assigned_ip: &str,
        prefix: u8,
        mac: &str,
    ) -> Result<()> {
        let host_if = host_iface_name(attachment_id);
        let container_if = container_iface_name(attachment_id);

        let host_index = match self.link_index(&host_if).await? {
            Some(index) => index,
            None => {
                self.create_veth(&host_if, &container_if).await?;
                self.link_index(&host_if)
                    .await?
                    .context("veth host interface missing after creation")?
            }
        };

        let container_index = self
            .link_index(&container_if)
            .await?
            .context("veth peer interface missing after creation")?;

        if mtu > 0 {
            self.handle
                .link()
                .set(LinkUnspec::new_with_index(host_index).mtu(mtu).build())
                .execute()
                .await
                .with_context(|| format!("failed to set mtu {mtu} on {host_if}"))?;
        }

        let bridge_index = self
            .link_index(bridge_name)
            .await?
            .context("bridge missing while configuring attachment")?;

        self.handle
            .link()
            .set(
                LinkUnspec::new_with_index(host_index)
                    .controller(bridge_index)
                    .build(),
            )
            .execute()
            .await
            .with_context(|| format!("failed to enslave {host_if} to {bridge_name}"))?;

        self.handle
            .link()
            .set(LinkUnspec::new_with_index(host_index).up().build())
            .execute()
            .await
            .with_context(|| format!("failed to bring {host_if} up"))?;

        let netns_pid: u32 = container_pid
            .try_into()
            .context("container pid is negative")?;

        self.handle
            .link()
            .set(
                LinkUnspec::new_with_index(container_index)
                    .setns_by_pid(netns_pid)
                    .build(),
            )
            .execute()
            .await
            .with_context(|| format!("failed to move {container_if} to pid {container_pid}"))?;

        self.configure_container_interface(
            container_if.clone(),
            mtu,
            assigned_ip.to_string(),
            prefix,
            mac.to_string(),
            container_pid,
        )
        .await
        .with_context(|| format!("configure container interface {container_if}"))?;

        Ok(())
    }

    async fn configure_container_interface(
        &self,
        iface: String,
        mtu: u32,
        assigned_ip: String,
        prefix: u8,
        mac: String,
        container_pid: i32,
    ) -> Result<()> {
        let assigned_addr = assigned_ip
            .parse::<Ipv4Addr>()
            .context("invalid IPv4 address for attachment")?;
        let mac_bytes = parse_mac(&mac)?;

        spawn_blocking(move || {
            configure_container_interface_blocking(
                iface,
                mtu,
                assigned_addr,
                prefix,
                mac_bytes,
                container_pid,
            )
        })
        .await
        .context("configure container interface task failed")??;

        Ok(())
    }

    pub async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()> {
        let host_if = host_iface_name(attachment_id);
        if let Some(index) = self.link_index(&host_if).await? {
            self.handle
                .link()
                .del(index)
                .execute()
                .await
                .with_context(|| format!("failed to delete interface {host_if}"))?;
        }
        Ok(())
    }

    pub async fn ensure_remote_fdb(
        &self,
        vxlan_name: &str,
        mac: &str,
        dst: std::net::IpAddr,
    ) -> Result<()> {
        let vxlan_index = self
            .link_index(vxlan_name)
            .await?
            .context("vxlan interface missing while programming fdb")?;

        let mac_bytes = parse_mac(mac)?;
        match self.program_fdb_entry(vxlan_index, &mac_bytes, dst).await {
            Ok(()) => {}
            Err(rtnetlink::Error::NetlinkError(message))
                if message.raw_code().abs() == libc::EOPNOTSUPP =>
            {
                debug!(
                    target: "network",
                    vxlan = vxlan_name,
                    mac,
                    dst = %dst,
                    "kernel rejected static fdb entry (unsupported); continuing"
                );
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to program fdb entry {mac} -> {dst}"));
            }
        }

        Ok(())
    }

    pub async fn remove_remote_fdb(
        &self,
        vxlan_name: &str,
        mac: &str,
        dst: std::net::IpAddr,
    ) -> Result<()> {
        let vxlan_index = match self.link_index(vxlan_name).await? {
            Some(idx) => idx,
            None => return Ok(()),
        };

        let mac_bytes = parse_mac(mac)?;

        if let Err(err) = self.delete_fdb_entry(vxlan_index, &mac_bytes, dst).await {
            warn_unless_not_found(err, || format!("remove fdb entry {mac} -> {dst}"));
        }

        Ok(())
    }

    pub async fn ensure_flood_entry(&self, vxlan_name: &str, dst: std::net::IpAddr) -> Result<()> {
        self.ensure_remote_fdb(vxlan_name, "00:00:00:00:00:00", dst)
            .await
    }

    pub async fn remove_flood_entry(&self, vxlan_name: &str, dst: std::net::IpAddr) -> Result<()> {
        self.remove_remote_fdb(vxlan_name, "00:00:00:00:00:00", dst)
            .await
    }

    async fn create_veth(&self, host_if: &str, container_if: &str) -> Result<()> {
        self.handle
            .link()
            .add(LinkVeth::new(host_if, container_if).build())
            .execute()
            .await
            .with_context(|| format!("failed to create veth {host_if}<->{container_if}"))?;
        Ok(())
    }

    async fn link_index(&self, name: &str) -> Result<Option<u32>> {
        let mut stream = self
            .handle
            .link()
            .get()
            .match_name(name.to_string())
            .execute();

        loop {
            match stream.try_next().await {
                Ok(Some(msg)) => return Ok(Some(msg.header.index)),
                Ok(None) => break,
                Err(rtnetlink::Error::NetlinkError(message)) => {
                    let raw = message.raw_code();
                    let errno = raw.abs();
                    if errno == libc::ENODEV || errno == libc::ENOENT {
                        tracing::debug!(
                            target: "task",
                            link = name,
                            errno,
                            raw_code = raw,
                            "link lookup returned ENODEV/ENOENT; treating as absent"
                        );
                        return Ok(None);
                    }
                    return Err(rtnetlink::Error::NetlinkError(message))
                        .context("query link state");
                }
                Err(err) => return Err(err).context("query link state"),
            }
        }

        Ok(None)
    }

    async fn program_fdb_entry(
        &self,
        vxlan_index: u32,
        mac: &[u8],
        dst: IpAddr,
    ) -> Result<(), rtnetlink::Error> {
        let mut message = NeighbourMessage::default();
        message.header.family = AddressFamily::Bridge;
        message.header.ifindex = vxlan_index;
        message.header.state = NeighbourState::Permanent;
        message.header.flags = NeighbourFlags::Own;

        message
            .attributes
            .push(NeighbourAttribute::LinkLocalAddress(mac.to_vec()));
        message
            .attributes
            .push(NeighbourAttribute::Destination(match dst {
                IpAddr::V4(v4) => NeighbourAddress::from(v4),
                IpAddr::V6(v6) => NeighbourAddress::from(v6),
            }));

        let mut request = NetlinkMessage::from(RouteNetlinkMessage::NewNeighbour(message));
        request.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_REPLACE;
        self.submit_request(request).await
    }

    async fn delete_fdb_entry(
        &self,
        vxlan_index: u32,
        mac: &[u8],
        dst: IpAddr,
    ) -> Result<(), rtnetlink::Error> {
        let mut message = NeighbourMessage::default();
        message.header.family = AddressFamily::Bridge;
        message.header.ifindex = vxlan_index;
        message.header.flags = NeighbourFlags::Own;

        message
            .attributes
            .push(NeighbourAttribute::LinkLocalAddress(mac.to_vec()));
        message
            .attributes
            .push(NeighbourAttribute::Destination(match dst {
                IpAddr::V4(v4) => NeighbourAddress::from(v4),
                IpAddr::V6(v6) => NeighbourAddress::from(v6),
            }));

        let mut request = NetlinkMessage::from(RouteNetlinkMessage::DelNeighbour(message));
        request.header.flags = NLM_F_REQUEST | NLM_F_ACK;
        self.submit_request(request).await
    }

    /// Send a raw rtnetlink message through the shared handle and drain the
    /// response stream so that ACK or error messages are handled immediately.
    async fn submit_request(
        &self,
        request: NetlinkMessage<RouteNetlinkMessage>,
    ) -> Result<(), rtnetlink::Error> {
        let mut handle = self.handle.clone();
        let mut responses = handle.request(request)?;
        while let Some(message) = responses.next().await {
            if let NetlinkPayload::Error(err) = message.payload {
                return Err(rtnetlink::Error::NetlinkError(err));
            }
        }
        Ok(())
    }
}

fn parse_mac(mac: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(6);
    for part in mac.split(':') {
        if part.len() != 2 {
            return Err(anyhow!("invalid mac address {mac}"));
        }
        bytes.push(u8::from_str_radix(part, 16).context("invalid mac component")?);
    }
    if bytes.len() != 6 {
        return Err(anyhow!("invalid mac address {mac}"));
    }
    Ok(bytes)
}

fn warn_unless_not_found(err: rtnetlink::Error, context: impl FnOnce() -> String) {
    use tracing::warn;
    if let rtnetlink::Error::NetlinkError(ref message) = err {
        if message
            .code
            .map(|code| code.get() == -libc::ENOENT)
            .unwrap_or(false)
        {
            return;
        }
    }
    warn!(target: "network", "{}: {err}", context());
}

fn configure_container_interface_blocking(
    iface: String,
    mtu: u32,
    assigned_ip: Ipv4Addr,
    prefix: u8,
    mac_bytes: Vec<u8>,
    container_pid: i32,
) -> Result<()> {
    let host_ns = File::open("/proc/self/ns/net").context("open host network namespace")?;
    let target_ns = File::open(format!("/proc/{container_pid}/ns/net"))
        .context("open container network namespace")?;

    unsafe { setns(&target_ns, CloneFlags::empty()) }
        .context("enter container network namespace")?;

    let configure_result =
        configure_interface_in_current_ns(&iface, mtu, assigned_ip, prefix, mac_bytes);

    let restore_result =
        unsafe { setns(&host_ns, CloneFlags::empty()) }.context("restore host network namespace");

    if let Err(err) = configure_result {
        // Ensure we restore even when configuration fails.
        restore_result?;
        return Err(err);
    }

    restore_result?;
    configure_result
}

fn configure_interface_in_current_ns(
    iface: &str,
    mtu: u32,
    assigned_ip: Ipv4Addr,
    prefix: u8,
    mac_bytes: Vec<u8>,
) -> Result<()> {
    let rt = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .context("create runtime for container network namespace operations")?;

    rt.block_on(async move {
        let (connection, handle, _) = rtnetlink::new_connection()
            .context("open rtnetlink connection in container namespace")?;
        tokio::spawn(connection);

        let mut links = handle.link().get().match_name(iface.to_string()).execute();

        let link = links
            .try_next()
            .await
            .context("query container interface state")?
            .context("container interface missing after namespace move")?;
        let index = link.header.index;

        if mtu > 0 {
            handle
                .link()
                .set(LinkUnspec::new_with_index(index).mtu(mtu).build())
                .execute()
                .await
                .context("set container interface mtu")?;
        }

        handle
            .link()
            .set(
                LinkUnspec::new_with_index(index)
                    .address(mac_bytes.clone())
                    .build(),
            )
            .execute()
            .await
            .context("assign container interface mac")?;

        handle
            .address()
            .add(index, IpAddr::V4(assigned_ip), prefix.into())
            .replace()
            .execute()
            .await
            .context("assign overlay ip to container interface")?;

        handle
            .link()
            .set(LinkUnspec::new_with_index(index).up().build())
            .execute()
            .await
            .context("bring container overlay interface up")?;

        // A connected route for the prefix is added automatically when the
        // address is configured. Nothing further required here.
        Ok(())
    })
}
