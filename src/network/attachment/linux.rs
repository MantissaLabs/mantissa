use anyhow::{Context, Result, anyhow};
use futures::{StreamExt, TryStreamExt};
use libc;
use rtnetlink::packet_core::{
    NLM_F_ACK, NLM_F_APPEND, NLM_F_CREATE, NLM_F_REPLACE, NLM_F_REQUEST, NetlinkMessage,
    NetlinkPayload,
};
use rtnetlink::packet_route::neighbour::{
    NeighbourAddress, NeighbourAttribute, NeighbourFlags, NeighbourMessage, NeighbourState,
};
use rtnetlink::packet_route::{AddressFamily, RouteNetlinkMessage, route::RouteType};
use rtnetlink::{Handle, LinkUnspec, LinkVeth};
use std::fs::File;
use std::net::{IpAddr, Ipv4Addr};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::spawn_blocking;
use tracing::debug;
use uuid::Uuid;

use super::{
    AttachmentProvisionerApi, AttachmentProvisioningRequest, container_iface_name, host_iface_name,
};
use async_trait::async_trait;
use nix::sched::{CloneFlags, setns};

#[derive(Clone)]
pub struct AttachmentProvisioner {
    handle: Option<Handle>,
}

impl Default for AttachmentProvisioner {
    fn default() -> Self {
        Self::new().expect("attachment provisioner initialization")
    }
}

impl AttachmentProvisioner {
    pub fn new() -> Result<Self> {
        if unsafe { libc::geteuid() } != 0 {
            debug!(
                target: "network",
                "running unprivileged; using stub attachment provisioner"
            );
            return Ok(Self::unavailable());
        }

        let (connection, handle, _) =
            rtnetlink::new_connection().context("failed to open rtnetlink connection")?;
        tokio::spawn(connection);
        Ok(Self {
            handle: Some(handle),
        })
    }

    pub fn unavailable() -> Self {
        Self { handle: None }
    }

    fn handle(&self) -> Option<&Handle> {
        self.handle.as_ref()
    }

    pub async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        let Some(handle) = self.handle() else {
            return Ok(false);
        };
        let host_if = host_iface_name(attachment_id);
        Ok(self.link_index(handle, &host_if).await?.is_some())
    }

    pub async fn ensure_attachment(
        &self,
        request: &AttachmentProvisioningRequest<'_>,
    ) -> Result<()> {
        let Some(handle) = self.handle() else {
            debug!(
                target: "task",
                attachment = %request.attachment_id,
                "skipping attachment provisioning; rtnetlink unavailable"
            );
            return Ok(());
        };

        let host_if = host_iface_name(request.attachment_id);
        let container_if = container_iface_name(request.attachment_id);

        let bridge_name = request.bridge_name;

        let host_index = match self.link_index(handle, &host_if).await? {
            Some(index) => index,
            None => {
                self.create_veth(handle, &host_if, &container_if).await?;
                self.link_index(handle, &host_if)
                    .await?
                    .context("veth host interface missing after creation")?
            }
        };

        let container_index = self
            .link_index(handle, &container_if)
            .await?
            .context("veth peer interface missing after creation")?;

        if request.mtu > 0 {
            handle
                .link()
                .set(
                    LinkUnspec::new_with_index(host_index)
                        .mtu(request.mtu)
                        .build(),
                )
                .execute()
                .await
                .with_context(|| format!("failed to set mtu {} on {host_if}", request.mtu))?;
        }

        let bridge_index = self
            .link_index(handle, bridge_name)
            .await?
            .context("bridge missing while configuring attachment")?;

        handle
            .link()
            .set(
                LinkUnspec::new_with_index(host_index)
                    .controller(bridge_index)
                    .build(),
            )
            .execute()
            .await
            .with_context(|| format!("failed to enslave {host_if} to {bridge_name}"))?;

        handle
            .link()
            .set(LinkUnspec::new_with_index(host_index).up().build())
            .execute()
            .await
            .with_context(|| format!("failed to bring {host_if} up"))?;

        let netns_pid: u32 = request
            .container_pid
            .try_into()
            .context("container pid is negative")?;

        handle
            .link()
            .set(
                LinkUnspec::new_with_index(container_index)
                    .setns_by_pid(netns_pid)
                    .build(),
            )
            .execute()
            .await
            .with_context(|| {
                format!(
                    "failed to move {container_if} to pid {}",
                    request.container_pid
                )
            })?;

        self.configure_container_interface(
            container_if.clone(),
            request.mtu,
            request.assigned_ip.to_string(),
            request.prefix,
            request.mac.to_string(),
            request.container_pid,
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
        let Some(handle) = self.handle() else {
            return Ok(());
        };
        let host_if = host_iface_name(attachment_id);
        if let Some(index) = self.link_index(handle, &host_if).await?
            && let Err(err) = handle.link().del(index).execute().await
        {
            match err {
                rtnetlink::Error::NetlinkError(message) => {
                    let raw = message.raw_code();
                    let errno = raw.abs();
                    if errno == libc::ENODEV || errno == libc::ENOENT || errno == libc::ENXIO {
                        debug!(
                            target: "task",
                            link = host_if,
                            errno,
                            raw_code = raw,
                            "interface already removed while deleting; ignoring"
                        );
                    } else {
                        return Err(rtnetlink::Error::NetlinkError(message))
                            .with_context(|| format!("failed to delete interface {host_if}"));
                    }
                }
                other => {
                    return Err(other)
                        .with_context(|| format!("failed to delete interface {host_if}"));
                }
            }
        }
        Ok(())
    }

    pub async fn ensure_remote_fdb(
        &self,
        vxlan_name: &str,
        mac: &str,
        dst: std::net::IpAddr,
    ) -> Result<bool> {
        let Some(handle) = self.handle() else {
            return Ok(false);
        };
        let vxlan_index = self
            .link_index(handle, vxlan_name)
            .await?
            .context("vxlan interface missing while programming fdb")?;

        let mac_bytes = parse_mac(mac)?;
        match self
            .program_fdb_entry(handle, vxlan_index, &mac_bytes, dst)
            .await
        {
            Ok(()) => {
                debug!(
                    target: "network",
                    vxlan = vxlan_name,
                    mac,
                    dst = %dst,
                    "programmed static vxlan fdb entry"
                );
                Ok(true)
            }
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
                Ok(false)
            }
            Err(err) => {
                Err(err).with_context(|| format!("failed to program fdb entry {mac} -> {dst}"))
            }
        }
    }

    pub async fn remove_remote_fdb(
        &self,
        vxlan_name: &str,
        mac: &str,
        dst: std::net::IpAddr,
    ) -> Result<()> {
        let Some(handle) = self.handle() else {
            return Ok(());
        };
        let vxlan_index = match self.link_index(handle, vxlan_name).await? {
            Some(idx) => idx,
            None => return Ok(()),
        };

        let mac_bytes = parse_mac(mac)?;

        if let Err(err) = self
            .delete_fdb_entry(handle, vxlan_index, &mac_bytes, dst)
            .await
        {
            warn_unless_not_found(err, || format!("remove fdb entry {mac} -> {dst}"));
        }

        Ok(())
    }

    pub async fn ensure_flood_entry(
        &self,
        vxlan_name: &str,
        dst: std::net::IpAddr,
    ) -> Result<bool> {
        self.ensure_remote_fdb(vxlan_name, "00:00:00:00:00:00", dst)
            .await
    }

    pub async fn remove_flood_entry(&self, vxlan_name: &str, dst: std::net::IpAddr) -> Result<()> {
        self.remove_remote_fdb(vxlan_name, "00:00:00:00:00:00", dst)
            .await
    }

    /// Read the VXLAN FDB entries currently programmed in the kernel for this device.
    ///
    /// Returns `(mac, dst)` tuples only for entries that carry an explicit destination, which is
    /// exactly the shape Mantissa manages for remote unicast and flood forwarding.
    pub async fn list_remote_fdb(&self, vxlan_name: &str) -> Result<Vec<(String, IpAddr)>> {
        let Some(handle) = self.handle() else {
            return Ok(Vec::new());
        };
        let vxlan_index = match self.link_index(handle, vxlan_name).await? {
            Some(idx) => idx,
            None => return Ok(Vec::new()),
        };

        let mut stream = handle.neighbours().get().execute();
        let mut entries = Vec::new();

        while let Some(msg) = stream.try_next().await.context("list vxlan fdb entries")? {
            if msg.header.ifindex != vxlan_index {
                continue;
            }

            let mut mac: Option<String> = None;
            let mut dst: Option<IpAddr> = None;
            for attr in msg.attributes {
                match attr {
                    NeighbourAttribute::LinkLocalAddress(value) => {
                        mac = format_mac_bytes(&value);
                    }
                    NeighbourAttribute::Destination(NeighbourAddress::Inet(addr)) => {
                        dst = Some(IpAddr::V4(addr));
                    }
                    NeighbourAttribute::Destination(NeighbourAddress::Inet6(addr)) => {
                        dst = Some(IpAddr::V6(addr));
                    }
                    _ => {}
                }
            }

            if let (Some(mac), Some(dst)) = (mac, dst) {
                entries.push((mac, dst));
            }
        }

        Ok(entries)
    }

    async fn create_veth(&self, handle: &Handle, host_if: &str, container_if: &str) -> Result<()> {
        handle
            .link()
            .add(LinkVeth::new(host_if, container_if).build())
            .execute()
            .await
            .with_context(|| format!("failed to create veth {host_if}<->{container_if}"))?;
        Ok(())
    }

    async fn link_index(&self, handle: &Handle, name: &str) -> Result<Option<u32>> {
        let mut stream = handle.link().get().match_name(name.to_string()).execute();

        match stream.try_next().await {
            Ok(Some(msg)) => Ok(Some(msg.header.index)),
            Ok(None) => Ok(None),
            Err(rtnetlink::Error::NetlinkError(message)) => {
                let raw = message.raw_code();
                let errno = raw.abs();
                if errno == libc::ENODEV || errno == libc::ENOENT {
                    debug!(
                        target: "task",
                        link = name,
                        errno,
                        raw_code = raw,
                        "link lookup returned ENODEV/ENOENT; treating as absent"
                    );
                    Ok(None)
                } else {
                    Err(rtnetlink::Error::NetlinkError(message)).context("query link state")
                }
            }
            Err(err) => Err(err).context("query link state"),
        }
    }

    async fn program_fdb_entry(
        &self,
        handle: &Handle,
        vxlan_index: u32,
        mac: &[u8],
        dst: IpAddr,
    ) -> Result<(), rtnetlink::Error> {
        let is_flood = mac.iter().all(|byte| *byte == 0);
        if is_flood {
            let mut message = NeighbourMessage::default();
            message.header.family = AddressFamily::Bridge;
            message.header.ifindex = vxlan_index;
            message.header.state = NeighbourState::Permanent;
            message.header.flags = NeighbourFlags::Own;
            message.header.kind = RouteType::Unspec;

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
            request.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_APPEND;
            return self.submit_request(request).await;
        }

        if let IpAddr::V6(v6) = dst {
            // The rtnetlink neighbour builder is historically brittle for bridge FDB entries that
            // carry IPv6 destinations. Build the netlink message directly so VXLAN forwarding works
            // reliably when the underlay is IPv6 (e.g. VXLAN-over-WireGuard).
            let mut message = NeighbourMessage::default();
            message.header.family = AddressFamily::Bridge;
            message.header.ifindex = vxlan_index;
            message.header.state = NeighbourState::Permanent;
            message.header.flags = NeighbourFlags::Own;
            message.header.kind = RouteType::Unspec;

            message
                .attributes
                .push(NeighbourAttribute::LinkLocalAddress(mac.to_vec()));
            message
                .attributes
                .push(NeighbourAttribute::Destination(NeighbourAddress::from(v6)));

            let mut request = NetlinkMessage::from(RouteNetlinkMessage::NewNeighbour(message));
            request.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_REPLACE;
            return self.submit_request(request).await;
        }

        handle
            .neighbours()
            .add_bridge(vxlan_index, mac)
            .flags(NeighbourFlags::Own)
            .destination(dst)
            .replace()
            .execute()
            .await
    }

    async fn delete_fdb_entry(
        &self,
        _handle: &Handle,
        vxlan_index: u32,
        mac: &[u8],
        dst: IpAddr,
    ) -> Result<(), rtnetlink::Error> {
        let mut message = NeighbourMessage::default();
        message.header.family = AddressFamily::Bridge;
        message.header.ifindex = vxlan_index;

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

    async fn submit_request(
        &self,
        request: NetlinkMessage<RouteNetlinkMessage>,
    ) -> Result<(), rtnetlink::Error> {
        let Some(handle) = self.handle() else {
            return Ok(());
        };
        let mut responses = handle.clone().request(request)?;
        while let Some(message) = responses.next().await {
            if let NetlinkPayload::Error(err) = message.payload {
                return Err(rtnetlink::Error::NetlinkError(err));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AttachmentProvisionerApi for AttachmentProvisioner {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        AttachmentProvisioner::attachment_exists(self, attachment_id).await
    }

    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        AttachmentProvisioner::ensure_attachment(self, request).await
    }

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()> {
        AttachmentProvisioner::teardown_attachment(self, attachment_id).await
    }

    async fn ensure_remote_fdb(&self, vxlan_name: &str, mac: &str, dst: IpAddr) -> Result<bool> {
        AttachmentProvisioner::ensure_remote_fdb(self, vxlan_name, mac, dst).await
    }

    async fn remove_remote_fdb(&self, vxlan_name: &str, mac: &str, dst: IpAddr) -> Result<()> {
        AttachmentProvisioner::remove_remote_fdb(self, vxlan_name, mac, dst).await
    }

    async fn ensure_flood_entry(&self, vxlan_name: &str, dst: IpAddr) -> Result<bool> {
        AttachmentProvisioner::ensure_flood_entry(self, vxlan_name, dst).await
    }

    async fn remove_flood_entry(&self, vxlan_name: &str, dst: IpAddr) -> Result<()> {
        AttachmentProvisioner::remove_flood_entry(self, vxlan_name, dst).await
    }

    async fn list_remote_fdb(&self, vxlan_name: &str) -> Result<Vec<(String, IpAddr)>> {
        AttachmentProvisioner::list_remote_fdb(self, vxlan_name).await
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

fn format_mac_bytes(mac: &[u8]) -> Option<String> {
    if mac.len() != 6 {
        return None;
    }
    Some(format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
    ))
}

fn warn_unless_not_found(err: rtnetlink::Error, context: impl FnOnce() -> String) {
    use tracing::warn;
    if let rtnetlink::Error::NetlinkError(ref message) = err
        && message
            .code
            .map(|code| code.get() == -libc::ENOENT)
            .unwrap_or(false)
    {
        return;
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

    setns(&target_ns, CloneFlags::empty()).context("enter container network namespace")?;

    let configure_result =
        configure_interface_in_current_ns(&iface, mtu, assigned_ip, prefix, mac_bytes);

    let restore_result =
        setns(&host_ns, CloneFlags::empty()).context("restore host network namespace");

    if let Err(err) = configure_result {
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
            .add(index, IpAddr::V4(assigned_ip), prefix)
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

        Ok(())
    })
}
