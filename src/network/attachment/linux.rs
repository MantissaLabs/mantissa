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
use std::net::{IpAddr, Ipv4Addr};
use uuid::Uuid;

use super::{container_iface_name, host_iface_name};

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

            self.handle
                .link()
                .set(LinkUnspec::new_with_index(container_index).mtu(mtu).build())
                .execute()
                .await
                .with_context(|| format!("failed to set mtu {mtu} on {container_if}"))?;
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

        let mac_bytes = parse_mac(mac)?;
        self.handle
            .link()
            .set(
                LinkUnspec::new_with_index(container_index)
                    .address(mac_bytes.clone())
                    .build(),
            )
            .execute()
            .await
            .with_context(|| format!("failed to assign mac {mac} to {container_if}"))?;

        let addr: IpAddr = assigned_ip
            .parse::<Ipv4Addr>()
            .map(IpAddr::V4)
            .context("invalid IPv4 address for attachment")?;

        self.handle
            .address()
            .add(container_index, addr, prefix.into())
            .replace()
            .execute()
            .await
            .with_context(|| {
                format!("failed to assign {assigned_ip}/{prefix} to {container_if}")
            })?;

        self.handle
            .link()
            .set(LinkUnspec::new_with_index(container_index).up().build())
            .execute()
            .await
            .with_context(|| format!("failed to bring {container_if} up"))?;

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
        self.program_fdb_entry(vxlan_index, &mac_bytes, dst)
            .await
            .with_context(|| format!("failed to program fdb entry {mac} -> {dst}"))?;

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
