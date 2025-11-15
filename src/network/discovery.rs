use crate::network::registry::NetworkRegistry;
use crate::network::types::{NetworkAttachmentState, NetworkSpecValue};
use crate::services::registry::ServiceRegistry;
use crate::services::types::ServiceSpecValue;
use crate::store::task_store::TaskStore;
use crate::task::container::ContainerState;
use crate::task::types::TaskValue;
use anyhow::{Context, Result};
use crdt_store::uuid_key::UuidKey;
use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

const SERVICE_ZONE_SUFFIX: &str = "svc.mantissa";
const SERVICE_TTL_SECS: u32 = 5;

#[derive(Clone)]
pub struct ServiceDiscovery {
    registry: NetworkRegistry,
    tasks: TaskStore,
    services: ServiceRegistry,
    servers: Arc<AsyncMutex<HashMap<Uuid, DnsServerHandle>>>,
}

struct DnsServerHandle {
    resolver_ip: Ipv4Addr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl ServiceDiscovery {
    pub fn new(registry: NetworkRegistry, tasks: TaskStore, services: ServiceRegistry) -> Self {
        Self {
            registry,
            tasks,
            services,
            servers: Arc::new(AsyncMutex::new(HashMap::new())),
        }
    }

    pub async fn ensure_network(
        &self,
        spec: &NetworkSpecValue,
        resolver_ip: Option<Ipv4Addr>,
    ) -> Result<()> {
        let Some(resolver_ip) = resolver_ip else {
            self.teardown_network(spec.id).await?;
            return Ok(());
        };

        {
            let guard = self.servers.lock().await;
            if let Some(existing) = guard.get(&spec.id) {
                if existing.resolver_ip == resolver_ip {
                    return Ok(());
                }
            }
        }

        self.teardown_network(spec.id).await?;

        let server = spawn_dns_server(
            self.registry.clone(),
            self.tasks.clone(),
            self.services.clone(),
            spec.id,
            spec.name.clone(),
            resolver_ip,
        )
        .await?;

        let mut guard = self.servers.lock().await;
        guard.insert(spec.id, server);
        Ok(())
    }

    pub async fn teardown_network(&self, network_id: Uuid) -> Result<()> {
        let handle = {
            let mut guard = self.servers.lock().await;
            guard.remove(&network_id)
        };

        if let Some(mut handle) = handle {
            if let Some(tx) = handle.shutdown.take() {
                let _ = tx.send(());
            }
            tokio::spawn(async move {
                if let Err(err) = handle.task.await {
                    warn!(
                        target: "network",
                        network = %network_id,
                        "service discovery loop exited with error: {err:#}"
                    );
                }
            });
        }

        Ok(())
    }
}

async fn spawn_dns_server(
    registry: NetworkRegistry,
    tasks: TaskStore,
    services: ServiceRegistry,
    network_id: Uuid,
    network_name: String,
    resolver_ip: Ipv4Addr,
) -> Result<DnsServerHandle> {
    let bind_addr = SocketAddr::new(IpAddr::V4(resolver_ip), 53);
    let socket = UdpSocket::bind(bind_addr)
        .await
        .with_context(|| format!("bind resolver socket {bind_addr}"))?;
    info!(
        target: "network",
        network = %network_id,
        resolver = %resolver_ip,
        "started service discovery listener"
    );

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let task_registry = registry.clone();
    let service_registry = services.clone();
    let server = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => {
                    break;
                }
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, peer)) => {
                            if let Err(err) = handle_datagram(
                                &socket,
                                &buf[..len],
                                peer,
                                &task_registry,
                                &tasks,
                                &service_registry,
                                network_id,
                                &network_name,
                            ).await {
                                warn!(
                                    target: "network",
                                    network = %network_id,
                                    "service discovery failed to handle udp datagram: {err:#}"
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                target: "network",
                                network = %network_id,
                                "service discovery socket recv failed: {err}"
                            );
                        }
                    }
                }
            }
        }
        info!(
            target: "network",
            network = %network_id,
            "service discovery listener stopped"
        );
    });

    Ok(DnsServerHandle {
        resolver_ip,
        shutdown: Some(shutdown_tx),
        task: server,
    })
}

async fn handle_datagram(
    socket: &UdpSocket,
    payload: &[u8],
    peer: SocketAddr,
    registry: &NetworkRegistry,
    tasks: &TaskStore,
    services: &ServiceRegistry,
    network_id: Uuid,
    network_name: &str,
) -> Result<()> {
    let request = match Message::from_vec(payload) {
        Ok(message) => message,
        Err(err) => {
            debug!(
                target: "network",
                network = %network_id,
                "discarding malformed dns query: {err}"
            );
            return Ok(());
        }
    };

    let mut response = Message::new();
    response.set_id(request.id());
    response.set_message_type(MessageType::Response);
    response.set_op_code(request.op_code());
    response.set_recursion_desired(request.recursion_desired());
    response.set_recursion_available(false);
    response.set_authoritative(true);

    for query in request.queries() {
        response.add_query(query.clone());
    }

    let mut answers_added = false;
    let mut saw_nxdomain = false;
    let mut saw_nodata = false;
    let mut saw_notimp = false;

    let service_specs = match services.list() {
        Ok(specs) => specs,
        Err(err) => {
            warn!(
                target: "network",
                network = %network_id,
                "failed to load service registry while answering dns query: {err}"
            );
            Vec::new()
        }
    };
    let template_index = build_task_template_index(&service_specs);

    for query in request.queries() {
        match answer_query(
            query,
            registry,
            tasks,
            &template_index,
            network_id,
            network_name,
        )
        .await?
        {
            LookupOutcome::Records(records) => {
                for record in records {
                    response.add_answer(record);
                    answers_added = true;
                }
            }
            LookupOutcome::NxDomain => saw_nxdomain = true,
            LookupOutcome::NoData => saw_nodata = true,
            LookupOutcome::NotImplemented => saw_notimp = true,
        }
    }

    if answers_added {
        response.set_response_code(ResponseCode::NoError);
    } else if saw_nodata {
        response.set_response_code(ResponseCode::NoError);
    } else if saw_notimp {
        response.set_response_code(ResponseCode::NotImp);
    } else if saw_nxdomain {
        response.set_response_code(ResponseCode::NXDomain);
    } else {
        response.set_response_code(ResponseCode::ServFail);
    }

    let bytes = response.to_vec().context("encode dns response")?;
    socket
        .send_to(&bytes, peer)
        .await
        .with_context(|| format!("send dns response to {}", peer))?;
    Ok(())
}

enum LookupOutcome {
    Records(Vec<Record>),
    NxDomain,
    NoData,
    NotImplemented,
}

async fn answer_query(
    query: &Query,
    registry: &NetworkRegistry,
    tasks: &TaskStore,
    template_index: &HashMap<Uuid, (String, String)>,
    network_id: Uuid,
    network_name: &str,
) -> Result<LookupOutcome> {
    if query.query_type() == RecordType::AAAA {
        return Ok(LookupOutcome::NoData);
    }
    if query.query_type() != RecordType::A {
        return Ok(LookupOutcome::NotImplemented);
    }

    let Some(service_name) = extract_service_label(query.name(), network_name) else {
        return Ok(LookupOutcome::NxDomain);
    };

    let addresses =
        resolve_service_ips(registry, tasks, template_index, network_id, &service_name).await?;
    if addresses.is_empty() {
        return Ok(LookupOutcome::NxDomain);
    }

    let records = addresses
        .into_iter()
        .map(|addr| {
            Record::from_rdata(
                query.name().clone(),
                SERVICE_TTL_SECS,
                RData::A(addr.into()),
            )
        })
        .collect();
    Ok(LookupOutcome::Records(records))
}

fn extract_service_label(name: &Name, network_name: &str) -> Option<String> {
    let mut labels = Vec::new();
    for raw in name.iter() {
        let lower = raw.to_ascii_lowercase();
        let label = match String::from_utf8(lower) {
            Ok(text) => text,
            Err(_) => return None,
        };
        labels.push(label);
    }
    let suffix_labels: Vec<&str> = SERVICE_ZONE_SUFFIX.split('.').collect();
    if labels.len() != suffix_labels.len() + 2 {
        return None;
    }
    for expected in suffix_labels.iter().rev() {
        if labels.pop()?.as_str() != *expected {
            return None;
        }
    }
    let network_label = labels.pop()?;
    if network_label != network_name.to_ascii_lowercase() {
        return None;
    }
    labels.pop()
}

async fn resolve_service_ips(
    registry: &NetworkRegistry,
    tasks: &TaskStore,
    template_index: &HashMap<Uuid, (String, String)>,
    network_id: Uuid,
    service_name: &str,
) -> Result<Vec<Ipv4Addr>> {
    let attachments = registry
        .list_attachments(Some(network_id))
        .context("list attachments for discovery")?;
    let mut cache: HashMap<Uuid, Option<TaskValue>> = HashMap::new();
    let mut results = Vec::new();

    for attachment in attachments {
        if attachment.state != NetworkAttachmentState::Ready {
            continue;
        }
        let Some(ip_text) = &attachment.assigned_ip else {
            continue;
        };
        let task_entry = cache
            .entry(attachment.task_id)
            .or_insert_with(|| load_task(tasks, attachment.task_id));
        let Some(task) = task_entry else {
            continue;
        };
        if task.state != ContainerState::Running {
            continue;
        }
        let template_match = attachment
            .template_name
            .as_deref()
            .map(|template| template.eq_ignore_ascii_case(service_name))
            .or_else(|| {
                task.service_metadata
                    .as_ref()
                    .map(|meta| meta.template.eq_ignore_ascii_case(service_name))
            })
            .or_else(|| {
                template_index
                    .get(&attachment.task_id)
                    .map(|(_, template)| template.eq_ignore_ascii_case(service_name))
            })
            .unwrap_or_else(|| task.name.eq_ignore_ascii_case(service_name));
        if !template_match {
            continue;
        }
        match ip_text.parse::<Ipv4Addr>() {
            Ok(addr) => results.push(addr),
            Err(err) => warn!(
                target: "network",
                network = %network_id,
                task = %task.id,
                "invalid attachment ip {}: {err}",
                ip_text
            ),
        }
    }

    Ok(results)
}

fn load_task(tasks: &TaskStore, id: Uuid) -> Option<TaskValue> {
    let key = UuidKey::from(id);
    let snapshot = tasks.get_snapshot(&key).ok()??;
    snapshot.as_slice().last().cloned()
}

fn build_task_template_index(specs: &[ServiceSpecValue]) -> HashMap<Uuid, (String, String)> {
    let mut index = HashMap::new();
    for spec in specs {
        let mut ids = spec.task_ids.iter();
        for template in &spec.tasks {
            for _ in 0..template.replicas {
                let Some(task_id) = ids.next() else { break };
                index.insert(*task_id, (spec.service_name.clone(), template.name.clone()));
            }
        }
    }
    index
}
