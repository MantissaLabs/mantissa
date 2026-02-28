use crate::info_capnp::info as SystemInfo;
use crate::node::id::new_node_id_v7;
use crate::node::info::NodeInfo;
use capnp::Error;
use capnp::message::Builder;
use protocol::node;
use tracing::info;

pub mod address;
pub mod id;
pub mod identity;
pub mod info;

pub type NodeId = uuid::Uuid;

/// This structure defines the delegate in charge of booking slots
/// running tasks on the machine.
#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeId,
    pub system_info: NodeInfo,
    // engine: Rc<Engine>,
}

impl Default for Node {
    fn default() -> Self {
        Node {
            id: new_node_id_v7(),
            system_info: NodeInfo::new(),
        }
    }
}

impl Node {
    /// Creates a new node instance.
    pub fn new() -> Node {
        Default::default()
    }

    /// Collects system wide informations.
    pub fn collect_system_info(&mut self) -> &mut Node {
        self.system_info.collect();
        self
    }
}

impl node::Server for Node {
    /// Returns the information on a host.
    ///
    /// This method returns general informations such as load average,
    /// CPU specs, amount of Memory, Disk capacity, etc. to print on cli.
    async fn info(
        self: std::rc::Rc<Self>,
        _params: node::InfoParams,
        mut results: node::InfoResults,
    ) -> Result<(), Error> {
        info!(target: "node", "Collecting system information...");

        let mut builder = Builder::new_default();

        let info = self.system_info.info.clone();

        {
            let builder = &mut builder;
            let mut system = builder.init_root::<SystemInfo::Builder>();

            if let Some(hostname) = info.hostname {
                system.set_hostname(hostname);
            }

            // Operating system
            {
                let mut os = system.reborrow().init_os();

                if let Some(os_info) = info.os_info {
                    os.set_name(&os_info.os_name);
                    os.set_version(&os_info.os_version);
                    os.set_kernel_version(&os_info.kernel_version)
                }
            }

            // Load average
            {
                let mut load = system.reborrow().init_load();

                if let Some(load_info) = info.load_info {
                    load.set_one(load_info.one);
                    load.set_five(load_info.five);
                    load.set_fifteen(load_info.fifteen);
                }
            }

            // CPU
            {
                let mut cpu = system.reborrow().init_cpu();

                if let Some(cpu_info) = info.cpu_info {
                    cpu.set_vendor(cpu_info.vendor.unwrap_or(String::from("Unknown")));
                    cpu.set_brand(cpu_info.brand.unwrap_or(String::from("Unknown")));
                    cpu.set_codename(cpu_info.codename.unwrap_or(String::from("Unknown")));
                    cpu.set_frequency(cpu_info.frequency.unwrap_or(0));
                    cpu.set_num_cores(cpu_info.num_cores);
                    cpu.set_logical_cpus(cpu_info.num_logical_cpus);
                    cpu.set_total_logical_cpus(cpu_info.total_logical_cpus.unwrap_or(0));
                    cpu.set_l1_data_cache(cpu_info.l1_data_cache.unwrap_or(0));
                    cpu.set_l1_instruction_cache(cpu_info.l1_instruction_cache.unwrap_or(0));
                    cpu.set_l2_cache(cpu_info.l2_cache.unwrap_or(0));
                    cpu.set_l3_cache(cpu_info.l3_cache.unwrap_or(0));
                }
            }

            // Memory
            {
                let mut mem = system.reborrow().init_memory();

                if let Some(mem_info) = info.mem_info {
                    mem.set_total(mem_info.total);
                    mem.set_free(mem_info.free);
                    mem.set_avail(mem_info.available);
                    mem.set_swap_total(mem_info.swap_total);
                    mem.set_swap_free(mem_info.swap_free);
                }
            }

            // Disk
            {
                let mut disk = system.reborrow().init_disk();

                if let Some(disk_info) = info.disk_info {
                    disk.set_total(disk_info.total);
                    disk.set_free(disk_info.free);
                }
            }

            // GPU
            {
                let mut gpu = system.reborrow().init_gpu();
                if let Some(gpu_info) = info.gpu_info {
                    gpu.set_vendor(&gpu_info.vendor);
                    let mut devices = gpu.reborrow().init_devices(gpu_info.devices.len() as u32);
                    for (idx, device) in gpu_info.devices.iter().enumerate() {
                        let mut entry = devices.reborrow().get(idx as u32);
                        entry.set_index(device.index);
                        entry.set_uuid(device.uuid.as_deref().unwrap_or(""));
                        entry.set_name(&device.name);
                        entry.set_memory_total_bytes(device.memory_total_bytes);
                        entry.set_memory_free_bytes(device.memory_free_bytes);
                        entry.set_compute_capability(
                            device.compute_capability.as_deref().unwrap_or(""),
                        );
                        entry.set_pci_bus_id(device.pci_bus_id.as_deref().unwrap_or(""));
                    }
                } else {
                    gpu.set_vendor("");
                    gpu.reborrow().init_devices(0);
                }
            }
        }

        match builder.get_root::<SystemInfo::Builder>() {
            Ok(system_reader) => {
                results.get().set_info(system_reader.into_reader())?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}
