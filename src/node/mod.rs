use crate::info_capnp::info as SystemInfo;
use crate::node::id::new_node_id_v7;
use crate::node::info::NodeInfo;
use capnp::Error;
use capnp::capability::Promise;
use capnp::message::Builder;
use protocol::node;

pub mod address;
pub mod id;
pub mod identity;
pub mod info;

// NodeState contains all of the node transitions during its lifetime.
// Change in state could occur when receiving messages from other peers,
// or performing actions like joining or leaving the cluster.
pub enum NodeState {
    // Node is initializing hardware, setting up network interfaces, etc.
    Initializing,

    // Node is ready but has not joined any cluster yet
    Bootstrapped,

    // Node is attempting to join a cluster
    JoiningCluster,

    // Node has joined the cluster but has not synchronized its state
    Synchronizing,

    // Node is fully synchronized and participating in the cluster
    Active,

    // Node is active but is currently running at its resource limits
    ResourceConstrained,

    // Node is in the process of leaving the cluster
    LeavingCluster,

    // Node has left the cluster but is still running
    LeftCluster,

    // Node is disconnecting from the network, releasing resources, etc.
    ShuttingDown,

    // Node is fully shut down
    Stopped,

    // Node is isolated from the rest of the cluster (network partition, etc.)
    NetworkIsolated,

    // Node is in a state of recovering from failures or inconsistencies
    Recovering,

    // Node is in maintenance mode, not participating in scheduling but part of cluster
    Maintenance,
}

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
    fn info(
        &mut self,
        _params: node::InfoParams,
        mut results: node::InfoResults,
    ) -> Promise<(), Error> {
        println!("Collecting system information...");

        let mut builder = Builder::new_default();

        let info = self.system_info.info.clone();

        {
            let builder = &mut builder;
            let mut system = builder.init_root::<SystemInfo::Builder>();

            if info.hostname.is_some() {
                system.set_hostname(info.hostname.unwrap());
            }

            // Operating system
            {
                let mut os = system.reborrow().init_os();

                if info.os_info.is_some() {
                    let os_info = info.os_info.unwrap();
                    os.set_name(&os_info.os_name);
                    os.set_version(&os_info.os_version);
                    os.set_kernel_version(&os_info.kernel_version)
                }
            }

            // Load average
            {
                let mut load = system.reborrow().init_load();

                if info.load_info.is_some() {
                    let load_info = info.load_info.unwrap();
                    load.set_one(load_info.one);
                    load.set_five(load_info.five);
                    load.set_fifteen(load_info.fifteen);
                }
            }

            // CPU
            {
                let mut cpu = system.reborrow().init_cpu();

                if info.cpu_info.is_some() {
                    let cpu_info = info.cpu_info.unwrap();
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

                if info.mem_info.is_some() {
                    let mem_info = info.mem_info.unwrap();
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

                if info.disk_info.is_some() {
                    let disk_info = info.disk_info.unwrap();
                    disk.set_total(disk_info.total);
                    disk.set_free(disk_info.free);
                }
            }
        }

        match builder.get_root::<SystemInfo::Builder>() {
            Ok(system_reader) => match results.get().set_info(system_reader.into_reader()) {
                Ok(_) => Promise::ok(()),
                Err(e) => Promise::err(e),
            },
            Err(e) => Promise::err(e),
        }
    }
}
