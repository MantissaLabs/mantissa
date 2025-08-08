use sysinfo::{CpuRefreshKind, Disks, RefreshKind, System};

/// # Description:
///
/// This structure contains System wide informations about the machine
/// such as the operating systems details, hardware components, load, etc.
pub struct NodeInfo {
    sys: System,
    pub info: Info,
}

/// # Description:
///
/// This structure contains System wide informations about the machine
/// such as the operating systems details, hardware components, load, etc.
#[derive(Clone, Debug)]
pub struct Info {
    pub device_ip: Option<String>,
    pub os_info: Option<OS>,
    pub hostname: Option<String>,
    pub cpu_info: Option<Cpu>,
    pub load_info: Option<Load>,
    pub mem_info: Option<Memory>,
    pub disk_info: Option<Disk>,
}

/// # Description:
///
/// This structure defines the client handle for a network member.
#[derive(Clone, Debug)]
pub struct OS {
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
}

/// # Description:
///
/// Holds general CPU informations.
#[derive(Clone, Debug)]
pub struct Cpu {
    /// CPU vendor string, for example *GenuineIntel*.
    pub vendor: Option<String>,

    /// Brand string, for example *Intel(R) Core(TM) i5-2410M CPU @
    /// 2.30GHz*.
    pub brand: Option<String>,

    /// Brief CPU codename, such as *Sandy Bridge (Core i5)*.
    pub codename: Option<String>,

    /// CPU frequency (in MHz).
    pub frequency: Option<u64>,

    /// Number of physical cores of the current CPU.
    pub num_cores: i32,

    /// Number of logical processors (may include HyperThreading or such).
    pub num_logical_cpus: i32,

    /// Total number of logical processors.
    pub total_logical_cpus: Option<i32>,

    /// L1 data cache size in kB. `Some(0)` if the CPU lacks cache, `None`
    /// if it couldn't be determined.
    pub l1_data_cache: Option<i32>,

    /// L1 instruction cache size in kB. `Some(0)` if the CPU lacks cache,
    /// `None` if it couldn't be determined.
    pub l1_instruction_cache: Option<i32>,

    /// L2 cache size in kB. `Some(0)` if the CPU lacks L2 cache, `None` if
    /// it couldn't be determined.
    pub l2_cache: Option<i32>,

    /// L3 cache size in kB. `Some(0)` if the CPU lacks L3 cache, `None` if
    /// it couldn't be determined.
    pub l3_cache: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct Load {
    /// Average load within one minute.
    pub one: f64,
    /// Average load within five minutes.
    pub five: f64,
    /// Average load within fifteen minutes.
    pub fifteen: f64,
}

#[derive(Clone, Debug)]
pub struct Memory {
    pub total: u64,
    pub free: u64,
    pub available: u64,
    pub used: u64,

    pub swap_total: u64,
    pub swap_used: u64,
    pub swap_free: u64,
}

#[derive(Clone, Debug)]
pub struct Disk {
    pub total: u64,
    pub free: u64,
}

impl NodeInfo {
    pub fn collect(&mut self) {
        self.get_cpu_frequency();
        self.get_cpu_info();
        self.get_disk_info();
        self.get_hostname();
        self.get_load_avg();
        self.get_memory_info();
        self.get_os_info();
    }

    pub fn new() -> Self {
        let sys = System::new_all();

        NodeInfo {
            sys,
            info: Info {
                device_ip: None,
                load_info: None,
                mem_info: None,
                disk_info: None,
                os_info: None,
                hostname: None,
                cpu_info: None,
            },
        }
    }

    pub fn get_hostname(&mut self) {
        match System::host_name() {
            Some(hostname) => self.info.hostname = Some(hostname),
            None => self.info.hostname = Some(String::from("Unknown")),
        }
    }

    pub fn get_cpu_frequency(&self) -> u64 {
        for cpu in self.sys.cpus() {
            return cpu.frequency();
        }

        0
    }

    pub fn get_load_avg(&mut self) {
        // static method returning the 1/5/15-minute load averages
        let avg = System::load_average();
        self.info.load_info = Some(Load {
            one: avg.one,
            five: avg.five,
            fifteen: avg.fifteen,
        });
    }

    pub fn get_memory_info(&mut self) {
        let total = self.sys.total_memory();
        let free = self.sys.free_memory();
        let available = self.sys.available_memory();
        let used = self.sys.used_memory();
        let swap_total = self.sys.total_swap();
        let swap_used = self.sys.used_swap();
        let swap_free = self.sys.free_swap();

        self.info.mem_info = Some(Memory {
            total,
            free,
            available,
            used,
            swap_total,
            swap_used,
            swap_free,
        });
    }

    pub fn get_disk_info(&mut self) {
        let mut total = 0;
        let mut free = 0;

        let disks = Disks::new_with_refreshed_list();
        for disk in &disks {
            total += disk.total_space();
            free += disk.available_space();
        }

        self.info.disk_info = Some(Disk { total, free });
    }

    pub fn get_os_info(&mut self) {
        let os_name = System::name().unwrap_or(String::from("Unknown"));
        let os_version = System::os_version().unwrap_or(String::from("Unknown"));
        let kernel_version = System::kernel_version().unwrap_or(String::from("Unknown"));

        self.info.os_info = Some(OS {
            os_name,
            os_version,
            kernel_version,
        });
    }

    /// Returns the CPU specs of the machine with the model, number of
    /// cores...
    ///
    /// # Remarks
    ///
    /// This only works if `libcpuid` is present on the machine, otherwise
    /// we return `None` and ignore the specs for further use of the
    /// delegate.
    ///
    /// # Examples
    ///
    /// ```
    /// match get_cpu_info() {
    ///     Some(info) => println!("cpu brand: {}", info.brand),
    ///     None => println!("can't collect cpu info on the machine"),
    /// }
    /// ```
    pub fn get_cpu_info(&mut self) {
        let mut sys = System::new_with_specifics(
            RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()),
        );

        sys.refresh_cpu_all();

        let cpus = sys.cpus();
        let model = cpus.first().map(|cpu| cpu.brand().to_string()).unwrap();
        let logical: i32 = cpus.len() as i32;
        let physical: i32 = System::physical_core_count().unwrap_or(cpus.len()) as i32;

        self.info.cpu_info = Some(Cpu {
            vendor: None,
            brand: Some(model),
            codename: None,
            frequency: None,
            num_cores: physical,
            num_logical_cpus: logical,
            total_logical_cpus: Some(logical),
            l1_data_cache: None,
            l1_instruction_cache: None,
            l2_cache: None,
            l3_cache: None,
        })
    }
}
