use sys_info::*;

/// # Description:
///
/// This structure contains System wide informations about the machine
/// such as the operating systems details, hardware components, load, etc.
#[derive(Clone)]
pub struct System {
    pub device_ip: &'static str,
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
#[derive(Clone, Default)]
pub struct OS {
    pub os_release: String,
    pub os_kind: String,
}

/// # Description:
///
/// Holds general CPU informations.
#[derive(Clone, Default)]
pub struct Cpu {
    /// CPU vendor string, for example *GenuineIntel*.
    pub vendor: String,

    /// Brand string, for example *Intel(R) Core(TM) i5-2410M CPU @
    /// 2.30GHz*.
    pub brand: String,

    /// Brief CPU codename, such as *Sandy Bridge (Core i5)*.
    pub codename: String,

    /// CPU frequency (in MHz).
    pub frequency: Option<i32>,

    /// Number of physical cores of the current CPU.
    pub num_cores: i32,

    /// Number of logical processors (may include HyperThreading or such).
    pub num_logical_cpus: i32,

    /// Total number of logical processors.
    pub total_logical_cpus: i32,

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

#[derive(Clone, Default)]
pub struct Load {
    /// Average load within one minute.
    pub one: f64,
    /// Average load within five minutes.
    pub five: f64,
    /// Average load within fifteen minutes.
    pub fifteen: f64,
}

#[derive(Clone, Default)]
pub struct Memory {
    pub total: u64,
    pub free: u64,
    pub avail: u64,

    pub buffers: u64,
    pub cached: u64,

    pub swap_total: u64,
    pub swap_free: u64,
}

#[derive(Clone, Default)]
pub struct Disk {
    pub total: u64,
    pub free: u64,
}

impl Default for System {
    fn default() -> Self {
        System {
            device_ip: "",
            os_info: None,
            hostname: None,
            cpu_info: None,
            load_info: None,
            mem_info: None,
            disk_info: None,
        }
    }
}

impl System {
    pub fn collect(&mut self) {
        self.get_cpu_frequency();
        self.get_cpu_info();
        self.get_disk_info();
        self.get_hostname();
        self.get_load_avg();
        self.get_memory_info();
        self.get_os_info();
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
        if !::cpuid::is_present() {
            println!("libcpuid is not installed on the machine, cannot collet cpu specs..");
            self.cpu_info = None;
            return;
        }

        match ::cpuid::identify() {
            Ok(info) => {
                self.cpu_info = Some(Cpu {
                    vendor: info.vendor,
                    brand: info.brand,
                    codename: info.codename,
                    frequency: self.get_cpu_frequency(),
                    num_cores: info.num_cores,
                    num_logical_cpus: info.num_logical_cpus,
                    total_logical_cpus: info.total_logical_cpus,
                    l1_data_cache: info.l1_data_cache,
                    l1_instruction_cache: info.l1_instruction_cache,
                    l2_cache: info.l2_cache,
                    l3_cache: info.l3_cache,
                })
            }
            Err(_) => self.cpu_info = None,
        }
    }

    /// Gets the CPU frequency of the machine.
    ///
    /// # Remarks
    ///
    /// This only works if `libcpuid` is present on the machine, otherwise
    /// we return `None` and ignore the frequency for further use of the
    /// delegate.
    ///
    /// # Examples
    ///
    /// ```
    /// match get_cpu_frequency() {
    ///     Some(frequency) => println!("cpu frequency: {}", frequency),
    ///     None => println!("can't collect cpu frequency on the machine"),
    /// }
    /// ```
    pub fn get_cpu_frequency(&self) -> Option<i32> {
        if !::cpuid::is_present() {
            println!("libcpuid is not present on the machine, cannot collet cpu frequency..");
            return None;
        }

        ::cpuid::clock_frequency()
    }

    /// Gets the average load of the machine.
    ///
    /// # Examples
    ///
    /// ```
    /// match get_load() {
    ///     Some(load) => println!("avg load: {}", load.one),
    ///     None => println!("can't collect load average on the machine"),
    /// }
    /// ```
    pub fn get_load_avg(&mut self) {
        match loadavg() {
            Ok(load) => {
                self.load_info = Some(Load {
                    one: load.one,
                    five: load.five,
                    fifteen: load.fifteen,
                })
            }
            Err(_) => self.load_info = None,
        }
    }

    /// Gets the memory usage of the machine.
    ///
    /// # Examples
    ///
    /// ```
    /// match get_memory_info() {
    ///     Some(mem) => println!("available memory: {}", mem.avail),
    ///     None => println!("can't collect memory usage on the machine"),
    /// }
    /// ```
    pub fn get_memory_info(&mut self) {
        match mem_info() {
            Ok(mem) => {
                self.mem_info = Some(Memory {
                    avail: mem.avail,
                    buffers: mem.buffers,
                    cached: mem.cached,
                    free: mem.free,
                    swap_free: mem.swap_free,
                    swap_total: mem.swap_total,
                    total: mem.total,
                })
            }
            Err(_) => self.mem_info = None,
        }
    }

    /// Gets the disk usage of the machine.
    ///
    /// # Examples
    ///
    /// ```
    /// match get_disk_info() {
    ///     Some(disk) => println!("free disk space: {}", disk.free),
    ///     None => println!("can't collect disk usage on the machine"),
    /// }
    /// ```
    pub fn get_disk_info(&mut self) {
        match disk_info() {
            Ok(disk) => {
                self.disk_info = Some(Disk {
                    free: disk.free,
                    total: disk.total,
                })
            }
            Err(_) => self.disk_info = None,
        }
    }

    /// Get the operating system informations.
    ///
    /// # Examples
    ///
    /// ```
    /// match get_os_info() {
    ///     Some(os) => println!("operating system type: {}", os.type),
    ///     None => println!("can't collect operating system informations"),
    /// }
    /// ```
    pub fn get_os_info(&mut self) {
        let release: String;
        let kind: String;

        match os_release() {
            Ok(r) => release = r,
            Err(_) => release = String::new(),
        }

        match os_type() {
            Ok(k) => kind = k,
            Err(_) => kind = String::new(),
        }

        self.os_info = Some(OS {
            os_release: release,
            os_kind: kind,
        })
    }

    /// Get the hostname of the machine.
    ///
    /// # Examples
    ///
    /// ```
    /// match get_hostname() {
    ///     Some(hostname) => println!("hostname: {}", hostname),
    ///     None => println!("can't find hostname for the machine"),
    /// }
    /// ```
    pub fn get_hostname(&mut self) {
        match hostname() {
            Ok(hostname) => self.hostname = Some(hostname),
            Err(_) => self.hostname = None,
        }
    }
}
