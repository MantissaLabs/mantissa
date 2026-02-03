use std::cell::RefCell;
use std::rc::Rc;
use sysinfo::{CpuRefreshKind, Disks, System};

/// # Description:
///
/// This structure contains System wide informations about the machine
/// such as the operating systems details, hardware components, load, etc.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    sys: Rc<RefCell<System>>,
    pub info: Info,
}

impl Default for NodeInfo {
    fn default() -> Self {
        Self {
            sys: Rc::new(RefCell::new(System::new_all())),
            info: Info::default(),
        }
    }
}

/// # Description:
///
/// This structure contains System wide informations about the machine
/// such as the operating systems details, hardware components, load, etc.
#[derive(Clone, Debug, Default)]
pub struct Info {
    #[allow(dead_code)]
    pub device_ip: Option<String>,
    pub os_info: Option<OS>,
    pub hostname: Option<String>,
    pub cpu_info: Option<Cpu>,
    pub load_info: Option<Load>,
    pub mem_info: Option<Memory>,
    pub disk_info: Option<Disk>,
    pub gpu_info: Option<GpuInfo>,
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
    #[allow(dead_code)]
    pub used: u64,

    pub swap_total: u64,
    #[allow(dead_code)]
    pub swap_used: u64,
    pub swap_free: u64,
}

#[derive(Clone, Debug)]
pub struct Disk {
    pub total: u64,
    pub free: u64,
}

/// # Description:
///
/// Stores summary information for detected GPU devices on the host so the
/// scheduler can reason about accelerator availability.
#[derive(Clone, Debug)]
pub struct GpuInfo {
    pub vendor: String,
    pub devices: Vec<GpuDevice>,
}

/// # Description:
///
/// Describes a single GPU device as reported by the platform inventory.
#[derive(Clone, Debug)]
pub struct GpuDevice {
    pub index: u32,
    pub uuid: Option<String>,
    pub name: String,
    pub memory_total_bytes: u64,
    pub memory_free_bytes: u64,
    pub compute_capability: Option<String>,
}

impl NodeInfo {
    /// Refresh the cached `sysinfo::System` instance so subsequent reads use
    /// up-to-date views without rebuilding the underlying snapshot.
    fn refresh_system_state(&self) {
        let mut sys = self.sys.borrow_mut();
        sys.refresh_cpu_specifics(CpuRefreshKind::everything());
        sys.refresh_memory();
    }

    pub fn collect(&mut self) {
        self.refresh_system_state();
        self.get_cpu_frequency();
        self.get_cpu_info();
        self.get_disk_info();
        self.get_gpu_info();
        self.get_hostname();
        self.get_load_avg();
        self.get_memory_info();
        self.get_os_info();
    }

    pub fn new() -> Self {
        NodeInfo::default()
    }

    pub fn get_hostname(&mut self) {
        match System::host_name() {
            Some(hostname) => self.info.hostname = Some(hostname),
            None => self.info.hostname = Some(String::from("Unknown")),
        }
    }

    pub fn get_cpu_frequency(&self) -> u64 {
        let sys = self.sys.borrow();
        if let Some(cpu) = sys.cpus().iter().next() {
            cpu.frequency()
        } else {
            0
        }
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
        let sys = self.sys.borrow();
        let total = sys.total_memory();
        let free = sys.free_memory();
        let available = sys.available_memory();
        let used = sys.used_memory();
        let swap_total = sys.total_swap();
        let swap_used = sys.used_swap();
        let swap_free = sys.free_swap();

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

    /// Collect NVIDIA GPU inventory via NVML so scheduler slots can reflect
    /// accelerator capacity when available.
    pub fn get_gpu_info(&mut self) {
        self.info.gpu_info = collect_nvidia_gpus();
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
    pub fn get_cpu_info(&mut self) {
        let (brand, logical, physical) = {
            let mut sys = self.sys.borrow_mut();
            sys.refresh_cpu_specifics(CpuRefreshKind::everything());
            let cpus = sys.cpus();

            if cpus.is_empty() {
                (None, 0, 0)
            } else {
                let logical = cpus.len() as i32;
                let physical = System::physical_core_count().unwrap_or(cpus.len()) as i32;
                let brand = cpus.first().map(|cpu| cpu.brand().to_string());
                (brand, logical, physical)
            }
        };

        self.info.cpu_info = Some(Cpu {
            vendor: None,
            brand,
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

/// Collect NVIDIA GPUs using NVML on Linux. Returns `None` when NVML is
/// unavailable or no devices are detected so callers can fall back cleanly.
#[cfg(target_os = "linux")]
fn collect_nvidia_gpus() -> Option<GpuInfo> {
    use nvml_wrapper::Nvml;

    let nvml = Nvml::init().ok()?;
    let count = nvml.device_count().ok()?;
    if count == 0 {
        return None;
    }

    let mut devices = Vec::with_capacity(count as usize);
    for index in 0..count {
        let device = match nvml.device_by_index(index) {
            Ok(device) => device,
            Err(_) => continue,
        };

        let name = device.name().unwrap_or_else(|_| "Unknown".to_string());
        let uuid = device.uuid().ok();
        let memory = device.memory_info().ok();
        let (memory_total_bytes, memory_free_bytes) = match memory {
            Some(info) => (info.total, info.free),
            None => (0, 0),
        };

        // NVML compute capability reporting varies across driver versions; keep it optional.
        let compute_capability = None;

        devices.push(GpuDevice {
            index: index as u32,
            uuid,
            name,
            memory_total_bytes,
            memory_free_bytes,
            compute_capability,
        });
    }

    if devices.is_empty() {
        None
    } else {
        Some(GpuInfo {
            vendor: "nvidia".to_string(),
            devices,
        })
    }
}

/// Placeholder for non-Linux builds where NVML is unavailable.
#[cfg(not(target_os = "linux"))]
fn collect_nvidia_gpus() -> Option<GpuInfo> {
    None
}
