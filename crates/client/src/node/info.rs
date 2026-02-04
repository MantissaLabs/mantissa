use crate::{config::ClientConfig, connection};
use anyhow::Result;

pub async fn info(cfg: &ClientConfig) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_node_request();
    let node = request.send().pipeline.get_node();
    let request = node.info_request();

    let response = request.send().promise.await?;

    let info = response.get()?.get_info()?;

    println!("Hostname: {:?}", info.get_hostname()?);

    let os = info.get_os()?;
    println!("Operating System:");
    println!("  name: {:?}", os.get_name()?);
    println!("  version: {:?}", os.get_version()?);
    println!("  kernel_version: {:?}", os.get_kernel_version()?);

    let cpu = info.get_cpu()?;
    println!("CPU:");
    println!("  vendor: {:?}", cpu.get_vendor()?);
    println!("  brand: {:?}", cpu.get_brand()?);
    println!("  codename: {:?}", cpu.get_codename()?);
    println!("  frequency (MHz): {}", cpu.get_frequency());
    println!("  cores: {}", cpu.get_num_cores());
    println!("  logical cpus: {}", cpu.get_logical_cpus());
    println!("  total logical cpus: {}", cpu.get_total_logical_cpus());
    println!("  L1 data cache: {}", cpu.get_l1_data_cache());
    println!("  L1 instruction cache: {}", cpu.get_l1_instruction_cache());
    println!("  L2 cache: {}", cpu.get_l2_cache());
    println!("  L3 cache: {}", cpu.get_l3_cache());

    let load = info.get_load()?;
    println!("Load Average:");
    println!(
        "  {} / {} / {}",
        load.get_one(),
        load.get_five(),
        load.get_fifteen(),
    );

    let mem = info.get_memory()?;
    println!("Memory (Kb):");
    println!("  total: {}", mem.get_total());
    println!("  free: {}", mem.get_free());
    println!("  available: {}", mem.get_avail());
    println!("  buffers: {}", mem.get_buffers());
    println!("  cached: {}", mem.get_cached());
    println!("  swap total: {}", mem.get_swap_total());
    println!("  swap free: {}", mem.get_swap_free());

    let disk = info.get_disk()?;
    println!("Disk (Kb):");
    println!("  total: {}", disk.get_total());
    println!("  free: {}", disk.get_free());

    let gpu = info.get_gpu()?;
    let devices = gpu.get_devices()?;
    if devices.is_empty() {
        println!("GPU:");
        println!("  no GPU device detected");
    } else {
        println!("GPU:");
        let vendor = gpu.get_vendor()?.to_str()?.to_string();
        if !vendor.is_empty() {
            println!("  vendor: {vendor}");
        }
        for device in devices.iter() {
            println!(
                "  - index: {}",
                device.get_index(),
            );
            let name = device.get_name()?.to_str()?.to_string();
            if !name.is_empty() {
                println!("    name: {name}");
            }
            let uuid = device.get_uuid()?.to_str()?.to_string();
            if !uuid.is_empty() {
                println!("    uuid: {uuid}");
            }
            let pci_bus_id = device.get_pci_bus_id()?.to_str()?.to_string();
            if !pci_bus_id.is_empty() {
                println!("    pci_bus_id: {pci_bus_id}");
            }
            let cc = device.get_compute_capability()?.to_str()?.to_string();
            if !cc.is_empty() {
                println!("    compute_capability: {cc}");
            }
            println!("    memory_total_bytes: {}", device.get_memory_total_bytes());
            println!("    memory_free_bytes: {}", device.get_memory_free_bytes());
        }
    }

    Ok(())
}
