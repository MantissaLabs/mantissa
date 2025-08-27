use crate::client::{config::ClientConfig, connection};
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

    Ok(())
}
