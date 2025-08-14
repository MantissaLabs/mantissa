use crate::client::common;
use crate::client::config::ClientConfig;
use crate::node::id::{id_sort_key_uuid_bytes, id_string};
use crate::topology_capnp::node_info::Reader as NodeInfo;
use std::error::Error;
use std::io::Write;
use tabwriter::TabWriter;

pub async fn list(cfg: &ClientConfig) -> Result<(), Box<dyn Error>> {
    let client = common::get_client_auto(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let request = topology.list_request();

    let response = request.send().promise.await?;

    let reader = response.get()?.get_nodes()?;
    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tHOSTNAME\tENDPOINT").unwrap();

    let mut list: Vec<NodeInfo> = reader.get_nodes()?.iter().collect();
    list.sort_by_key(id_sort_key_uuid_bytes);

    for n in &list {
        writeln!(
            &mut tw,
            "{}\t{}\t{}",
            id_string(n)?,
            n.get_hostname()?.to_str()?,
            n.get_addr()?.to_str()?
        )?;
    }

    tw.flush().unwrap();
    let output = String::from_utf8(tw.into_inner().unwrap()).unwrap();

    println!("{}", output);

    Ok(())
}
