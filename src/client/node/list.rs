use crate::client::common;
use crate::topology_capnp::node_info::Reader as NodeInfo;
use std::error::Error;
use std::io::Write;
use tabwriter::TabWriter;

pub async fn list(server_address: &str, _cluster: &str) -> Result<(), Box<dyn Error>> {
    let client = common::get_client(server_address).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let request = topology.list_request();

    let response = request.send().promise.await?;

    let reader = response.get()?.get_nodes()?;
    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tHOSTNAME\tENDPOINT").unwrap();

    let mut list: Vec<NodeInfo> = reader.get_nodes()?.iter().collect();
    list.sort_by_key(|n| n.get_id());

    for n in &list {
        writeln!(
            &mut tw,
            "{}\t{:?}\t{:?}",
            n.get_id(),
            n.get_hostname()?,
            n.get_addr()?
        )
        .unwrap();
    }

    tw.flush().unwrap();
    let output = String::from_utf8(tw.into_inner().unwrap()).unwrap();

    println!("{}", output);

    Ok(())
}
