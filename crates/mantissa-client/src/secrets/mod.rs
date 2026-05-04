use anyhow::{Result, anyhow};
use mantissa_protocol::secrets::secret_metadata_entry;
use mantissa_protocol::secrets::{secret_spec, secret_version_data};
use std::collections::BTreeMap;
use uuid::Uuid;

pub mod create;
pub mod delete;
pub mod list;
pub mod rotate;
pub mod show;
pub mod update;

pub use create::create;
pub use delete::delete;
pub use list::list;
pub use rotate::rotate_master_key;
pub use show::show;
pub use update::update;

#[derive(Debug, Clone)]
pub struct SecretSummary {
    pub name: String,
    pub description: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub version_id: Uuid,
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct SecretDetail {
    pub summary: SecretSummary,
    pub plaintext: Vec<u8>,
}

pub(super) fn parse_secret_spec(reader: secret_spec::Reader<'_>) -> Result<SecretSummary> {
    let name = reader.get_name()?.to_str()?.to_string();
    let description_raw = reader.get_description()?.to_str()?.to_string();
    let description = if description_raw.trim().is_empty() {
        None
    } else {
        Some(description_raw)
    };

    let created_at = reader.get_created_at()?.to_str()?.to_string();
    let updated_at = reader.get_updated_at()?.to_str()?.to_string();

    let version_reader = reader.get_current_version()?;
    let version_bytes = version_reader.get_version_id()?;
    if version_bytes.len() != 16 {
        return Err(anyhow!(
            "secret '{}' returned invalid version id length {}",
            name,
            version_bytes.len()
        ));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(version_bytes);
    let version_id = Uuid::from_bytes(bytes);

    let mut labels = Vec::new();
    for entry in reader.get_metadata()?.iter() {
        let key = entry.get_key()?.to_str()?.to_string();
        let value = entry.get_value()?.to_str()?.to_string();
        labels.push((key, value));
    }

    labels.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(SecretSummary {
        name,
        description,
        created_at,
        updated_at,
        version_id,
        labels,
    })
}

pub(super) fn parse_secret_detail(reader: secret_version_data::Reader<'_>) -> Result<SecretDetail> {
    let summary = parse_secret_spec(reader.get_spec()?)?;
    let plaintext = reader.get_plaintext()?.to_owned();
    Ok(SecretDetail { summary, plaintext })
}

pub(super) fn set_metadata(
    metadata_builder: &mut capnp::struct_list::Builder<secret_metadata_entry::Owned>,
    labels: &[(String, String)],
) {
    for (idx, (key, value)) in labels.iter().enumerate() {
        let mut entry = metadata_builder.reborrow().get(idx as u32);
        entry.set_key(key);
        entry.set_value(value);
    }
}

pub(super) fn normalize_labels(raw: &[(String, String)]) -> Vec<(String, String)> {
    let mut map = BTreeMap::new();
    for (key, value) in raw {
        map.insert(key.trim().to_string(), value.trim().to_string());
    }
    map.into_iter().collect()
}
