use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use protocol::secrets::secret_metadata_entry;
use protocol::secrets::{secret_spec, secret_version_data};
use std::collections::BTreeMap;
use uuid::Uuid;

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

fn parse_secret_spec(reader: secret_spec::Reader<'_>) -> Result<SecretSummary> {
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
    bytes.copy_from_slice(&version_bytes);
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

fn parse_secret_detail(reader: secret_version_data::Reader<'_>) -> Result<SecretDetail> {
    let summary = parse_secret_spec(reader.get_spec()?)?;
    let plaintext = reader.get_plaintext()?.to_owned();
    Ok(SecretDetail { summary, plaintext })
}

fn set_metadata(
    metadata_builder: &mut capnp::struct_list::Builder<secret_metadata_entry::Owned>,
    labels: &[(String, String)],
) {
    for (idx, (key, value)) in labels.iter().enumerate() {
        let mut entry = metadata_builder.reborrow().get(idx as u32);
        entry.set_key(key);
        entry.set_value(value);
    }
}

fn normalize_labels(raw: &[(String, String)]) -> Vec<(String, String)> {
    let mut map = BTreeMap::new();
    for (key, value) in raw {
        map.insert(key.trim().to_string(), value.trim().to_string());
    }
    map.into_iter().collect()
}

pub async fn list(cfg: &ClientConfig) -> Result<Vec<SecretSummary>> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let response = secrets_client
        .list_request()
        .send()
        .promise
        .await
        .context("secrets list request failed")?;
    let reader = response.get()?.get_secrets()?;

    let mut summaries = Vec::with_capacity(reader.len() as usize);
    for spec in reader.iter() {
        summaries.push(parse_secret_spec(spec)?);
    }
    Ok(summaries)
}

pub async fn create(
    cfg: &ClientConfig,
    name: &str,
    plaintext: &[u8],
    description: Option<&str>,
    labels: &[(String, String)],
) -> Result<SecretSummary> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let mut create = secrets_client.create_request();
    {
        let mut inner = create.get().init_request();
        inner.set_name(name);
        inner.set_plaintext(plaintext);
        inner.set_description(description.unwrap_or(""));
        let normalized = normalize_labels(labels);
        let mut metadata_builder = inner.reborrow().init_metadata(normalized.len() as u32);
        set_metadata(&mut metadata_builder, &normalized);
    }

    let response = create
        .send()
        .promise
        .await
        .context("secrets create request failed")?;
    let reader = response.get()?.get_secret()?;
    parse_secret_spec(reader)
}

pub async fn update(
    cfg: &ClientConfig,
    name: &str,
    plaintext: &[u8],
    description: Option<&str>,
    labels: &[(String, String)],
) -> Result<SecretSummary> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let mut update = secrets_client.update_request();
    {
        let mut inner = update.get().init_request();
        inner.set_name(name);
        inner.set_plaintext(plaintext);
        inner.set_description(description.unwrap_or(""));
        let normalized = normalize_labels(labels);
        let mut metadata_builder = inner.reborrow().init_metadata(normalized.len() as u32);
        set_metadata(&mut metadata_builder, &normalized);
    }

    let response = update
        .send()
        .promise
        .await
        .context("secrets update request failed")?;
    let reader = response.get()?.get_secret()?;
    parse_secret_spec(reader)
}

pub async fn delete(cfg: &ClientConfig, names: &[String]) -> Result<()> {
    if names.is_empty() {
        return Ok(());
    }

    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let mut delete = secrets_client.delete_request();
    {
        let mut list = delete.get().init_names(names.len() as u32);
        for (idx, name) in names.iter().enumerate() {
            list.set(idx as u32, name);
        }
    }

    delete
        .send()
        .promise
        .await
        .context("secrets delete request failed")?;
    Ok(())
}

pub async fn show(cfg: &ClientConfig, name: &str, version: Option<Uuid>) -> Result<SecretDetail> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let mut get_req = secrets_client.get_request();
    {
        let mut inner = get_req.get();
        inner.set_name(name);
        if let Some(version) = version {
            inner.set_version_id(version.as_bytes());
        } else {
            inner.set_version_id(&[]);
        }
    }

    let response = get_req
        .send()
        .promise
        .await
        .context("secrets get request failed")?;
    let reader = response.get()?.get_version()?;
    parse_secret_detail(reader)
}
