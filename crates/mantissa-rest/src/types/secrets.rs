use base64::{Engine, engine::general_purpose::STANDARD};
use mantissa_client::secrets::{
    SecretDetail as ClientSecretDetail, SecretSummary as ClientSecretSummary,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// REST-facing secret metadata label.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SecretLabel {
    pub key: String,
    pub value: String,
}

impl From<(String, String)> for SecretLabel {
    /// Converts a client label tuple into the REST JSON shape.
    fn from((key, value): (String, String)) -> Self {
        Self { key, value }
    }
}

impl SecretLabel {
    /// Converts this REST label into the tuple accepted by the client API.
    pub fn into_tuple(self) -> (String, String) {
        (self.key, self.value)
    }
}

/// REST-facing secret summary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct SecretSummary {
    pub name: String,
    pub description: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub version_id: String,
    pub labels: Vec<SecretLabel>,
}

impl From<ClientSecretSummary> for SecretSummary {
    /// Converts the client secret summary into the REST JSON shape.
    fn from(value: ClientSecretSummary) -> Self {
        Self {
            name: value.name,
            description: value.description,
            created_at: value.created_at,
            updated_at: value.updated_at,
            version_id: value.version_id.to_string(),
            labels: value.labels.into_iter().map(SecretLabel::from).collect(),
        }
    }
}

/// REST-facing decrypted secret detail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct SecretDetail {
    pub summary: SecretSummary,
    pub plaintext_base64: String,
}

impl From<ClientSecretDetail> for SecretDetail {
    /// Converts the client secret detail into the REST JSON shape.
    fn from(value: ClientSecretDetail) -> Self {
        Self {
            summary: value.summary.into(),
            plaintext_base64: STANDARD.encode(value.plaintext),
        }
    }
}

/// REST request body for creating or updating one secret.
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SecretUpsertRequest {
    pub plaintext_base64: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub labels: Vec<SecretLabel>,
}

impl SecretUpsertRequest {
    /// Decodes the base64 plaintext carried by this request.
    pub fn plaintext(&self) -> Result<Vec<u8>, String> {
        STANDARD
            .decode(&self.plaintext_base64)
            .map_err(|error| format!("invalid plaintext_base64: {error}"))
    }

    /// Converts labels into the tuple form accepted by the client API.
    pub fn labels(&self) -> Vec<(String, String)> {
        self.labels
            .iter()
            .cloned()
            .map(SecretLabel::into_tuple)
            .collect()
    }
}

/// REST request body for creating one named secret.
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SecretCreateRequest {
    pub name: String,
    pub plaintext_base64: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub labels: Vec<SecretLabel>,
}

impl SecretCreateRequest {
    /// Splits this create request into the worker name plus payload form.
    pub fn into_named_upsert(self) -> (String, SecretUpsertRequest) {
        (
            self.name,
            SecretUpsertRequest {
                plaintext_base64: self.plaintext_base64,
                description: self.description,
                labels: self.labels,
            },
        )
    }
}

/// REST response returned after deleting one or more secrets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct SecretDeleteResponse {
    pub deleted: usize,
}
