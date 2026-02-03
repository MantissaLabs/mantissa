@0xd7f4ffbce9f9a5dd;

struct SecretMetadataEntry {
  key @0 :Text;
  # Metadata key name.

  value @1 :Text;
  # Metadata value content.
}

struct SecretRef {
  name @0 :Text;
  # Logical secret name.

  versionId @1 :Data;
  # 16-byte UUID when set, empty = latest.
}

struct SecretVersion {
  versionId @0 :Data;
  # 16-byte UUID.

  createdAt @1 :Text;
  # RFC3339 timestamp.

  createdBy @2 :Data;
  # 16-byte UUID, optional (empty = unknown).

  masterKeyVersion @3 :UInt64;
  # Master key version used to encrypt this secret.
}

struct SecretCiphertext {
  nonce @0 :Data;
  # 12-byte ChaCha20-Poly1305 nonce.

  ciphertext @1 :Data;
  # Encrypted payload bytes (includes Poly1305 tag).

  digest @2 :Data;
  # 32-byte Blake3 digest of plaintext.

  masterKeyVersion @3 :UInt64;
  # Master key version used to encrypt this payload.
}

struct SecretSpec {
  id @0 :Data;
  # Deterministic secret UUID (16 bytes).

  name @1 :Text;
  # Logical secret name.

  createdAt @2 :Text;
  # RFC3339 timestamp when first created.

  updatedAt @3 :Text;
  # RFC3339 timestamp for latest version.

  metadata @4 :List(SecretMetadataEntry);
  # User-defined metadata key/value pairs.

  currentVersion @5 :SecretVersion;
  # Metadata for the current secret version.

  description @6 :Text;
  # Human-readable description of the secret.
}

struct SecretVersionData {
  spec @0 :SecretSpec;
  # Secret specification metadata.

  plaintext @1 :Data;
  # Decrypted bytes of the requested version.
}

struct SecretUpsertRequest {
  name @0 :Text;
  # Logical secret name.

  plaintext @1 :Data;
  # Raw secret bytes to encrypt.

  description @2 :Text;
  # Description for operators.

  metadata @3 :List(SecretMetadataEntry);
  # User-defined metadata entries.
}

struct SecretMasterKey {
  version @0 :UInt64;
  # Master key version number.

  key @1 :Data;
  # 32-byte cluster master key.
}

struct SecretRecord {
  spec @0 :SecretSpec;
  # Secret metadata.

  ciphertext @1 :SecretCiphertext;
  # Encrypted payload and integrity metadata.
}

struct SecretEvent {
  union {
    upsert @0 :SecretRecord;
    # Secret upsert payload.

    remove @1 :Data;
    # 16-byte UUID for the secret identifier.
  }
}

interface Secrets {
  list @0 () -> (secrets :List(SecretSpec));
  # List secret specifications (no plaintext).

  create @1 (request :SecretUpsertRequest) -> (secret :SecretSpec);
  # Create a new secret and return its spec.

  update @2 (request :SecretUpsertRequest) -> (secret :SecretSpec);
  # Update a secret by creating a new version.

  delete @3 (names :List(Text));
  # Delete secrets by name.

  get @4 (name :Text, versionId :Data) -> (version :SecretVersionData);
  # Fetch a secret version (plaintext returned to authorized caller).

  getMasterKey @5 () -> (envelope :SecretMasterKey);
  # Fetch the current cluster master key envelope.

  installMasterKey @6 (envelope :SecretMasterKey);
  # Install or replace the cluster master key envelope.

  rotateMasterKey @7 () -> (version :UInt64);
  # Rotate the master key and return the new version.
}
