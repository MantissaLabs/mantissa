@0xd7f4ffbce9f9a5dd;

using import "topology.capnp".ClusterViewId;

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

  getMasterKeyTransfer @5 (request :SecretMasterKeyTransferRequest) -> (envelope :SecretMasterKeyTransfer);
  # Fetch the current cluster master key encrypted to the requested node key.

  installMasterKeyTransfer @6 (envelope :SecretMasterKeyTransfer);
  # Install or replace the cluster master key from an encrypted transfer envelope.

  rotateMasterKey @7 () -> (keyId :Data, generation :UInt64);
  # Rotate the master key and return the new key identity.
}

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

  masterKeyId @3 :Data;
  # 16-byte UUID of the master key used to encrypt this secret.

  masterKeyGeneration @4 :UInt64;
  # Human-readable generation for the master key. Not a unique key identity.
}

struct SecretCiphertext {
  nonce @0 :Data;
  # 12-byte ChaCha20-Poly1305 nonce.

  ciphertext @1 :Data;
  # Encrypted payload bytes (includes Poly1305 tag).

  digest @2 :Data;
  # 32-byte Blake3 digest of plaintext.

  masterKeyId @3 :Data;
  # 16-byte UUID of the master key used to encrypt this payload.

  masterKeyGeneration @4 :UInt64;
  # Human-readable generation for the master key. Not a unique key identity.
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

struct SecretMasterKeyTransferRequest {
  recipientNodeId @0 :Data;
  # 16-byte node UUID that will receive and unwrap the transfer.

  recipientNoiseStaticPub @1 :Data;
  # 32-byte X25519 static public key advertised by the recipient node.
}

struct MasterKeyDescriptor {
  keyId @0 :Data;
  # 16-byte UUID that uniquely identifies this master key.

  generation @1 :UInt64;
  # Monotonic generation inside a key lineage/scope, used for display only.

  scopeView @2 :ClusterViewId;
  # Cluster view this key protects.

  originView @3 :ClusterViewId;
  # Local active view when the key was created.

  createdByNodeId @4 :Data;
  # 16-byte node UUID that generated this key.

  createdByOperationId @5 :Data;
  # 16-byte split/merge operation UUID when applicable, empty otherwise.

  parentKeyIds @6 :List(Data);
  # Parent master-key ids used to derive lineage and merge diagnostics.

  createdAtUnixSecs @7 :UInt64;
  # Unix timestamp when this key was created.
}

struct SecretMasterKeyTransfer {
  descriptor @0 :MasterKeyDescriptor;
  # Metadata for the transferred master key. Contains no key material.

  senderNodeId @1 :Data;
  # 16-byte node UUID that encrypted the transfer.

  recipientNodeId @2 :Data;
  # 16-byte node UUID allowed to decrypt the transfer.

  transferPublicKey @3 :Data;
  # 32-byte ephemeral X25519 public key used for this transfer.

  recipientNoiseStaticPub @4 :Data;
  # 32-byte X25519 static public key the transfer was encrypted to.

  nonce @5 :Data;
  # 24-byte XChaCha20-Poly1305 nonce.

  ciphertext @6 :Data;
  # Encrypted 32-byte master key payload including the Poly1305 tag.

  senderNoiseStaticPub @7 :Data;
  # 32-byte X25519 static public key used to authenticate the sender.
}

struct SecretMasterKeyGrant {
  descriptor @0 :MasterKeyDescriptor;
  # Metadata for the granted master key. Contains no key material.

  senderNodeId @1 :Data;
  # 16-byte node UUID that encrypted the grant.

  recipientNodeId @2 :Data;
  # 16-byte node UUID allowed to decrypt the grant.

  transferPublicKey @3 :Data;
  # 32-byte ephemeral X25519 public key used for this grant.

  recipientNoiseStaticPub @4 :Data;
  # 32-byte X25519 static public key the grant was encrypted to.

  nonce @5 :Data;
  # 24-byte XChaCha20-Poly1305 nonce.

  ciphertext @6 :Data;
  # Encrypted 32-byte master key payload including the Poly1305 tag.

  senderNoiseStaticPub @7 :Data;
  # 32-byte X25519 static public key used to authenticate the sender.
}

struct SecretMasterKeyCurrent {
  scopeView @0 :ClusterViewId;
  # Cluster view for which this key is current.

  keyId @1 :Data;
  # 16-byte UUID of the current key for the scope.

  generation @2 :UInt64;
  # Descriptor generation copied here for compact deterministic selection.

  createdByOperationId @3 :Data;
  # 16-byte split/merge/rotation operation UUID, empty when not applicable.

  parentKeyIds @4 :List(Data);
  # Parent master-key ids this current pointer supersedes.
}

struct SecretMasterKeySyncRecord {
  union {
    descriptor @0 :MasterKeyDescriptor;
    # Public metadata for one master key id.

    grant @1 :SecretMasterKeyGrant;
    # Recipient-specific encrypted key material.

    current @2 :SecretMasterKeyCurrent;
    # Current-key pointer for one cluster view scope.
  }
}

struct WrappedSecretMasterKey {
  schemaVersion @0 :UInt16;
  # Durable envelope schema version.

  descriptor @1 :MasterKeyDescriptor;
  # Metadata for the wrapped master key. Contains no key material.

  provider @2 :Text;
  # Local key-protection provider identifier.

  providerKeyId @3 :Text;
  # Provider-specific local key identifier.

  cipherSuite @4 :Text;
  # AEAD used for the envelope ciphertext.

  nonce @5 :Data;
  # AEAD nonce used to wrap the master key.

  ciphertext @6 :Data;
  # Encrypted 32-byte master key payload including the authentication tag.

  createdAtUnixSecs @7 :UInt64;
  # Unix timestamp when this envelope was created.

  providerMetadata @8 :Data;
  # Opaque provider-specific durable metadata.
}

struct PassphraseMasterKeyMetadata {
  salt @0 :Data;
  # Random Argon2id salt bytes.

  argon2MemoryCostKib @1 :UInt32;
  # Argon2id memory cost in KiB.

  argon2TimeCost @2 :UInt32;
  # Argon2id iteration count.

  argon2Parallelism @3 :UInt32;
  # Argon2id parallelism parameter.
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
