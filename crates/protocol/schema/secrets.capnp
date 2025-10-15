@0xd7f4ffbce9f9a5dd;

struct SecretMetadataEntry {
  key @0 :Text;
  value @1 :Text;
}

struct SecretRef {
  name @0 :Text;
  versionId @1 :Data; # 16-byte UUID when set, empty = latest.
}

struct SecretVersion {
  versionId @0 :Data;  # 16-byte UUID
  createdAt @1 :Text;  # RFC3339 timestamp
  createdBy @2 :Data;  # 16-byte UUID, optional (empty = unknown)
  masterKeyVersion @3 :UInt64;
}

struct SecretCiphertext {
  nonce @0 :Data;      # 12-byte ChaCha20-Poly1305 nonce
  ciphertext @1 :Data; # encrypted payload bytes (includes Poly1305 tag)
  digest @2 :Data;     # 32-byte Blake3 digest of plaintext
  masterKeyVersion @3 :UInt64;
}

struct SecretSpec {
  id @0 :Data;                  # Deterministic secret UUID (16 bytes)
  name @1 :Text;                # Logical secret name
  createdAt @2 :Text;           # RFC3339 timestamp when first created
  updatedAt @3 :Text;           # RFC3339 timestamp for latest version
  metadata @4 :List(SecretMetadataEntry);
  currentVersion @5 :SecretVersion;
  description @6 :Text;
}

struct SecretVersionData {
  spec @0 :SecretSpec;
  plaintext @1 :Data; # Decrypted bytes of the requested version
}

struct SecretUpsertRequest {
  name @0 :Text;
  plaintext @1 :Data;
  description @2 :Text;
  metadata @3 :List(SecretMetadataEntry);
}

struct SecretMasterKey {
  version @0 :UInt64;
  key @1 :Data; # 32-byte cluster master key
}

interface Secrets {
  list @0 () -> (secrets :List(SecretSpec));

  create @1 (request :SecretUpsertRequest) -> (secret :SecretSpec);

  update @2 (request :SecretUpsertRequest) -> (secret :SecretSpec);

  delete @3 (names :List(Text));

  get @4 (name :Text, versionId :Data) -> (version :SecretVersionData);

  getMasterKey @5 () -> (envelope :SecretMasterKey);

  rotateMasterKey @6 () -> (version :UInt64);
}
