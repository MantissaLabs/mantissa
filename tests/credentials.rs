#![allow(clippy::unwrap_used)]

use ed25519_dalek::SigningKey;
use mantissa::server::credential::ClusterCredential;
use uuid::Uuid;

#[test]
fn credential_sign_verify_roundtrip() {
    let sk = SigningKey::from_bytes(&[7u8; 32]); // deterministic
    let subject = Uuid::new_v4();

    let cred = ClusterCredential::sign(&sk, subject, 60, [1u8; 16]); // TTL 60s
    cred.verify().expect("should verify");

    // bytes roundtrip
    let bytes = cred.to_bytes().expect("serialize");
    let parsed = ClusterCredential::from_bytes_verified(&bytes).expect("parse+verify");
    assert_eq!(parsed.subject, subject);
    assert_eq!(parsed.issuer.to_bytes(), sk.verifying_key().to_bytes());
}

#[test]
fn credential_expiry_and_tamper() {
    let sk = SigningKey::from_bytes(&[9u8; 32]);
    let subject = Uuid::new_v4();

    // Very short TTL so it expires quickly
    let cred = ClusterCredential::sign(&sk, subject, 1, [2u8; 16]);
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert!(cred.verify().is_err(), "should be expired");

    // Tamper a signed field and re-encode it; verification must reject it.
    let mut tampered = ClusterCredential::sign(&sk, subject, 60, [3u8; 16]);
    tampered.subject = Uuid::new_v4();
    let bytes = tampered.to_bytes().unwrap();
    assert!(ClusterCredential::from_bytes_verified(&bytes).is_err());
}
