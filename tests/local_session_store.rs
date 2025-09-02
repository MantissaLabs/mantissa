use mantissa::local_session_store::LocalSessionStore;
use uuid::Uuid;

mod common;
use common::{fixed_noise_keys, temp_db, temp_db_dir};

#[test]
fn local_sessions_seal_open_and_purge() {
    let tmp = temp_db_dir();
    let db_path = tmp.path().join("state.redb");
    let db = temp_db(&db_path);

    // Derive KEK from deterministic Noise key
    let keys = fixed_noise_keys(42);
    let store = LocalSessionStore::open(db.clone(), &keys).expect("open");

    let peer_kek_bound = Uuid::new_v4(); // stays non-expiring for KEK-mismatch check
    let peer_expired = Uuid::new_v4(); // used to test purge
    let ticket = b"super-secret-ticket".to_vec();

    // A) Put non-expiring record (will remain to test KEK binding)
    store.put(peer_kek_bound, &ticket).expect("put A");

    // B) Put an already-expired record and purge it (no sleep needed)
    let past = now() - 1;
    store
        .put_with_meta(peer_expired, &ticket, Some(past), Some("expired".into()))
        .expect("put expired");
    let removed = store.purge_expired().expect("purge");
    assert!(removed >= 1, "should purge at least one expired record");

    // Re-open with a different Noise key: existing blob should be undecryptable
    let other_keys = fixed_noise_keys(7);
    let reopened = LocalSessionStore::open(db.clone(), &other_keys).expect("reopen");

    // Expect decryption failure for the non-expiring record (KEK mismatch)
    let err = reopened.get(peer_kek_bound).expect_err("must fail decrypt");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    // Expired peer should be gone
    let none = reopened.get(peer_expired).expect("ok(None) on missing");
    assert!(none.is_none());
}

fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
