#![allow(clippy::unwrap_used)]

use mantissa::store::local::LocalSessionStore;
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

    let peer_kek_bound = Uuid::new_v4(); // stays valid long enough for KEK-mismatch check
    let peer_expired = Uuid::new_v4(); // used to test purge
    let ticket = b"super-secret-ticket".to_vec();

    // A) Put a default-lifetime record (will remain to test KEK binding)
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

#[test]
fn local_sessions_get_and_list_skip_expired_records() {
    let tmp = temp_db_dir();
    let db_path = tmp.path().join("state.redb");
    let db = temp_db(&db_path);
    let keys = fixed_noise_keys(42);
    let store = LocalSessionStore::open_with_ticket_ttl(db, &keys, 60).expect("open");

    let valid_peer = Uuid::new_v4();
    let expired_peer = Uuid::new_v4();
    let ticket = b"resume-ticket".to_vec();
    let now = now();

    store
        .put_with_meta(valid_peer, &ticket, Some(now + 60), None)
        .expect("put valid");
    store
        .put_with_meta(expired_peer, &ticket, Some(now - 1), None)
        .expect("put expired");

    assert_eq!(store.get(valid_peer).expect("get valid"), Some(ticket));
    assert!(store.get(expired_peer).expect("get expired").is_none());

    let listed = store.list().expect("list valid");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0, valid_peer);
    assert!(
        store
            .get_record(expired_peer)
            .expect("expired purged")
            .is_none()
    );
}

#[test]
fn local_sessions_list_purges_invalid_records() {
    let tmp = temp_db_dir();
    let db_path = tmp.path().join("state.redb");
    let db = temp_db(&db_path);
    let keys = fixed_noise_keys(42);
    let store = LocalSessionStore::open(db.clone(), &keys).expect("open");
    let peer = Uuid::new_v4();

    store.put(peer, b"resume-ticket").expect("put");

    let other_keys = fixed_noise_keys(7);
    let reopened = LocalSessionStore::open(db.clone(), &other_keys).expect("reopen");
    let err = reopened
        .get(peer)
        .expect_err("record is bound to original key");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    let listed = reopened.list().expect("list skips invalid record");
    assert!(listed.is_empty());
    assert!(store.get(peer).expect("invalid record purged").is_none());
}

#[test]
fn local_sessions_clear_removes_invalid_records() {
    let tmp = temp_db_dir();
    let db_path = tmp.path().join("state.redb");
    let db = temp_db(&db_path);
    let keys = fixed_noise_keys(42);
    let store = LocalSessionStore::open(db.clone(), &keys).expect("open");
    let peer = Uuid::new_v4();

    store.put(peer, b"resume-ticket").expect("put");

    let other_keys = fixed_noise_keys(7);
    let reopened = LocalSessionStore::open(db.clone(), &other_keys).expect("reopen");
    reopened.clear().expect("clear does not decrypt records");

    assert!(store.get(peer).expect("record cleared").is_none());
}

fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
