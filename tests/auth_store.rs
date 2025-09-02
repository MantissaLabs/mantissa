use mantissa::server::auth::AuthStore;
use uuid::Uuid;

mod common;
use common::{temp_db, temp_db_dir};

#[test]
fn auth_issue_lookup_revoke() {
    // temp redb
    let tmp = temp_db_dir();
    let db_path = tmp.path().join("state.redb");
    let db = temp_db(&db_path);

    let store = AuthStore::new(db.clone()).expect("open");

    let peer = Uuid::new_v4();

    // Issue a ticket
    let ticket = store.issue_ticket(peer).expect("issue");
    assert_eq!(ticket.len(), 32, "ticket should be 32 random bytes");

    // Lookup returns the peer
    let got = store.lookup(&ticket).expect("lookup").expect("some");
    assert_eq!(got, peer);

    // Revoke by peer: both forward and reverse maps cleared
    store.revoke_by_peer(peer).expect("revoke-by-peer");
    assert!(store.lookup(&ticket).expect("lookup2").is_none());

    // Issue again and revoke by ticket this time
    let ticket2 = store.issue_ticket(peer).expect("issue2");
    store.revoke_by_ticket(&ticket2).expect("revoke-by-ticket");
    assert!(store.lookup(&ticket2).expect("lookup3").is_none());

    // Revoke non-existing should be fine (idempotent-ish)
    store.revoke_by_peer(peer).expect("revoke-non-existing");
}
