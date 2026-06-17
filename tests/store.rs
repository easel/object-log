//! Direct conditional-write conformance for the bundled `ObjectStore` adapters.

use bytes::Bytes;
use object_log::{
    LocalObjectStore, MemoryObjectStore, ObjectKey, ObjectLogError, ObjectStore, PutOutcome,
};

/// Exercise `put_if_absent` / `compare_and_set` / `get` / `list` / `delete`
/// semantics that both adapters must satisfy identically.
async fn conditional_write_suite(store: &dyn ObjectStore) {
    let key = ObjectKey::new("dir/obj").unwrap();
    assert!(store.get(&key).await.unwrap().is_none());

    // put_if_absent: create, idempotent same-bytes, conflict on different bytes.
    assert_eq!(
        store
            .put_if_absent(&key, Bytes::from_static(b"v1"))
            .await
            .unwrap(),
        PutOutcome::Created
    );
    assert_eq!(
        store
            .put_if_absent(&key, Bytes::from_static(b"v1"))
            .await
            .unwrap(),
        PutOutcome::AlreadyExistsSame
    );
    assert_eq!(
        store
            .put_if_absent(&key, Bytes::from_static(b"different"))
            .await
            .unwrap_err(),
        ObjectLogError::ObjectConflict
    );

    let current = store.get(&key).await.unwrap().unwrap();
    assert_eq!(current.value, Bytes::from_static(b"v1"));

    // compare_and_set: None-expected against an existing key conflicts.
    assert_eq!(
        store
            .compare_and_set(&key, None, Bytes::from_static(b"x"))
            .await
            .unwrap_err(),
        ObjectLogError::Conflict
    );

    // compare_and_set: matching version succeeds and advances the version.
    let updated = store
        .compare_and_set(
            &key,
            Some(current.version.clone()),
            Bytes::from_static(b"v2"),
        )
        .await
        .unwrap();
    assert_eq!(updated.value, Bytes::from_static(b"v2"));
    assert_ne!(updated.version, current.version);

    // compare_and_set: the now-stale version conflicts.
    assert_eq!(
        store
            .compare_and_set(
                &key,
                Some(current.version.clone()),
                Bytes::from_static(b"v3")
            )
            .await
            .unwrap_err(),
        ObjectLogError::Conflict
    );

    // list by prefix finds the key; an unrelated prefix does not.
    assert!(store.list("dir/").await.unwrap().contains(&key));
    assert!(store.list("other/").await.unwrap().is_empty());

    // delete, then deleting a missing key is a no-op success.
    store.delete(&key).await.unwrap();
    assert!(store.get(&key).await.unwrap().is_none());
    store.delete(&key).await.unwrap();

    // compare_and_set with None creates a fresh key.
    let created = store
        .compare_and_set(&key, None, Bytes::from_static(b"fresh"))
        .await
        .unwrap();
    assert_eq!(created.value, Bytes::from_static(b"fresh"));
}

#[tokio::test]
async fn memory_store_conditional_writes() {
    conditional_write_suite(&MemoryObjectStore::default()).await;
}

#[tokio::test]
async fn local_store_conditional_writes() {
    let dir = tempfile::tempdir().unwrap();
    conditional_write_suite(&LocalObjectStore::new(dir.path())).await;
}

#[tokio::test]
async fn local_store_lists_nested_keys_without_temp_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(dir.path());
    for k in [
        "topics/a/segments/0",
        "topics/a/segments/1",
        "topics/a/manifest",
    ] {
        let key = ObjectKey::new(k).unwrap();
        store
            .put_if_absent(&key, Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    let keys = store.list("topics/a/").await.unwrap();
    assert_eq!(keys.len(), 3, "no leftover temp files should be listed");
}
