#![cfg(feature = "s3")]
//! Live S3-compatible adapter round-trip. Runs only when the Garage/S3 env is
//! configured; otherwise it skips and passes, so default CI stays hermetic.
//!
//!   FJORD_GARAGE_ENDPOINT=… FJORD_GARAGE_BUCKET=… FJORD_GARAGE_KEY_ID=… \
//!     FJORD_GARAGE_SECRET=… cargo test -p object-log --features s3 --test s3 -- --nocapture

use bytes::Bytes;
use object_log::{BlobStore, S3BlobStore};

#[tokio::test]
async fn s3_blob_store_round_trip() {
    let (Ok(endpoint), Ok(bucket), Ok(key_id), Ok(secret)) = (
        std::env::var("FJORD_GARAGE_ENDPOINT"),
        std::env::var("FJORD_GARAGE_BUCKET"),
        std::env::var("FJORD_GARAGE_KEY_ID"),
        std::env::var("FJORD_GARAGE_SECRET"),
    ) else {
        eprintln!("FJORD_GARAGE_* not fully set — skipping live S3 test");
        return;
    };
    let region = std::env::var("FJORD_GARAGE_REGION").unwrap_or_else(|_| "garage".to_string());
    let store = S3BlobStore::new(&endpoint, &region, &bucket, &key_id, &secret);

    let key = format!("object-log-test/{}-roundtrip", std::process::id());
    store
        .put(&key, Bytes::from_static(b"hello world"))
        .await
        .unwrap();
    assert_eq!(
        store.get(&key).await.unwrap().unwrap(),
        Bytes::from_static(b"hello world")
    );
    assert_eq!(
        store.get_range(&key, 6..11).await.unwrap().unwrap(),
        Bytes::from_static(b"world")
    );
    assert!(
        store
            .list("object-log-test/")
            .await
            .unwrap()
            .iter()
            .any(|k| k == &key)
    );
    store.delete(&key).await.unwrap();
    assert!(store.get(&key).await.unwrap().is_none());
}
