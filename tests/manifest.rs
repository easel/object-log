//! The persisted (manifest) sequencer makes a standalone log crash-durable: its
//! offset index is rebuilt from the BlobStore after a restart.

use bytes::Bytes;
use object_log::{
    BlobStore, Durability, FlushConfig, LogEngine, ManifestSequencer, MemoryBlobStore,
    PartitionKey, Sequencer,
};
use std::sync::Arc;

fn pk(s: &str) -> PartitionKey {
    PartitionKey(s.to_string())
}

#[tokio::test]
async fn manifest_index_survives_restart() {
    // The BlobStore persists across the "restart"; only the engine + sequencer
    // are recreated (their in-memory state is gone).
    let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let p = pk("t-0");

    // First "process": produce two batches.
    {
        let seq = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "_manifest/")
                .await
                .unwrap(),
        );
        let engine = LogEngine::new(
            Arc::clone(&blob),
            Arc::clone(&seq),
            FlushConfig::default(),
            "log/",
        );
        engine
            .produce(
                p.clone(),
                Bytes::from_static(b"a"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        engine
            .produce(
                p.clone(),
                Bytes::from_static(b"bb"),
                2,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
    } // engine + sequencer dropped — in-memory index gone.

    // "Restart": a fresh sequencer rebuilds the index from the manifest objects.
    let seq2 = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), "_manifest/")
            .await
            .unwrap(),
    );
    assert_eq!(
        seq2.high_watermark(&p).unwrap(),
        3,
        "index restored from manifests"
    );

    let engine2 = LogEngine::new(
        Arc::clone(&blob),
        Arc::clone(&seq2),
        FlushConfig::default(),
        "log/",
    );
    let all = engine2.fetch(&p, 0, 1 << 20).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].payload, "a");
    assert_eq!(all[1].base_offset, 1);
    assert_eq!(all[1].payload, "bb");

    // New writes continue from the recovered high-watermark.
    let out = engine2
        .produce(
            p.clone(),
            Bytes::from_static(b"c"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(out.base_offset, Some(3));
}
