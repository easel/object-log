//! Minimal engine example: produce a couple of batches and read them back.
//!
//! Run with: `cargo run --example quickstart`

use bytes::Bytes;
use object_log::{
    Durability, FlushConfig, InMemorySequencer, LogEngine, MemoryBlobStore, PartitionKey,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let partition = PartitionKey("events-0".into());

    for msg in ["hello", "world"] {
        let out = engine
            .produce(
                partition.clone(),
                Bytes::copy_from_slice(msg.as_bytes()),
                1,
                (),
                Durability::Sequenced,
            )
            .await?;
        println!("appended {msg:?} at offset {:?}", out.base_offset);
    }

    for batch in engine.fetch(&partition, 0, 1 << 20).await? {
        println!(
            "offset {} = {}",
            batch.base_offset,
            String::from_utf8_lossy(&batch.payload)
        );
    }
    Ok(())
}
