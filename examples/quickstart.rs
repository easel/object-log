//! Minimal end-to-end example: append a couple of records and read them back.
//!
//! Run with: `cargo run --example quickstart`

use object_log::{
    AppendBatch, AppendRecord, LogBackend, MemoryObjectStore, ObjectLogBackend, PartitionId,
    ReadRequest, TopicName, TopicPartition,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Any ObjectStore works; MemoryObjectStore is the simplest for a demo.
    let store = Arc::new(MemoryObjectStore::default());
    let backend = ObjectLogBackend::new(store);

    let tp = TopicPartition::new(TopicName::new("events")?, PartitionId(0));

    let appended = backend
        .append(AppendBatch::new(
            tp.clone(),
            vec![AppendRecord::new("hello"), AppendRecord::new("world")],
        ))
        .await?;
    println!(
        "appended offsets {:?}..={:?}",
        appended.base_offset, appended.last_offset
    );

    let read = backend
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 0,
            max_records: 10,
        })
        .await?;

    for record in &read.records {
        println!(
            "offset {} = {}",
            record.offset,
            String::from_utf8_lossy(&record.value)
        );
    }
    assert_eq!(read.records.len(), 2);
    Ok(())
}
