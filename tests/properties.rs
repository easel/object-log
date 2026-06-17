//! Property tests for log invariants via the public `LogBackend` API.

use object_log::{
    AppendBatch, AppendRecord, LogBackend, MemoryObjectStore, ObjectLogBackend, PartitionId,
    ReadRequest, TopicName, TopicPartition,
};
use proptest::prelude::*;
use std::sync::Arc;

proptest! {
    // Appending a sequence of batches yields dense, contiguous offsets starting
    // at 0, and a full read returns every record in offset order.
    #[test]
    fn offsets_are_dense_and_contiguous(sizes in prop::collection::vec(1usize..=5, 1..20)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let backend = ObjectLogBackend::new(Arc::new(MemoryObjectStore::default()));
            let tp = TopicPartition::new(TopicName::new("p").unwrap(), PartitionId(0));

            let mut expected_base = 0u64;
            for &size in &sizes {
                let records: Vec<_> = (0..size).map(|_| AppendRecord::new("x")).collect();
                let result = backend
                    .append(AppendBatch::new(tp.clone(), records))
                    .await
                    .unwrap();
                prop_assert_eq!(result.base_offset, Some(expected_base));
                prop_assert_eq!(result.last_offset, Some(expected_base + size as u64 - 1));
                expected_base += size as u64;
            }

            let total = expected_base;
            let read = backend
                .read(ReadRequest {
                    topic_partition: tp,
                    start_offset: 0,
                    max_records: total as usize + 10,
                })
                .await
                .unwrap();
            prop_assert_eq!(read.records.len() as u64, total);
            for (i, record) in read.records.iter().enumerate() {
                prop_assert_eq!(record.offset, i as u64);
            }
            Ok(())
        })?;
    }
}
