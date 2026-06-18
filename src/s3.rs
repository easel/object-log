//! S3-compatible [`BlobStore`] adapter (AWS S3, MinIO, self-hosted Garage).
//!
//! Enabled by the `s3` cargo feature. Uses path-style addressing and static
//! credentials. `put` is durable-on-return (an S3 `PutObject` 200 means the
//! object is durably stored) and dispatches multipart upload above a size
//! threshold so large coalesced objects are not a single whole-buffer PUT.

use crate::{BlobStore, ObjectLogError};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use std::ops::Range;

fn unavailable<E: std::fmt::Display>(e: E) -> ObjectLogError {
    ObjectLogError::StorageUnavailable(e.to_string())
}

/// S3-compatible blob store.
pub struct S3BlobStore {
    client: Client,
    bucket: String,
    /// Objects larger than this use multipart upload.
    multipart_threshold: usize,
    /// Multipart part size.
    part_size: usize,
}

impl S3BlobStore {
    /// Build a store for an S3-compatible endpoint with static credentials and
    /// path-style addressing (required by Garage/MinIO).
    pub fn new(
        endpoint_url: &str,
        region: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Self {
        let creds = Credentials::new(access_key_id, secret_access_key, None, None, "object-log");
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint_url)
            .region(Region::new(region.to_string()))
            .credentials_provider(creds)
            .force_path_style(true)
            .build();
        Self {
            client: Client::from_conf(conf),
            bucket: bucket.to_string(),
            multipart_threshold: 16 * 1024 * 1024,
            part_size: 8 * 1024 * 1024,
        }
    }

    /// Override the multipart threshold and part size (both in bytes).
    pub fn with_multipart(mut self, threshold: usize, part_size: usize) -> Self {
        self.multipart_threshold = threshold;
        self.part_size = part_size.max(5 * 1024 * 1024); // S3 minimum part size
        self
    }

    async fn put_multipart(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        let created = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(unavailable)?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| ObjectLogError::StorageUnavailable("missing upload id".into()))?
            .to_string();

        match self.upload_parts(key, &upload_id, &value).await {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                Err(e)
            }
        }
    }

    async fn upload_parts(
        &self,
        key: &str,
        upload_id: &str,
        value: &Bytes,
    ) -> Result<(), ObjectLogError> {
        let mut parts = Vec::new();
        let mut offset = 0usize;
        let mut part_number = 1i32;
        while offset < value.len() {
            let end = (offset + self.part_size).min(value.len());
            let chunk = value.slice(offset..end);
            let uploaded = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(chunk))
                .send()
                .await
                .map_err(unavailable)?;
            parts.push(
                CompletedPart::builder()
                    .set_e_tag(uploaded.e_tag().map(str::to_string))
                    .part_number(part_number)
                    .build(),
            );
            part_number += 1;
            offset = end;
        }
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(unavailable)?;
        Ok(())
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        if value.len() > self.multipart_threshold {
            return self.put_multipart(key, value).await;
        }
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(value))
            .send()
            .await
            .map_err(unavailable)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => {
                let data = resp.body.collect().await.map_err(unavailable)?;
                Ok(Some(data.into_bytes()))
            }
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(unavailable(svc))
                }
            }
        }
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        if range.start > range.end {
            return Err(ObjectLogError::RangeOutOfBounds(format!(
                "start {} > end {}",
                range.start, range.end
            )));
        }
        if range.start == range.end {
            // S3 has no zero-length range; treat as an empty read (if the key exists).
            return match self.get(key).await? {
                Some(_) => Ok(Some(Bytes::new())),
                None => Ok(None),
            };
        }
        // HTTP byte ranges are inclusive: bytes=start-(end-1).
        let header = format!("bytes={}-{}", range.start, range.end - 1);
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(header)
            .send()
            .await
        {
            Ok(resp) => {
                let data = resp.body.collect().await.map_err(unavailable)?;
                Ok(Some(data.into_bytes()))
            }
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_no_such_key() {
                    Ok(None)
                } else {
                    // A 416 (range past EOF) and other errors both surface here.
                    Err(unavailable(svc))
                }
            }
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let resp = req.send().await.map_err(unavailable)?;
            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    keys.push(k.to_string());
                }
            }
            match resp.next_continuation_token() {
                Some(t) => token = Some(t.to_string()),
                None => break,
            }
        }
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(unavailable)?;
        Ok(())
    }
}
