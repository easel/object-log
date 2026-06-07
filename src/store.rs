use crate::ObjectLogError;
use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Safe object key used by object-log storage adapters.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(value: impl Into<String>) -> Result<Self, ObjectLogError> {
        let value = value.into();
        if value.is_empty() || value.contains('\0') || value.split('/').any(|part| part == "..") {
            return Err(ObjectLogError::InvalidObjectKey(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectVersion(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    pub value: Bytes,
    pub version: ObjectVersion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    AlreadyExistsSame,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreCapabilities {
    pub compare_and_set: bool,
    pub put_if_absent: bool,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, key: &ObjectKey) -> Result<Option<StoredObject>, ObjectLogError>;
    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        value: Bytes,
    ) -> Result<PutOutcome, ObjectLogError>;
    async fn compare_and_set(
        &self,
        key: &ObjectKey,
        expected: Option<ObjectVersion>,
        value: Bytes,
    ) -> Result<StoredObject, ObjectLogError>;
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectKey>, ObjectLogError>;
    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectLogError>;
    fn capabilities(&self) -> StoreCapabilities;
}

#[derive(Clone, Default)]
pub struct MemoryObjectStore {
    objects: Arc<Mutex<BTreeMap<String, (Bytes, u64)>>>,
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn get(&self, key: &ObjectKey) -> Result<Option<StoredObject>, ObjectLogError> {
        let objects = self.objects.lock().await;
        Ok(objects
            .get(key.as_str())
            .map(|(value, version)| StoredObject {
                value: value.clone(),
                version: ObjectVersion(version.to_string()),
            }))
    }

    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        value: Bytes,
    ) -> Result<PutOutcome, ObjectLogError> {
        let mut objects = self.objects.lock().await;
        match objects.get(key.as_str()) {
            Some((existing, _)) if existing == &value => Ok(PutOutcome::AlreadyExistsSame),
            Some(_) => Err(ObjectLogError::ObjectConflict),
            None => {
                objects.insert(key.as_str().to_string(), (value, 1));
                Ok(PutOutcome::Created)
            }
        }
    }

    async fn compare_and_set(
        &self,
        key: &ObjectKey,
        expected: Option<ObjectVersion>,
        value: Bytes,
    ) -> Result<StoredObject, ObjectLogError> {
        let mut objects = self.objects.lock().await;
        let current = objects
            .get(key.as_str())
            .map(|(_, version)| version.to_string());
        if current != expected.map(|version| version.0) {
            return Err(ObjectLogError::Conflict);
        }
        let next_version = objects
            .get(key.as_str())
            .map_or(1, |(_, version)| version.saturating_add(1));
        objects.insert(key.as_str().to_string(), (value.clone(), next_version));
        Ok(StoredObject {
            value,
            version: ObjectVersion(next_version.to_string()),
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectKey>, ObjectLogError> {
        let objects = self.objects.lock().await;
        objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .map(|key| ObjectKey::new(key.clone()))
            .collect()
    }

    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectLogError> {
        self.objects.lock().await.remove(key.as_str());
        Ok(())
    }

    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            compare_and_set: true,
            put_if_absent: true,
        }
    }
}

#[derive(Clone)]
pub struct LocalObjectStore {
    root: Arc<PathBuf>,
    lock: Arc<Mutex<()>>,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
            lock: Arc::new(Mutex::new(())),
        }
    }

    fn path_for(&self, key: &ObjectKey) -> PathBuf {
        self.root.join(key.as_str())
    }

    async fn read_current(&self, key: &ObjectKey) -> Result<Option<StoredObject>, ObjectLogError> {
        let path = self.path_for(key);
        match tokio::fs::read(&path).await {
            Ok(value) => {
                let value = Bytes::from(value);
                Ok(Some(StoredObject {
                    version: ObjectVersion(hash_hex(&value)),
                    value,
                }))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn atomic_write(path: &Path, value: Bytes) -> Result<(), ObjectLogError> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = path.with_extension("tmp");
        tokio::fs::write(&tmp, value).await?;
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn get(&self, key: &ObjectKey) -> Result<Option<StoredObject>, ObjectLogError> {
        self.read_current(key).await
    }

    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        value: Bytes,
    ) -> Result<PutOutcome, ObjectLogError> {
        let _guard = self.lock.lock().await;
        match self.read_current(key).await? {
            Some(existing) if existing.value == value => Ok(PutOutcome::AlreadyExistsSame),
            Some(_) => Err(ObjectLogError::ObjectConflict),
            None => {
                Self::atomic_write(&self.path_for(key), value).await?;
                Ok(PutOutcome::Created)
            }
        }
    }

    async fn compare_and_set(
        &self,
        key: &ObjectKey,
        expected: Option<ObjectVersion>,
        value: Bytes,
    ) -> Result<StoredObject, ObjectLogError> {
        let _guard = self.lock.lock().await;
        let current = self.read_current(key).await?;
        if current.as_ref().map(|object| object.version.clone()) != expected {
            return Err(ObjectLogError::Conflict);
        }
        Self::atomic_write(&self.path_for(key), value.clone()).await?;
        Ok(StoredObject {
            version: ObjectVersion(hash_hex(&value)),
            value,
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectKey>, ObjectLogError> {
        let mut keys = Vec::new();
        let root = self.root.clone();
        let prefix = prefix.to_string();
        let files = tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            collect_files(&root, &root, &mut out)?;
            Ok::<_, std::io::Error>(out)
        })
        .await
        .map_err(|err| ObjectLogError::StorageUnavailable(err.to_string()))??;
        for key in files {
            if key.starts_with(&prefix) {
                keys.push(ObjectKey::new(key)?);
            }
        }
        keys.sort();
        Ok(keys)
    }

    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectLogError> {
        match tokio::fs::remove_file(self.path_for(key)).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            compare_and_set: true,
            put_if_absent: true,
        }
    }
}

fn collect_files(root: &Path, path: &Path, out: &mut Vec<String>) -> Result<(), std::io::Error> {
    if !path.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, out)?;
        } else if path.extension().is_none_or(|ext| ext != "tmp")
            && let Ok(relative) = path.strip_prefix(root)
        {
            out.push(relative.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}

fn hash_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
