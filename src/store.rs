use crate::ObjectLogError;
use crate::util::sha256_hex;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Suffix appended to in-flight temp files written by [`LocalObjectStore`].
/// Keys ending in this marker are not representable as durable objects.
const TMP_SUFFIX: &str = ".olog-tmp";

/// Safe object key used by object-log storage adapters.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Validate and wrap an object key.
    ///
    /// Rejects empty keys and keys containing `\0` or a `..` path component
    /// (returning [`ObjectLogError::InvalidObjectKey`]).
    pub fn new(value: impl Into<String>) -> Result<Self, ObjectLogError> {
        let value = value.into();
        if value.is_empty() || value.contains('\0') || value.split('/').any(|part| part == "..") {
            return Err(ObjectLogError::InvalidObjectKey(value));
        }
        Ok(Self(value))
    }

    /// Borrow the validated key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque version token for an object, used as the precondition for
/// [`ObjectStore::compare_and_set`].
///
/// The token's meaning is store-defined: [`MemoryObjectStore`] uses a monotonic
/// write counter, while [`LocalObjectStore`] uses the SHA-256 content hash. Two
/// writes of identical bytes therefore share a version on the local store but
/// not in memory — treat versions as opaque and never compare them across
/// stores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectVersion(pub String);

/// An object's bytes together with its current [`ObjectVersion`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    /// The stored bytes.
    pub value: Bytes,
    /// The version token for these bytes.
    pub version: ObjectVersion,
}

/// Outcome of [`ObjectStore::put_if_absent`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    /// The object did not exist and was written.
    Created,
    /// The object already existed with byte-identical content (idempotent no-op).
    AlreadyExistsSame,
}

/// Conditional-write capabilities advertised by an [`ObjectStore`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreCapabilities {
    /// Whether [`ObjectStore::compare_and_set`] is supported.
    pub compare_and_set: bool,
    /// Whether [`ObjectStore::put_if_absent`] is supported.
    pub put_if_absent: bool,
}

/// A minimal async object store: the storage *port* the log is built on.
///
/// Implement this for your own backend (e.g. an S3 client) to durably store the
/// log elsewhere. The log relies on the conditional writes
/// ([`put_if_absent`](ObjectStore::put_if_absent) /
/// [`compare_and_set`](ObjectStore::compare_and_set)) being atomic per key.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Fetch an object and its version, or `None` if absent.
    async fn get(&self, key: &ObjectKey) -> Result<Option<StoredObject>, ObjectLogError>;
    /// Write `value` only if the key is absent. Re-writing identical bytes is an
    /// idempotent [`PutOutcome::AlreadyExistsSame`]; different bytes conflict
    /// with [`ObjectLogError::ObjectConflict`].
    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        value: Bytes,
    ) -> Result<PutOutcome, ObjectLogError>;
    /// Atomically replace the object iff its current version equals `expected`
    /// (`None` meaning "must not exist"), returning the newly stored object. A
    /// version mismatch yields [`ObjectLogError::Conflict`].
    async fn compare_and_set(
        &self,
        key: &ObjectKey,
        expected: Option<ObjectVersion>,
        value: Bytes,
    ) -> Result<StoredObject, ObjectLogError>;
    /// List keys beginning with `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectKey>, ObjectLogError>;
    /// Delete an object; deleting a missing key is a no-op success.
    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectLogError>;
    /// Report which conditional writes this store supports.
    fn capabilities(&self) -> StoreCapabilities;
}

/// In-process [`ObjectStore`] backed by a `BTreeMap`. For tests and development;
/// state is not shared across processes and is lost on drop. Versions are a
/// monotonic per-key write counter.
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

/// Filesystem-backed [`ObjectStore`] rooted at a directory.
///
/// Objects are written atomically (temp file + rename) and versioned by SHA-256
/// content hash. Conditional writes are serialized by an in-process lock, so
/// atomicity holds **only within a single process** — do not point two processes
/// at the same root and expect compare-and-set to be safe. Suitable for
/// single-node use and tests against a real filesystem.
#[derive(Clone)]
pub struct LocalObjectStore {
    root: Arc<PathBuf>,
    lock: Arc<Mutex<()>>,
}

impl LocalObjectStore {
    /// Create a store rooted at `root` (created lazily on first write).
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
                    version: ObjectVersion(sha256_hex(&value)),
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
        // Append (not replace-extension) so the temp name is unique per key and
        // never collides with a sibling key, e.g. `a.olseg` -> `a.olseg.olog-tmp`.
        let mut tmp = OsString::from(path.as_os_str());
        tmp.push(TMP_SUFFIX);
        let tmp = PathBuf::from(tmp);
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
            version: ObjectVersion(sha256_hex(&value)),
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
        } else if !path.to_string_lossy().ends_with(TMP_SUFFIX)
            && let Ok(relative) = path.strip_prefix(root)
        {
            out.push(relative.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}
