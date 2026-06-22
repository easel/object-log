//! Raw S3/Garage write-saturation benchmark.
//!
//! This intentionally bypasses `LogEngine` and Kafka. It answers one question:
//! can this process feed Garage PUTs fast enough to approach the network link?
//!
//! Required env:
//!   FJORD_GARAGE_ENDPOINT
//!   FJORD_GARAGE_BUCKET
//!   FJORD_GARAGE_KEY_ID
//!   FJORD_GARAGE_SECRET
//!
//! Tuning env:
//!   S3_SAT_TOTAL_BYTES       default 4294967296       (4 GiB)
//!   S3_SAT_OBJECT_BYTES      default 134217728        (128 MiB)
//!   S3_SAT_PART_BYTES        default 8388608          (8 MiB)
//!   S3_SAT_CONCURRENCY       default 8
//!   S3_SAT_MEMORY_BYTES      default 536870912        (512 MiB reporting/budget guard)
//!   S3_SAT_PREFIX            default object-log-saturate/<pid>
//!   S3_SAT_DELETE            default 1                (delete after timed write phase)
//!   S3_SAT_EVIDENCE_FILE     optional JSON summary path
//!
//! Run:
//!   cargo run --release --features s3 --example s3_saturate

#[cfg(not(feature = "s3"))]
fn main() {
    eprintln!("s3_saturate requires `--features s3`");
    std::process::exit(2);
}

#[cfg(feature = "s3")]
use bytes::{Bytes, BytesMut};
#[cfg(feature = "s3")]
use object_log::{BlobStore, S3BlobStore};
#[cfg(feature = "s3")]
use std::fs;
#[cfg(feature = "s3")]
use std::path::PathBuf;
#[cfg(feature = "s3")]
use std::sync::Arc;
#[cfg(feature = "s3")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "s3")]
use std::time::Instant;

#[cfg(feature = "s3")]
#[derive(Clone, Debug)]
struct Config {
    endpoint: String,
    region: String,
    bucket: String,
    key_id: String,
    secret: String,
    total_bytes: u64,
    object_bytes: usize,
    part_bytes: usize,
    concurrency: usize,
    memory_bytes: usize,
    prefix: String,
    delete_after: bool,
    evidence_file: Option<PathBuf>,
}

#[cfg(feature = "s3")]
fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[cfg(feature = "s3")]
fn env_required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

#[cfg(feature = "s3")]
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(feature = "s3")]
fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(feature = "s3")]
fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name).as_deref() {
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES") => true,
        Ok("0") | Ok("false") | Ok("FALSE") | Ok("no") | Ok("NO") => false,
        _ => default,
    }
}

#[cfg(feature = "s3")]
fn config() -> Config {
    let object_bytes = env_usize("S3_SAT_OBJECT_BYTES", 128 * 1024 * 1024);
    let part_bytes = env_usize("S3_SAT_PART_BYTES", 8 * 1024 * 1024);
    let concurrency = env_usize("S3_SAT_CONCURRENCY", 8).max(1);
    let memory_bytes = env_usize("S3_SAT_MEMORY_BYTES", 512 * 1024 * 1024);
    assert!(object_bytes > 0, "S3_SAT_OBJECT_BYTES must be > 0");
    assert!(
        part_bytes >= 5 * 1024 * 1024,
        "S3 part size must be >= 5 MiB"
    );
    assert!(
        memory_bytes >= part_bytes,
        "S3_SAT_MEMORY_BYTES must fit at least one part buffer"
    );

    Config {
        endpoint: env_required("FJORD_GARAGE_ENDPOINT"),
        region: env_string("FJORD_GARAGE_REGION", "garage"),
        bucket: env_required("FJORD_GARAGE_BUCKET"),
        key_id: env_required("FJORD_GARAGE_KEY_ID"),
        secret: env_required("FJORD_GARAGE_SECRET"),
        total_bytes: env_u64("S3_SAT_TOTAL_BYTES", 4 * 1024 * 1024 * 1024),
        object_bytes,
        part_bytes,
        concurrency,
        memory_bytes,
        prefix: env_string(
            "S3_SAT_PREFIX",
            &format!("object-log-saturate/{}", std::process::id()),
        ),
        delete_after: env_bool("S3_SAT_DELETE", true),
        evidence_file: std::env::var("S3_SAT_EVIDENCE_FILE")
            .ok()
            .map(PathBuf::from),
    }
}

#[cfg(feature = "s3")]
fn make_part(size: usize) -> Bytes {
    let mut out = BytesMut::with_capacity(size);
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    while out.len() < size {
        x ^= x >> 30;
        x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x ^= x >> 27;
        x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(size);
    out.freeze()
}

#[cfg(feature = "s3")]
fn object_chunks(part: &Bytes, object_bytes: usize, part_bytes: usize) -> Vec<Bytes> {
    let mut chunks = Vec::with_capacity(object_bytes.div_ceil(part_bytes));
    let mut remaining = object_bytes;
    while remaining > 0 {
        let n = remaining.min(part_bytes);
        chunks.push(part.slice(0..n));
        remaining -= n;
    }
    chunks
}

#[cfg(feature = "s3")]
fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(feature = "s3")]
fn rss_kib() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(pages * 4)
}

#[cfg(feature = "s3")]
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config();
    let objects = cfg.total_bytes.div_ceil(cfg.object_bytes as u64);
    let effective_total = objects * cfg.object_bytes as u64;
    let logical_inflight = cfg.concurrency.saturating_mul(cfg.object_bytes);
    let source_buffer_bytes = cfg.part_bytes;
    assert!(
        source_buffer_bytes <= cfg.memory_bytes,
        "source buffer exceeds S3_SAT_MEMORY_BYTES"
    );

    let store: Arc<dyn BlobStore> = Arc::new(
        S3BlobStore::new(
            &cfg.endpoint,
            &cfg.region,
            &cfg.bucket,
            &cfg.key_id,
            &cfg.secret,
        )
        .with_multipart(cfg.part_bytes, cfg.part_bytes),
    );
    let part = Arc::new(make_part(cfg.part_bytes));
    let next = Arc::new(AtomicU64::new(0));
    let bytes_done = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    eprintln!(
        "s3_saturate start: total={} object={} part={} concurrency={} logical_inflight={} source_buffer={} prefix={}",
        effective_total,
        cfg.object_bytes,
        cfg.part_bytes,
        cfg.concurrency,
        logical_inflight,
        source_buffer_bytes,
        cfg.prefix
    );

    let mut workers = Vec::with_capacity(cfg.concurrency);
    for _ in 0..cfg.concurrency {
        let store = Arc::clone(&store);
        let part = Arc::clone(&part);
        let next = Arc::clone(&next);
        let bytes_done = Arc::clone(&bytes_done);
        let prefix = cfg.prefix.clone();
        let object_bytes = cfg.object_bytes;
        let part_bytes = cfg.part_bytes;
        workers.push(tokio::spawn(async move {
            loop {
                let object = next.fetch_add(1, Ordering::Relaxed);
                if object >= objects {
                    break;
                }
                let key = format!("{prefix}/{object:020}");
                let chunks = object_chunks(&part, object_bytes, part_bytes);
                store.put_chunks(&key, chunks).await?;
                bytes_done.fetch_add(object_bytes as u64, Ordering::Relaxed);
            }
            Ok::<(), object_log::ObjectLogError>(())
        }));
    }

    for worker in workers {
        worker.await??;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let written = bytes_done.load(Ordering::Relaxed);
    let bytes_per_sec = written as f64 / elapsed.max(0.001);
    let mib_per_sec = bytes_per_sec / 1024.0 / 1024.0;
    let gbit_per_sec = bytes_per_sec * 8.0 / 1_000_000_000.0;
    let percent_target = gbit_per_sec * 100.0 / 2.5;
    let rss_kib = rss_kib().unwrap_or(0);

    eprintln!(
        "s3_saturate done: {:.1} MiB/s {:.3} Gbit/s ({:.1}% of 2.5 Gbit/s), elapsed={:.3}s rss_kib={}",
        mib_per_sec, gbit_per_sec, percent_target, elapsed, rss_kib
    );

    let summary = format!(
        concat!(
            "{{\n",
            "  \"endpoint\":\"{}\",\n",
            "  \"bucket\":\"{}\",\n",
            "  \"prefix\":\"{}\",\n",
            "  \"total_bytes\":{},\n",
            "  \"written_bytes\":{},\n",
            "  \"object_bytes\":{},\n",
            "  \"part_bytes\":{},\n",
            "  \"objects\":{},\n",
            "  \"concurrency\":{},\n",
            "  \"memory_budget_bytes\":{},\n",
            "  \"source_buffer_bytes\":{},\n",
            "  \"logical_inflight_bytes\":{},\n",
            "  \"rss_kib\":{},\n",
            "  \"elapsed_secs\":{:.3},\n",
            "  \"bytes_per_sec\":{:.0},\n",
            "  \"mib_per_sec\":{:.1},\n",
            "  \"gbit_per_sec\":{:.3},\n",
            "  \"percent_of_2_5_gbit\":{:.1},\n",
            "  \"delete_after\":{}\n",
            "}}\n"
        ),
        json_escape(&cfg.endpoint),
        json_escape(&cfg.bucket),
        json_escape(&cfg.prefix),
        effective_total,
        written,
        cfg.object_bytes,
        cfg.part_bytes,
        objects,
        cfg.concurrency,
        cfg.memory_bytes,
        source_buffer_bytes,
        logical_inflight,
        rss_kib,
        elapsed,
        bytes_per_sec,
        mib_per_sec,
        gbit_per_sec,
        percent_target,
        cfg.delete_after
    );
    print!("{summary}");
    if let Some(path) = &cfg.evidence_file {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &summary)?;
    }

    if cfg.delete_after {
        let cleanup_started = Instant::now();
        let next_delete = Arc::new(AtomicU64::new(0));
        let mut cleaners = Vec::with_capacity(cfg.concurrency);
        for _ in 0..cfg.concurrency {
            let store = Arc::clone(&store);
            let next_delete = Arc::clone(&next_delete);
            let prefix = cfg.prefix.clone();
            cleaners.push(tokio::spawn(async move {
                loop {
                    let object = next_delete.fetch_add(1, Ordering::Relaxed);
                    if object >= objects {
                        break;
                    }
                    let key = format!("{prefix}/{object:020}");
                    store.delete(&key).await?;
                }
                Ok::<(), object_log::ObjectLogError>(())
            }));
        }
        for cleaner in cleaners {
            cleaner.await??;
        }
        eprintln!(
            "s3_saturate cleanup complete in {:.3}s",
            cleanup_started.elapsed().as_secs_f64()
        );
    }

    Ok(())
}
