// Copyright 2026 Kata Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! erofs rootfs conversion for VM snapshot restore.
//!
//! When restoring a VM from a template snapshot, virtio-fs is unavailable
//! (vhost-user reconnection and FUSE operations fail with hot-added devices).
//! Instead, we convert the container rootfs overlay to an erofs image on the
//! host, hot-plug it as a read-only block device, and the guest agent mounts
//! it directly — no FUSE, no shared filesystem.
//!
//! This follows the pattern from containerd-cloudhypervisor which achieves
//! 77ms cold starts using erofs + OnDemand restore.

use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};

const EROFS_CACHE_DIR: &str = "/run/vc/erofs-cache";

/// Convert a container rootfs directory to a cached erofs image.
///
/// The erofs image is cached at `/run/vc/erofs-cache/<cache_key>.erofs`.
/// Concurrent builds for the same image are serialized with `flock`.
/// If the cache already has the image, returns immediately.
pub fn prepare_erofs(rootfs_path: &Path, cache_key: &str) -> Result<PathBuf> {
    let cache_path = PathBuf::from(EROFS_CACHE_DIR).join(format!("{cache_key}.erofs"));

    if cache_path.exists() {
        return Ok(cache_path);
    }

    fs::create_dir_all(EROFS_CACHE_DIR).context("create erofs cache dir")?;

    // Serialize concurrent builds for the same image via flock
    let lock_path = PathBuf::from(EROFS_CACHE_DIR).join(format!("{cache_key}.lock"));
    let lock_file = fs::File::create(&lock_path).context("create erofs lock file")?;
    let fd = lock_file.as_raw_fd();
    let ret = unsafe { libc::flock(fd, libc::LOCK_EX) };
    if ret != 0 {
        return Err(anyhow!(
            "flock on erofs lock file: {}",
            std::io::Error::last_os_error()
        ));
    }

    // Re-check after acquiring lock (another process may have created it)
    if cache_path.exists() {
        return Ok(cache_path);
    }

    let tmp_path =
        PathBuf::from(EROFS_CACHE_DIR).join(format!("{cache_key}.{}.tmp", std::process::id()));

    let output = Command::new("mkfs.erofs")
        .arg(&tmp_path)
        .arg(rootfs_path)
        .output()
        .context("failed to execute mkfs.erofs")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Clean up temp file on failure
        let _ = fs::remove_file(&tmp_path);
        return Err(anyhow!("mkfs.erofs failed: {stderr}"));
    }

    fs::rename(&tmp_path, &cache_path).context("rename erofs temp to cache")?;

    Ok(cache_path)
}

/// Compute a stable cache key from a rootfs path.
///
/// Uses FNV-1a hash of the path string for fast, deterministic keying.
pub fn stable_cache_key(input: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}
