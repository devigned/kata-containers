// Copyright 2026 Kata Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Pre-warmed VM pool daemon for Kata Containers.
//!
//! Maintains a pool of pre-restored Cloud Hypervisor VMs using OnDemand
//! (userfaultfd) memory restore. Shims acquire ready VMs from the pool
//! instead of restoring from snapshot on every container creation.
//!
//! Architecture follows containerd-cloudhypervisor's daemon pattern:
//! - Base snapshot created once at startup
//! - Pool of N VMs pre-restored and agent-ready
//! - Shims acquire via Unix socket, pool replenishes asynchronously
//! - ~6MB idle RSS per pooled VM via UFFD demand paging

mod pool;
mod server;
mod snapshot_cache;
mod vm_lifecycle;

use anyhow::{Context, Result};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct DaemonConfig {
    /// Number of VMs to keep pre-warmed in the pool.
    pub pool_size: usize,
    /// Path to the base snapshot directory (generic VM, no container rootfs).
    pub snapshot_dir: PathBuf,
    /// Directory for per-image snapshot cache (shadow snapshots).
    pub image_snapshot_dir: PathBuf,
    /// Path to the Cloud Hypervisor binary.
    pub ch_path: PathBuf,
    /// Path to the guest kernel.
    pub kernel_path: PathBuf,
    /// Path to the guest rootfs image.
    pub rootfs_path: PathBuf,
    /// Default guest memory in MB.
    pub default_memory_mb: u32,
    /// Maximum vCPUs.
    pub max_vcpus: u8,
    /// Unix socket path for the pool API.
    pub socket_path: PathBuf,
    /// State directory for per-VM runtime files.
    pub state_dir: PathBuf,
    /// Memory restore mode: "OnDemand" (UFFD) or "Eager" (full copy).
    pub memory_restore_mode: String,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            pool_size: 10,
            snapshot_dir: PathBuf::from("/run/vc/vm/template"),
            image_snapshot_dir: PathBuf::from("/run/vc/vm/image-snapshots"),
            ch_path: PathBuf::from("/opt/kata/bin/cloud-hypervisor-ondemand"),
            kernel_path: PathBuf::from(
                std::env::var("POOL_KERNEL")
                    .unwrap_or_else(|_| "/opt/kata/share/kata-containers/vmlinux.container".into()),
            ),
            rootfs_path: PathBuf::from(
                std::env::var("POOL_ROOTFS")
                    .unwrap_or_else(|_| "/opt/kata/share/kata-containers/kata-containers.img".into()),
            ),
            default_memory_mb: 256,
            max_vcpus: 8,
            socket_path: PathBuf::from("/run/kata/pool.sock"),
            state_dir: PathBuf::from("/run/kata/pool"),
            memory_restore_mode: std::env::var("POOL_RESTORE_MODE")
                .unwrap_or_else(|_| "OnDemand".to_string()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("kata_vm_pool=debug".parse().unwrap()),
        )
        .init();

    let config = DaemonConfig::default();

    tracing::info!("kata-vm-pool daemon starting (pool_size={}, restore_mode={})",
        config.pool_size, config.memory_restore_mode);

    // Ensure state directories exist
    std::fs::create_dir_all(&config.state_dir)
        .context("create state dir")?;
    std::fs::create_dir_all(&config.image_snapshot_dir)
        .context("create image snapshot dir")?;

    // Create base snapshot if it doesn't exist
    if !config.snapshot_dir.join("config.json").exists() {
        tracing::info!("creating base snapshot...");
        vm_lifecycle::create_base_snapshot(&config).await
            .context("create base snapshot")?;
    } else {
        tracing::info!("base snapshot exists at {:?}", config.snapshot_dir);
    }

    // Initialize snapshot cache and pool
    let snapshot_cache = snapshot_cache::SnapshotCache::new(config.clone());
    let pool = pool::Pool::new(config.clone(), snapshot_cache);
    pool.initialize().await.context("initialize pool")?;

    // Start API server
    tracing::info!("serving on {:?}", config.socket_path);
    server::serve(config.socket_path, pool).await
        .context("api server")?;

    Ok(())
}
