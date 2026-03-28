// Copyright 2026 Kata Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Pre-warmed VM pool with per-image snapshot cache.
//!
//! Maintains generic pre-restored VMs for instant acquisition. When the
//! shim provides an image_key + erofs_path, the pool checks for a cached
//! per-image snapshot first (warm path). On cache miss, it serves a generic
//! VM and triggers async shadow snapshot creation for next time.

use crate::{snapshot_cache::SnapshotCache, vm_lifecycle, DaemonConfig};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolVm {
    pub vm_id: String,
    pub api_socket: String,
    pub vsock_socket: String,
    pub ch_pid: u32,
    /// True if this VM was restored from a per-image snapshot
    /// (erofs rootfs already attached, no hot-plug needed).
    #[serde(default)]
    pub rootfs_preattached: bool,
    #[serde(skip)]
    pub created_at: Option<Instant>,
}

pub struct Pool {
    config: DaemonConfig,
    ready: Mutex<Vec<PoolVm>>,
    active: Mutex<std::collections::HashMap<String, PoolVm>>,
    next_id: AtomicU64,
    snapshot_cache: Arc<SnapshotCache>,
}

impl Pool {
    pub fn new(config: DaemonConfig, snapshot_cache: Arc<SnapshotCache>) -> Arc<Self> {
        Arc::new(Self {
            config,
            ready: Mutex::new(Vec::new()),
            active: Mutex::new(std::collections::HashMap::new()),
            next_id: AtomicU64::new(1),
            snapshot_cache,
        })
    }

    pub async fn initialize(self: &Arc<Self>) -> Result<()> {
        let target = self.config.pool_size;
        tracing::info!("initializing pool with {} VMs", target);

        for i in 0..target {
            match self.restore_one_base().await {
                Ok(vm) => {
                    tracing::info!(
                        "pool VM {}/{} ready: {} (PID={})",
                        i + 1, target, vm.vm_id, vm.ch_pid
                    );
                    self.ready.lock().await.push(vm);
                }
                Err(e) => {
                    tracing::error!("failed to create pool VM {}/{}: {:#}", i + 1, target, e);
                }
            }
        }

        let count = self.ready.lock().await.len();
        tracing::info!("pool initialized: {}/{} VMs ready", count, target);
        Ok(())
    }

    /// Acquire a VM. If image_key + erofs_path are provided, check the
    /// per-image snapshot cache first for a warm restore.
    pub async fn acquire(
        self: &Arc<Self>,
        image_key: Option<&str>,
        erofs_path: Option<&str>,
    ) -> Result<PoolVm> {
        // Try per-image snapshot cache (warm path)
        if let (Some(key), Some(_erofs)) = (image_key, erofs_path) {
            // Check in-memory cache first, then disk
            let snapshot = self.snapshot_cache.get(key).await
                .or_async(self.snapshot_cache.load_from_disk(key)).await;

            if let Some(snapshot) = snapshot {
                tracing::info!("image snapshot cache hit for {}", key);
                match self.restore_from_image_snapshot(&snapshot).await {
                    Ok(vm) => {
                        self.active.lock().await.insert(vm.vm_id.clone(), vm.clone());
                        return Ok(vm);
                    }
                    Err(e) => {
                        tracing::warn!("image snapshot restore failed, falling back to pool: {:#}", e);
                    }
                }
            }
        }

        // Generic pool (cold path for this image)
        let vm = self.ready.lock().await.pop();
        let vm = if let Some(vm) = vm {
            tracing::info!("acquired generic pool VM {} (instant)", vm.vm_id);
            vm
        } else {
            tracing::warn!("pool empty, restoring VM synchronously");
            self.restore_one_base().await.context("sync restore")?
        };

        self.active.lock().await.insert(vm.vm_id.clone(), vm.clone());

        // Replenish generic pool asynchronously
        let pool = Arc::clone(self);
        tokio::spawn(async move { pool.replenish_one().await });

        // Trigger async shadow snapshot creation for this image
        if let (Some(key), Some(erofs)) = (image_key, erofs_path) {
            let cache = Arc::clone(&self.snapshot_cache);
            let key = key.to_string();
            let erofs = erofs.to_string();
            let base_dir = self.config.snapshot_dir.clone();
            tokio::spawn(async move {
                if let Err(e) = cache.create_shadow(&key, &erofs, &base_dir).await {
                    tracing::error!("shadow snapshot creation failed for {}: {:#}", key, e);
                }
            });
        }

        Ok(vm)
    }

    pub async fn release(&self, vm_id: &str) -> Result<()> {
        let vm = self.active.lock().await.remove(vm_id);
        if let Some(vm) = vm {
            tracing::info!("releasing VM {} (PID={})", vm.vm_id, vm.ch_pid);
            let api_socket = PathBuf::from(&vm.api_socket);
            let _ = vm_lifecycle::shutdown_vmm(&api_socket).await;
            let _ = std::fs::remove_dir_all(self.config.state_dir.join(&vm.vm_id));
        }
        Ok(())
    }

    pub async fn status(&self) -> (usize, usize) {
        (self.ready.lock().await.len(), self.active.lock().await.len())
    }

    async fn replenish_one(self: &Arc<Self>) {
        if self.ready.lock().await.len() >= self.config.pool_size {
            return;
        }
        match self.restore_one_base().await {
            Ok(vm) => {
                tracing::info!("pool replenished: {} (PID={})", vm.vm_id, vm.ch_pid);
                self.ready.lock().await.push(vm);
            }
            Err(e) => tracing::error!("pool replenish failed: {:#}", e),
        }
    }

    /// Restore a generic VM from the base snapshot (no container rootfs).
    async fn restore_one_base(&self) -> Result<PoolVm> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let vm_id = format!("pool-vm-{id}");
        let vm_dir = self.config.state_dir.join(&vm_id);
        std::fs::create_dir_all(&vm_dir)?;

        let api_socket = vm_dir.join("ch-api.sock");
        let vsock_socket = vm_dir.join("ch-vm.sock");

        let snapshot_dir = vm_lifecycle::prepare_snapshot(
            &self.config.snapshot_dir, &vm_dir, &vsock_socket,
        ).context("prepare snapshot")?;

        let ch_pid = vm_lifecycle::spawn_ch(&self.config, &api_socket)
            .await.context("spawn CH")?;
        vm_lifecycle::wait_ch_ready(&api_socket).await?;
        vm_lifecycle::restore_vm(&api_socket, &snapshot_dir).await?;
        vm_lifecycle::resume_vm(&api_socket).await?;
        vm_lifecycle::wait_for_agent(&vsock_socket).await?;

        Ok(PoolVm {
            vm_id,
            api_socket: api_socket.to_string_lossy().to_string(),
            vsock_socket: vsock_socket.to_string_lossy().to_string(),
            ch_pid,
            rootfs_preattached: false,
            created_at: Some(Instant::now()),
        })
    }

    /// Restore a VM from a per-image snapshot (erofs rootfs already attached).
    async fn restore_from_image_snapshot(&self, snapshot: &crate::snapshot_cache::ImageSnapshot) -> Result<PoolVm> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let vm_id = format!("img-vm-{id}");
        let vm_dir = self.config.state_dir.join(&vm_id);
        std::fs::create_dir_all(&vm_dir)?;

        let api_socket = vm_dir.join("ch-api.sock");
        let vsock_socket = vm_dir.join("ch-vm.sock");

        let restore_dir = vm_lifecycle::prepare_snapshot(
            &snapshot.snapshot_dir, &vm_dir, &vsock_socket,
        ).context("prepare image snapshot")?;

        let ch_pid = vm_lifecycle::spawn_ch(&self.config, &api_socket)
            .await.context("spawn CH")?;
        vm_lifecycle::wait_ch_ready(&api_socket).await?;
        vm_lifecycle::restore_vm(&api_socket, &restore_dir).await?;
        vm_lifecycle::resume_vm(&api_socket).await?;
        vm_lifecycle::wait_for_agent(&vsock_socket).await?;

        Ok(PoolVm {
            vm_id,
            api_socket: api_socket.to_string_lossy().to_string(),
            vsock_socket: vsock_socket.to_string_lossy().to_string(),
            ch_pid,
            rootfs_preattached: true,
            created_at: Some(Instant::now()),
        })
    }
}

/// Helper trait for chaining Option async operations.
trait OrAsync<T> {
    async fn or_async<F: std::future::Future<Output = Option<T>>>(self, f: F) -> Option<T>;
}

impl<T> OrAsync<T> for Option<T> {
    async fn or_async<F: std::future::Future<Output = Option<T>>>(self, f: F) -> Option<T> {
        match self {
            Some(v) => Some(v),
            None => f.await,
        }
    }
}
