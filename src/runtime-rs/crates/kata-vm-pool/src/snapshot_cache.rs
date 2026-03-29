// Copyright 2026 Kata Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-image snapshot cache.
//!
//! After the first container run with a given image, a "shadow" snapshot
//! is created asynchronously that includes the erofs rootfs disk already
//! attached. Subsequent runs for the same image restore from this snapshot,
//! eliminating both erofs conversion and PCI hot-plug from the critical path.

use crate::{vm_lifecycle, DaemonConfig};
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Cached per-image snapshot metadata.
#[derive(Clone, Debug)]
pub struct ImageSnapshot {
    /// The image key (hash of image digest or name).
    pub image_key: String,
    /// Path to the erofs rootfs file.
    pub erofs_path: String,
    /// Path to the snapshot directory (config.json, memory-ranges, state.json).
    pub snapshot_dir: PathBuf,
}

/// Thread-safe cache of per-image snapshots.
pub struct SnapshotCache {
    config: DaemonConfig,
    cache: Mutex<HashMap<String, ImageSnapshot>>,
    /// Track in-progress shadow creations to avoid duplicate work.
    in_progress: Mutex<std::collections::HashSet<String>>,
}

impl SnapshotCache {
    pub fn new(config: DaemonConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            cache: Mutex::new(HashMap::new()),
            in_progress: Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// Look up a cached snapshot for the given image key.
    pub async fn get(&self, image_key: &str) -> Option<ImageSnapshot> {
        let cache = self.cache.lock().await;
        cache.get(image_key).cloned()
    }

    /// Check if a cached snapshot exists on disk for the given image key.
    pub async fn load_from_disk(&self, image_key: &str) -> Option<ImageSnapshot> {
        let snapshot_dir = self.config.image_snapshot_dir.join(image_key);
        if snapshot_dir.join("config.json").exists()
            && snapshot_dir.join("memory-ranges").exists()
            && snapshot_dir.join("state.json").exists()
        {
            // Check if the erofs path is stored alongside the snapshot
            let meta_path = snapshot_dir.join("erofs_path.txt");
            let erofs_path = std::fs::read_to_string(&meta_path).ok()?;
            let erofs_path = erofs_path.trim().to_string();

            // Verify the erofs file still exists
            if !Path::new(&erofs_path).exists() {
                tracing::warn!("cached snapshot for {} has stale erofs path: {}", image_key, erofs_path);
                return None;
            }

            let snapshot = ImageSnapshot {
                image_key: image_key.to_string(),
                erofs_path,
                snapshot_dir,
            };

            // Populate in-memory cache
            self.cache.lock().await.insert(image_key.to_string(), snapshot.clone());

            Some(snapshot)
        } else {
            None
        }
    }

    /// Create a shadow snapshot for the given image asynchronously.
    /// This acquires a base VM from the pool, hot-plugs the erofs disk,
    /// boots and connects the agent, then pauses and snapshots the VM.
    pub async fn create_shadow(
        self: &Arc<Self>,
        image_key: &str,
        erofs_path: &str,
        base_snapshot_dir: &Path,
    ) -> Result<ImageSnapshot> {
        let image_key = image_key.to_string();
        let erofs_path_str = erofs_path.to_string();
        let snapshot_dir = self.config.image_snapshot_dir.join(&image_key);

        // Skip if already cached
        if snapshot_dir.join("config.json").exists() {
            if let Some(snap) = self.load_from_disk(&image_key).await {
                return Ok(snap);
            }
        }

        // Skip if another task is already creating this snapshot
        {
            let mut in_progress = self.in_progress.lock().await;
            if in_progress.contains(&image_key) {
                return Err(anyhow!("shadow snapshot already in progress for {}", image_key));
            }
            in_progress.insert(image_key.clone());
        }

        tracing::info!("creating shadow snapshot for image {}", image_key);

        let vm_dir = self.config.state_dir.join(format!("shadow-{}", &image_key[..8.min(image_key.len())]));
        std::fs::create_dir_all(&vm_dir)?;

        let api_socket = vm_dir.join("ch-api.sock");
        let vsock_socket = vm_dir.join("ch-vm.sock");

        // Prepare snapshot from base template
        let restore_dir = vm_lifecycle::prepare_snapshot(
            base_snapshot_dir, &vm_dir, &vsock_socket,
        ).context("prepare shadow snapshot")?;

        // Spawn CH, restore, resume
        let ch_pid = vm_lifecycle::spawn_ch(&self.config, &api_socket)
            .await
            .context("spawn shadow CH")?;
        vm_lifecycle::wait_ch_ready(&api_socket).await?;
        vm_lifecycle::restore_vm(&api_socket, &restore_dir).await?;
        vm_lifecycle::resume_vm(&api_socket).await?;

        // Wait for agent
        vm_lifecycle::wait_for_agent(&vsock_socket).await
            .context("shadow: wait for agent")?;

        // Hot-plug the erofs rootfs disk
        let disk_body = serde_json::json!({
            "path": erofs_path_str,
            "readonly": true,
            "id": "_container_rootfs",
        });
        vm_lifecycle::api_request_pub(
            &api_socket, "PUT", "/api/v1/vm.add-disk",
            Some(&disk_body.to_string()),
        ).await.context("shadow: add-disk")?;

        // Brief settle for PCI enumeration
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Pause and snapshot
        vm_lifecycle::api_request_pub(&api_socket, "PUT", "/api/v1/vm.pause", None)
            .await.context("shadow: pause")?;

        std::fs::create_dir_all(&snapshot_dir)?;
        let snap_body = serde_json::json!({
            "destination_url": format!("file://{}", snapshot_dir.display()),
        });
        vm_lifecycle::api_request_pub(
            &api_socket, "PUT", "/api/v1/vm.snapshot",
            Some(&snap_body.to_string()),
        ).await.context("shadow: snapshot")?;

        // Store erofs path metadata
        std::fs::write(snapshot_dir.join("erofs_path.txt"), &erofs_path_str)?;

        // Shutdown shadow VM
        vm_lifecycle::shutdown_vmm(&api_socket).await?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let _ = std::fs::remove_dir_all(&vm_dir);

        let snapshot = ImageSnapshot {
            image_key: image_key.clone(),
            erofs_path: erofs_path_str,
            snapshot_dir,
        };

        self.cache.lock().await.insert(image_key.clone(), snapshot.clone());
        self.in_progress.lock().await.remove(&image_key);
        tracing::info!("shadow snapshot created for image {}", image_key);

        Ok(snapshot)
    }
}
