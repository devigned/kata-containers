// Copyright (c) 2019-2022 Alibaba Cloud
// Copyright (c) 2019-2022 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

mod nydus_rootfs;
mod share_fs_rootfs;
pub mod erofs_rootfs;
use agent::Storage;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use kata_types::mount::Mount;
mod block_rootfs;
pub mod virtual_volume;

use hypervisor::{device::device_manager::DeviceManager, Hypervisor};
use virtual_volume::{is_kata_virtual_volume, VirtualVolume};

use std::{collections::HashMap, sync::Arc, vec::Vec};
use tokio::sync::RwLock;

use self::{block_rootfs::is_block_rootfs, nydus_rootfs::NYDUS_ROOTFS_TYPE};
use crate::share_fs::ShareFs;
use oci_spec::runtime as oci;

const ROOTFS: &str = "rootfs";
const HYBRID_ROOTFS_LOWER_DIR: &str = "rootfs_lower";
const TYPE_OVERLAY_FS: &str = "overlay";

#[async_trait]
pub trait Rootfs: Send + Sync {
    async fn get_guest_rootfs_path(&self) -> Result<String>;
    async fn get_rootfs_mount(&self) -> Result<Vec<oci::Mount>>;
    async fn get_storage(&self) -> Option<Storage>;
    async fn cleanup(&self, device_manager: &RwLock<DeviceManager>) -> Result<()>;
    async fn get_device_id(&self) -> Result<Option<String>>;
}

#[derive(Default)]
struct RootFsResourceInner {
    rootfs: Vec<Arc<dyn Rootfs>>,
}

pub struct RootFsResource {
    inner: Arc<RwLock<RootFsResourceInner>>,
}

impl Default for RootFsResource {
    fn default() -> Self {
        Self::new()
    }
}

impl RootFsResource {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RootFsResourceInner::default())),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn handler_rootfs(
        &self,
        share_fs: &Option<Arc<dyn ShareFs>>,
        device_manager: &RwLock<DeviceManager>,
        h: &dyn Hypervisor,
        sid: &str,
        cid: &str,
        root: &oci::Root,
        bundle_path: &str,
        rootfs_mounts: &[Mount],
        annotations: &HashMap<String, String>,
    ) -> Result<Arc<dyn Rootfs>> {
        match rootfs_mounts {
            // if rootfs_mounts is empty
            [] => {
                if let Some(share_fs) = share_fs {
                    // handle share fs rootfs
                    Ok(Arc::new(
                        share_fs_rootfs::ShareFsRootfs::new(
                            share_fs,
                            cid,
                            root.path().display().to_string().as_str(),
                            None,
                        )
                        .await
                        .context("new share fs rootfs")?,
                    ))
                } else {
                    Err(anyhow!("share fs is unavailable"))
                }
            }
            mounts_vec if is_single_layer_rootfs(mounts_vec) => {
                // Safe as single_layer_rootfs must have one layer
                let layer = &mounts_vec[0];
                let mut inner = self.inner.write().await;

                if is_guest_pull_volume(share_fs, layer) {
                    let mount_options = layer.options.clone();
                    let virtual_volume: Arc<dyn Rootfs> = Arc::new(
                        VirtualVolume::new(cid, annotations, mount_options.to_vec())
                            .await
                            .context("kata virtual volume failed.")?,
                    );
                    return Ok(virtual_volume);
                }

                let rootfs = if let Some((dev_id, layer)) = is_block_rootfs(layer) {
                    // handle block rootfs
                    info!(sl!(), "block device: {}", dev_id);
                    let block_rootfs: Arc<dyn Rootfs> = Arc::new(
                        block_rootfs::BlockRootfs::new(device_manager, sid, cid, dev_id, &layer)
                            .await
                            .context("new block rootfs")?,
                    );
                    Ok(block_rootfs)
                } else if let Some(share_fs) = share_fs {
                    // handle nydus rootfs
                    let share_rootfs: Arc<dyn Rootfs> = if layer.fs_type == NYDUS_ROOTFS_TYPE {
                        Arc::new(
                            nydus_rootfs::NydusRootfs::new(
                                device_manager,
                                share_fs,
                                h,
                                sid,
                                cid,
                                layer,
                            )
                            .await
                            .context("new nydus rootfs")?,
                        )
                    }
                    // handle sharefs rootfs
                    else {
                        Arc::new(
                            share_fs_rootfs::ShareFsRootfs::new(
                                share_fs,
                                cid,
                                bundle_path,
                                Some(layer),
                            )
                            .await
                            .context("new share fs rootfs")?,
                        )
                    };
                    Ok(share_rootfs)
                } else if layer.fs_type == TYPE_OVERLAY_FS {
                    // If the VM was restored from a per-image snapshot, the
                    // container rootfs disk is already attached as /dev/vdb.
                    // Skip erofs conversion and hot-plug entirely.
                    if h.is_rootfs_preattached().await {
                        let guest_rootfs_path = format!("/run/kata-rootfs/{cid}");
                        let guest_device = "/dev/vdb";
                        info!(sl!(), "rootfs preattached from image snapshot, using {}", guest_device);

                        let block_rootfs: Arc<dyn Rootfs> = Arc::new(
                            block_rootfs::BlockRootfs::new_preattached(
                                cid, guest_device, &guest_rootfs_path, "erofs",
                            ),
                        );
                        inner.rootfs.push(block_rootfs.clone());
                        return Ok(block_rootfs);
                    }

                    info!(sl!(), "converting overlay rootfs to erofs block device");

                    let rootfs_path = std::path::Path::new(bundle_path).join(ROOTFS);
                    // Mount the overlay at the bundle rootfs path first
                    layer
                        .mount(&rootfs_path)
                        .context("mount overlay for erofs conversion")?;

                    // Build cache key from source AND mount options to avoid
                    // collisions — overlay mounts share the same source string
                    // ("overlay") but differ in lowerdir/upperdir/workdir.
                    let mut key_material = layer.source.clone();
                    if !layer.options.is_empty() {
                        key_material.push('|');
                        key_material.push_str(&layer.options.join(","));
                    }
                    let cache_key = erofs_rootfs::stable_cache_key(&key_material);
                    let erofs_path = match erofs_rootfs::prepare_erofs(&rootfs_path, &cache_key) {
                        Ok(path) => {
                            if let Err(e) = nix::mount::umount(&rootfs_path) {
                                warn!(sl!(), "failed to unmount overlay after erofs conversion: {}", e);
                            }
                            path
                        }
                        Err(e) => {
                            if let Err(ue) = nix::mount::umount(&rootfs_path) {
                                warn!(sl!(), "failed to unmount overlay after erofs error: {}", ue);
                            }
                            return Err(e).context("prepare erofs image");
                        }
                    };

                    let guest_rootfs_path = format!("/run/kata-rootfs/{cid}");

                    let erofs_mount = Mount {
                        source: erofs_path.to_string_lossy().to_string(),
                        fs_type: "erofs".to_string(),
                        options: vec!["ro".to_string()],
                        ..Default::default()
                    };

                        let fstat = nix::sys::stat::stat(erofs_path.to_str().unwrap())
                            .context("stat erofs image")?;

                        let block_rootfs: Arc<dyn Rootfs> = Arc::new(
                            block_rootfs::BlockRootfs::new_with_guest_path(
                                device_manager,
                                cid,
                                fstat.st_ino,
                                &erofs_mount,
                                &guest_rootfs_path,
                            )
                            .await
                            .context("new erofs block rootfs")?,
                        );
                        Ok(block_rootfs)
                } else {
                    Err(anyhow!("unsupported rootfs {:?}", &layer))
                }?;
                inner.rootfs.push(rootfs.clone());
                Ok(rootfs)
            }
            _ => Err(anyhow!(
                "unsupported rootfs mounts count {}",
                rootfs_mounts.len()
            )),
        }
    }

    pub async fn dump(&self) {
        let inner = self.inner.read().await;
        for r in &inner.rootfs {
            info!(
                sl!(),
                "rootfs {:?}: count {}",
                r.get_guest_rootfs_path().await,
                Arc::strong_count(r)
            );
        }
    }
}

fn is_single_layer_rootfs(rootfs_mounts: &[Mount]) -> bool {
    rootfs_mounts.len() == 1
}

pub fn is_guest_pull_volume(
    share_fs: &Option<Arc<dyn ShareFs>>,
    m: &kata_types::mount::Mount,
) -> bool {
    share_fs.is_none() && is_kata_virtual_volume(m)
}
