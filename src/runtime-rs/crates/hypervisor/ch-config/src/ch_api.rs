// Copyright (c) 2022-2023 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

use crate::{
    DeviceConfig, DiskConfig, FsConfig, NetConfig, VmConfig, VmInfo, VmResize, VsockConfig,
};
use anyhow::{anyhow, Context, Result};
use api_client::{
    simple_api_full_command_and_response, simple_api_full_command_with_fds_and_response,
};

use serde::{Deserialize, Serialize};
use std::os::{fd::RawFd, unix::net::UnixStream};
use tokio::task;

pub async fn cloud_hypervisor_vmm_ping(mut socket: UnixStream) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(&mut socket, "GET", "vmm.ping", None)
            .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vmm_shutdown(mut socket: UnixStream) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response =
            simple_api_full_command_and_response(&mut socket, "PUT", "vmm.shutdown", None)
                .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_create(
    mut socket: UnixStream,
    cfg: VmConfig,
) -> Result<Option<String>> {
    let serialised = serde_json::to_string_pretty(&cfg)?;

    task::spawn_blocking(move || -> Result<Option<String>> {
        let data = Some(serialised.as_str());

        let response = simple_api_full_command_and_response(&mut socket, "PUT", "vm.create", data)
            .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_start(mut socket: UnixStream) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(&mut socket, "PUT", "vm.boot", None)
            .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

#[allow(dead_code)]
pub async fn cloud_hypervisor_vm_stop(mut socket: UnixStream) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response =
            simple_api_full_command_and_response(&mut socket, "PUT", "vm.shutdown", None)
                .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

#[derive(Deserialize, Debug)]
pub struct PciDeviceInfo {
    pub id: String,
    pub bdf: String,
}

#[derive(Clone, Deserialize, Serialize, Default, Debug)]
pub struct VmRemoveDeviceData {
    #[serde(default)]
    pub id: String,
}

pub async fn cloud_hypervisor_vm_blockdev_add(
    mut socket: UnixStream,
    blk_config: DiskConfig,
) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(
            &mut socket,
            "PUT",
            "vm.add-disk",
            Some(&serde_json::to_string(&blk_config)?),
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_netdev_add(
    mut socket: UnixStream,
    net_config: NetConfig,
) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(
            &mut socket,
            "PUT",
            "vm.add-net",
            Some(&serde_json::to_string(&net_config)?),
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_netdev_add_with_fds(
    mut socket: UnixStream,
    net_config: NetConfig,
    request_fds: Vec<RawFd>,
) -> Result<Option<String>> {
    let serialised = serde_json::to_string(&net_config)?;

    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_with_fds_and_response(
            &mut socket,
            "PUT",
            "vm.add-net",
            Some(&serialised),
            &request_fds,
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_device_add(
    mut socket: UnixStream,
    device_config: DeviceConfig,
) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(
            &mut socket,
            "PUT",
            "vm.add-device",
            Some(&serde_json::to_string(&device_config)?),
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

#[allow(dead_code)]
pub async fn cloud_hypervisor_vm_device_remove(
    mut socket: UnixStream,
    device_data: VmRemoveDeviceData,
) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(
            &mut socket,
            "PUT",
            "vm.remove-device",
            Some(&serde_json::to_string(&device_data)?),
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_fs_add(
    mut socket: UnixStream,
    fs_config: FsConfig,
) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(
            &mut socket,
            "PUT",
            "vm.add-fs",
            Some(&serde_json::to_string(&fs_config)?),
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_vsock_add(
    mut socket: UnixStream,
    vsock_config: VsockConfig,
) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(
            &mut socket,
            "PUT",
            "vm.add-vsock",
            Some(&serde_json::to_string(&vsock_config)?),
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_info(mut socket: UnixStream) -> Result<VmInfo> {
    let vm_info = task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(&mut socket, "GET", "vm.info", None)
            .map_err(|e| anyhow!(format!("failed to run get vminfo with err: {:?}", e)))?;

        Ok(response)
    })
    .await??;

    let vm_info = vm_info.ok_or(anyhow!("failed to get vminfo"))?;
    serde_json::from_str(&vm_info).with_context(|| format!("failed to serde {vm_info}"))
}

pub async fn cloud_hypervisor_vm_resize(
    mut socket: UnixStream,
    vmresize: VmResize,
) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(
            &mut socket,
            "PUT",
            "vm.resize",
            Some(&serde_json::to_string(&vmresize)?),
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_pause(mut socket: UnixStream) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(&mut socket, "PUT", "vm.pause", None)
            .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_resume(mut socket: UnixStream) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(&mut socket, "PUT", "vm.resume", None)
            .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

/// Configuration for VM snapshot.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct VmSnapshotConfig {
    pub destination_url: String,
}

/// Configuration for VM restore.
///
/// The `memory_restore_mode` field controls how guest memory is populated from the
/// snapshot. When set to `"OnDemand"`, Cloud Hypervisor uses `userfaultfd` to lazily
/// fault pages in on first access rather than eagerly copying the entire snapshot.
/// This requires a Cloud Hypervisor build from the `main` branch (post PR #7800).
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RestoreConfig {
    pub source_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefault: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_restore_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<bool>,
}

pub async fn cloud_hypervisor_vm_snapshot(
    mut socket: UnixStream,
    snapshot_config: VmSnapshotConfig,
) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(
            &mut socket,
            "PUT",
            "vm.snapshot",
            Some(&serde_json::to_string(&snapshot_config)?),
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}

pub async fn cloud_hypervisor_vm_restore(
    mut socket: UnixStream,
    restore_config: RestoreConfig,
) -> Result<Option<String>> {
    task::spawn_blocking(move || -> Result<Option<String>> {
        let response = simple_api_full_command_and_response(
            &mut socket,
            "PUT",
            "vm.restore",
            Some(&serde_json::to_string(&restore_config)?),
        )
        .map_err(|e| anyhow!(e))?;

        Ok(response)
    })
    .await?
}
