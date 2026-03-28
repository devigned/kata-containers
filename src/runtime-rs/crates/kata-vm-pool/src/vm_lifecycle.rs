// Copyright 2026 Kata Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! VM lifecycle operations for Cloud Hypervisor.

use crate::DaemonConfig;
use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

/// Spawn a Cloud Hypervisor process with the given API socket path.
pub async fn spawn_ch(config: &DaemonConfig, api_socket: &Path) -> Result<u32> {
    let child = Command::new(&config.ch_path)
        .arg("--api-socket")
        .arg(api_socket)
        .arg("-v")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawn cloud-hypervisor")?;

    let pid = child.id().ok_or_else(|| anyhow!("no PID for CH process"))?;
    tracing::debug!("spawned CH PID={}", pid);

    // Detach — we track by PID, not child handle
    tokio::spawn(async move {
        let mut child = child;
        let stderr = child.stderr.take();
        if let Some(stderr) = stderr {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::trace!("ch[{}]: {}", pid, line);
            }
        }
        let _ = child.wait().await;
    });

    Ok(pid)
}

/// Wait for the CH API socket to become ready by polling vmm.ping.
pub async fn wait_ch_ready(api_socket: &Path) -> Result<()> {
    let deadline = Duration::from_secs(10);
    let poll_interval = Duration::from_millis(50);

    timeout(deadline, async {
        loop {
            match api_request(api_socket, "GET", "/api/v1/vmm.ping", None).await {
                Ok(response) => {
                    tracing::info!("vmm.ping succeeded: {:?}", response);
                    if response.is_some() {
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::debug!("vmm.ping failed: {:#}", e);
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    })
    .await
    .map_err(|_| anyhow!("CH API socket not ready within {:?}", deadline))?
}

/// Prepare a per-VM snapshot directory from the base snapshot.
/// Hardlinks state.json, symlinks memory-ranges, patches config.json.
pub fn prepare_snapshot(
    base_dir: &Path,
    vm_state_dir: &Path,
    new_vsock_socket: &Path,
) -> Result<PathBuf> {
    let snapshot_dir = vm_state_dir.join("snapshot");
    if snapshot_dir.exists() {
        std::fs::remove_dir_all(&snapshot_dir)?;
    }
    std::fs::create_dir_all(&snapshot_dir)?;

    // Copy config.json and state.json (small), symlink memory-ranges (large)
    std::fs::copy(base_dir.join("config.json"), snapshot_dir.join("config.json"))
        .context("copy config.json")?;
    std::fs::copy(base_dir.join("state.json"), snapshot_dir.join("state.json"))
        .context("copy state.json")?;
    std::os::unix::fs::symlink(
        base_dir.join("memory-ranges"),
        snapshot_dir.join("memory-ranges"),
    )
    .context("symlink memory-ranges")?;

    // Patch vsock socket path in config.json
    let config_json = std::fs::read_to_string(snapshot_dir.join("config.json"))?;
    let config: serde_json::Value = serde_json::from_str(&config_json)?;

    let old_socket = config
        .pointer("/vsock/socket")
        .and_then(|v| v.as_str())
        .map(String::from);

    if let Some(old) = old_socket {
        let old_dir = Path::new(&old)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        let new_dir = new_vsock_socket
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        if old_dir != new_dir {
            let patched = config_json.replace(&old_dir, &new_dir);
            std::fs::write(snapshot_dir.join("config.json"), patched)?;
        }
    }

    Ok(snapshot_dir)
}

/// Restore a VM from a snapshot with OnDemand memory mode.
pub async fn restore_vm(api_socket: &Path, snapshot_dir: &Path) -> Result<()> {
    let body = serde_json::json!({
        "source_url": format!("file://{}", snapshot_dir.display()),
        "prefault": false,
        "memory_restore_mode": "OnDemand",
    });
    api_request(
        api_socket,
        "PUT",
        "/api/v1/vm.restore",
        Some(&body.to_string()),
    )
    .await?;
    Ok(())
}

/// Resume a paused/restored VM.
pub async fn resume_vm(api_socket: &Path) -> Result<()> {
    api_request(api_socket, "PUT", "/api/v1/vm.resume", None).await?;
    Ok(())
}

/// Shutdown the VMM process.
pub async fn shutdown_vmm(api_socket: &Path) -> Result<()> {
    let _ = api_request(api_socket, "PUT", "/api/v1/vmm.shutdown", None).await;
    Ok(())
}

/// Create a base snapshot from a fresh VM boot.
pub async fn create_base_snapshot(config: &DaemonConfig) -> Result<()> {
    let vm_dir = config.state_dir.join("base-vm");
    std::fs::create_dir_all(&vm_dir)?;
    let api_socket = vm_dir.join("ch-api.sock");
    let vsock_socket = vm_dir.join("ch-vm.sock");

    // Boot a fresh VM
    let ch_pid = spawn_ch(config, &api_socket).await?;
    wait_ch_ready(&api_socket).await?;

    // Create VM config
    let vm_config = serde_json::json!({
        "payload": {
            "kernel": config.kernel_path,
            "cmdline": "console=hvc0 root=/dev/vda1 rw quiet"
        },
        "memory": {
            "size": (config.default_memory_mb as u64) * 1024 * 1024,
            "shared": true
        },
        "cpus": {
            "boot_vcpus": 1,
            "max_vcpus": config.max_vcpus
        },
        "disks": [{
            "path": config.rootfs_path,
            "readonly": true
        }],
        "vsock": {
            "cid": 3,
            "socket": vsock_socket
        },
        "serial": {"mode": "Off"},
        "console": {"mode": "Off"}
    });

    api_request(
        &api_socket,
        "PUT",
        "/api/v1/vm.create",
        Some(&vm_config.to_string()),
    )
    .await
    .context("vm.create")?;

    api_request(&api_socket, "PUT", "/api/v1/vm.boot", None)
        .await
        .context("vm.boot")?;

    // Wait for the guest to settle
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pause and snapshot
    api_request(&api_socket, "PUT", "/api/v1/vm.pause", None)
        .await
        .context("vm.pause")?;

    std::fs::create_dir_all(&config.snapshot_dir)?;
    let snapshot_body = serde_json::json!({
        "destination_url": format!("file://{}", config.snapshot_dir.display()),
    });
    api_request(
        &api_socket,
        "PUT",
        "/api/v1/vm.snapshot",
        Some(&snapshot_body.to_string()),
    )
    .await
    .context("vm.snapshot")?;

    // Shutdown the template VM
    shutdown_vmm(&api_socket).await?;
    // Wait for process exit
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Clean up temp dir
    let _ = std::fs::remove_dir_all(&vm_dir);

    tracing::info!(
        "base snapshot created at {:?} (PID {} stopped)",
        config.snapshot_dir,
        ch_pid
    );
    Ok(())
}

/// Wait for the guest agent to be reachable on the vsock socket.
pub async fn wait_for_agent(vsock_socket: &Path) -> Result<()> {
    let deadline = Duration::from_secs(10);
    let poll_interval = Duration::from_millis(50);

    timeout(deadline, async {
        loop {
            match UnixStream::connect(vsock_socket).await {
                Ok(mut stream) => {
                    use tokio::io::AsyncWriteExt;
                    // Hybrid vsock handshake: "connect <port>\n"
                    if stream
                        .write_all(b"connect 1024\n")
                        .await
                        .is_ok()
                    {
                        use tokio::io::AsyncReadExt;
                        let mut buf = [0u8; 64];
                        if let Ok(n) = stream.read(&mut buf).await {
                            let response = String::from_utf8_lossy(&buf[..n]);
                            if response.contains("OK") {
                                return Ok(());
                            }
                        }
                    }
                }
                Err(_) => {}
            }
            tokio::time::sleep(poll_interval).await;
        }
    })
    .await
    .map_err(|_| anyhow!("agent not reachable on {:?} within {:?}", vsock_socket, deadline))?
}

/// Make an HTTP API request to a CH Unix socket.
/// Public wrapper for api_request, used by snapshot_cache.
pub async fn api_request_pub(
    socket_path: &Path,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<Option<String>> {
    api_request(socket_path, method, path, body).await
}

/// Uses raw HTTP over blocking UnixStream with read timeout.
async fn api_request(
    socket_path: &Path,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<Option<String>> {
    let socket_path = socket_path.to_path_buf();
    let method = method.to_string();
    let path = path.to_string();
    let body = body.map(String::from);

    tokio::task::spawn_blocking(move || -> Result<Option<String>> {
        use std::io::{Read, Write};

        let stream = std::os::unix::net::UnixStream::connect(&socket_path)
            .with_context(|| format!("connect to {:?}", socket_path))?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .ok();
        let mut stream = stream;

        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAccept: */*\r\n"
        );
        if let Some(ref body) = body {
            request.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        request.push_str("\r\n");
        if let Some(ref body) = body {
            request.push_str(body);
        }

        stream.write_all(request.as_bytes())?;
        stream.flush()?;

        let mut buf = [0u8; 8192];
        let n = stream.read(&mut buf)?;
        let text = String::from_utf8_lossy(&buf[..n]).to_string();

        if text.contains("200") || text.contains("204") {
            if let Some(pos) = text.find("\r\n\r\n") {
                let body = text[pos + 4..].to_string();
                if body.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(body));
            }
            return Ok(None);
        }

        Err(anyhow!("API error: {}", text))
    })
    .await?
}
