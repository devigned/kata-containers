// Copyright 2026 Kata Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Unix socket API server for the VM pool daemon.
//!
//! Protocol: newline-delimited JSON over a Unix stream socket.
//! Each request is a JSON object with an "action" field.

use crate::pool::Pool;
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

/// Serve the pool API on a Unix socket.
pub async fn serve(socket_path: PathBuf, pool: Arc<Pool>) -> Result<()> {
    // Remove stale socket
    let _ = std::fs::remove_file(&socket_path);

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind {:?}", socket_path))?;

    // Make socket accessible
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o660))?;
    }

    tracing::info!("pool API listening on {:?}", socket_path);

    loop {
        let (stream, _addr) = listener.accept().await?;
        let pool = Arc::clone(&pool);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, pool).await {
                tracing::warn!("connection error: {:#}", e);
            }
        });
    }
}

async fn handle_connection(
    stream: tokio::net::UnixStream,
    pool: Arc<Pool>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        let request: serde_json::Value = serde_json::from_str(&line)
            .with_context(|| format!("parse request: {}", line))?;

        let action = request
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let response = match action {
            "acquire" => handle_acquire(&pool, &request).await,
            "release" => {
                let vm_id = request
                    .get("vm_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                handle_release(&pool, vm_id).await
            }
            "status" => handle_status(&pool).await,
            _ => serde_json::json!({"error": format!("unknown action: {}", action)}),
        };

        let mut response_bytes = serde_json::to_vec(&response)?;
        response_bytes.push(b'\n');
        writer.write_all(&response_bytes).await?;
    }

    Ok(())
}

async fn handle_acquire(pool: &Arc<Pool>, request: &serde_json::Value) -> serde_json::Value {
    let image_key = request.get("image_key").and_then(|v| v.as_str());
    let erofs_path = request.get("erofs_path").and_then(|v| v.as_str());

    match pool.acquire(image_key, erofs_path).await {
        Ok(vm) => serde_json::json!({
            "vm_id": vm.vm_id,
            "api_socket": vm.api_socket,
            "vsock_socket": vm.vsock_socket,
            "ch_pid": vm.ch_pid,
            "rootfs_preattached": vm.rootfs_preattached,
        }),
        Err(e) => serde_json::json!({"error": format!("{:#}", e)}),
    }
}

async fn handle_release(pool: &Pool, vm_id: &str) -> serde_json::Value {
    match pool.release(vm_id).await {
        Ok(()) => serde_json::json!({"ok": true}),
        Err(e) => serde_json::json!({"error": format!("{:#}", e)}),
    }
}

async fn handle_status(pool: &Pool) -> serde_json::Value {
    let (ready, active) = pool.status().await;
    serde_json::json!({
        "ready": ready,
        "active": active,
    })
}
