// Copyright 2026 Kata Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! crun OCI runtime integration.
//!
//! Replaces rustjail's in-process container creation with crun subprocess
//! calls. This eliminates the fork+namespace+pivot_root overhead of rustjail
//! (~75ms) by delegating to crun's optimized C implementation (~20-30ms).
//!
//! crun must be installed at /usr/bin/crun in the guest rootfs image.

use anyhow::{anyhow, Context, Result};
use oci_spec::runtime::Spec;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const CRUN_PATH: &str = "/usr/bin/crun";
const CONTAINER_BASE: &str = "/run/kata-containers";

/// A container managed by crun.
#[derive(Debug)]
pub struct CrunContainer {
    pub id: String,
    pub bundle: PathBuf,
    pub pid: Option<u32>,
}

impl CrunContainer {
    /// Create and start a container using `crun run`.
    ///
    /// The OCI spec must already be written to `<bundle>/config.json`.
    /// crun handles namespace creation, cgroup setup, pivot_root, and exec.
    pub fn run(id: &str, bundle: &Path) -> Result<Self> {
        if !Path::new(CRUN_PATH).exists() {
            return Err(anyhow!("crun not found at {}", CRUN_PATH));
        }

        // crun run --detach creates + starts the container, then exits.
        // We use .output() to wait for crun to finish and capture errors.
        let output = Command::new(CRUN_PATH)
            .arg("run")
            .arg("--bundle")
            .arg(bundle)
            .arg("--detach")
            .arg(id)
            .stdin(Stdio::null())
            .output()
            .with_context(|| format!("execute crun run (bundle={})", bundle.display()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("crun run failed: {}", stderr));
        }

        // Query the container's init PID via `crun state`
        let pid = Self::query_pid(id)?;

        Ok(CrunContainer {
            id: id.to_string(),
            bundle: bundle.to_path_buf(),
            pid,
        })
    }

    /// Query the container's init PID via `crun state <id>`.
    fn query_pid(id: &str) -> Result<Option<u32>> {
        let output = Command::new(CRUN_PATH)
            .arg("state")
            .arg(id)
            .output()
            .context("crun state")?;

        if !output.status.success() {
            return Ok(None);
        }

        let state: serde_json::Value = serde_json::from_slice(&output.stdout)
            .context("parse crun state")?;
        Ok(state.get("pid").and_then(|p| p.as_u64()).map(|p| p as u32))
    }

    /// Wait for the container process to exit using `crun state` polling.
    pub fn wait(&self) -> Result<i32> {
        // Poll crun state until the container is stopped
        loop {
            let output = Command::new(CRUN_PATH)
                .arg("state")
                .arg(&self.id)
                .output();

            match output {
                Ok(out) if out.status.success() => {
                    let state: serde_json::Value = serde_json::from_slice(&out.stdout)
                        .unwrap_or_default();
                    let status = state.get("status")
                        .and_then(|s| s.as_str())
                        .unwrap_or("unknown");
                    if status == "stopped" {
                        return Ok(0);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                _ => return Ok(0), // container gone
            }
        }
    }

    /// Send a signal to the container.
    pub fn kill(&self, signal: i32) -> Result<()> {
        let output = Command::new(CRUN_PATH)
            .arg("kill")
            .arg(&self.id)
            .arg(signal.to_string())
            .output()
            .context("crun kill")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("crun kill failed: {}", stderr));
        }
        Ok(())
    }

    /// Delete the container.
    pub fn delete(&self) -> Result<()> {
        let output = Command::new(CRUN_PATH)
            .arg("delete")
            .arg("--force")
            .arg(&self.id)
            .output()
            .context("crun delete")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Don't error on "not found" — container may already be cleaned up
            if !stderr.contains("not found") {
                return Err(anyhow!("crun delete failed: {}", stderr));
            }
        }
        Ok(())
    }
}

/// Prepare the OCI bundle directory for crun.
///
/// Writes the OCI spec to `<bundle>/config.json` and ensures the rootfs
/// directory exists. This is the bridge between the Kata agent's storage
/// setup and crun's expectations.
pub fn prepare_bundle(container_id: &str, spec: &Spec) -> Result<PathBuf> {
    let bundle = PathBuf::from(CONTAINER_BASE).join(container_id);
    std::fs::create_dir_all(&bundle)
        .with_context(|| format!("create bundle dir {:?}", bundle))?;

    // Ensure rootfs directory exists
    let rootfs = bundle.join("rootfs");
    if !rootfs.exists() {
        std::fs::create_dir_all(&rootfs)?;
    }

    // Write OCI spec
    let config_path = bundle.join("config.json");
    let spec_json = serde_json::to_string_pretty(spec)
        .context("serialize OCI spec")?;
    std::fs::write(&config_path, spec_json)
        .context("write config.json")?;

    Ok(bundle)
}
