use crate::protocol::{Effect, EffectOperation, ResourceRef, StateRef};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const MAX_STATE_REF_HASH_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Default, Clone)]
pub struct EffectRecorder {
    effects: Vec<Effect>,
}

impl EffectRecorder {
    pub fn push(&mut self, effect: Effect) {
        self.effects.push(effect);
    }

    pub fn into_effects(self) -> Vec<Effect> {
        self.effects
    }

    pub fn record_file_read(
        &mut self,
        invocation_id: &str,
        logical_path: &str,
        state: Option<StateRef>,
    ) {
        self.push(Effect {
            id: Uuid::new_v4().to_string(),
            invocation_id: invocation_id.to_string(),
            kind: "file.read".to_string(),
            resource: ResourceRef {
                resource_type: "file".to_string(),
                uri: format!("file://{logical_path}"),
            },
            operation: EffectOperation::Read,
            before: state,
            after: None,
            summary: Some(format!("Read {logical_path}")),
            reversible: false,
            occurred_at: now_string(),
        });
    }

    pub fn record_file_write(
        &mut self,
        invocation_id: &str,
        logical_path: &str,
        before: Option<StateRef>,
        after: Option<StateRef>,
        created: bool,
    ) {
        self.push(Effect {
            id: Uuid::new_v4().to_string(),
            invocation_id: invocation_id.to_string(),
            kind: "file.write".to_string(),
            resource: ResourceRef {
                resource_type: "file".to_string(),
                uri: format!("file://{logical_path}"),
            },
            operation: if created {
                EffectOperation::Create
            } else {
                EffectOperation::Update
            },
            before,
            after,
            summary: Some(format!("Wrote {logical_path}")),
            reversible: true,
            occurred_at: now_string(),
        });
    }

    pub fn record_file_delete(
        &mut self,
        invocation_id: &str,
        logical_path: &str,
        before: Option<StateRef>,
    ) {
        self.push(Effect {
            id: Uuid::new_v4().to_string(),
            invocation_id: invocation_id.to_string(),
            kind: "file.delete".to_string(),
            resource: ResourceRef {
                resource_type: "file".to_string(),
                uri: format!("file://{logical_path}"),
            },
            operation: EffectOperation::Delete,
            before,
            after: None,
            summary: Some(format!("Deleted {logical_path}")),
            reversible: true,
            occurred_at: now_string(),
        });
    }

    pub fn record_process_exec(
        &mut self,
        invocation_id: &str,
        command: &str,
        exit_code: Option<i32>,
    ) {
        self.push(Effect {
            id: Uuid::new_v4().to_string(),
            invocation_id: invocation_id.to_string(),
            kind: "process.exec".to_string(),
            resource: ResourceRef {
                resource_type: "process".to_string(),
                uri: format!("process://{}", Uuid::new_v4().simple()),
            },
            operation: EffectOperation::Execute,
            before: None,
            after: None,
            summary: Some(format!(
                "Executed command with exit code {exit_code:?}: {command}"
            )),
            reversible: false,
            occurred_at: now_string(),
        });
    }

    pub fn record_network_request(&mut self, invocation_id: &str, url: &str, status: Option<u16>) {
        self.push(Effect {
            id: Uuid::new_v4().to_string(),
            invocation_id: invocation_id.to_string(),
            kind: "network.request".to_string(),
            resource: ResourceRef {
                resource_type: "network".to_string(),
                uri: url.to_string(),
            },
            operation: EffectOperation::Read,
            before: None,
            after: None,
            summary: Some(format!("Requested {url} with status {status:?}")),
            reversible: false,
            occurred_at: now_string(),
        });
    }
}

pub fn state_ref_for_file(path: &Path) -> std::io::Result<StateRef> {
    let (mut file, bytes) = open_regular_file_no_follow(path)?;
    if bytes > MAX_STATE_REF_HASH_BYTES {
        let mut metadata = Map::<String, Value>::new();
        metadata.insert("hashSkipped".to_string(), Value::Bool(true));
        metadata.insert(
            "hashLimitBytes".to_string(),
            Value::Number(MAX_STATE_REF_HASH_BYTES.into()),
        );
        return Ok(StateRef {
            hash: None,
            bytes: Some(bytes),
            content_ref: None,
            snapshot_ref: None,
            metadata,
        });
    }

    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(StateRef {
        hash: Some(format!("sha256:{:x}", hasher.finalize())),
        bytes: Some(bytes),
        content_ref: None,
        snapshot_ref: None,
        metadata: Map::<String, Value>::new(),
    })
}

#[cfg(unix)]
fn open_regular_file_no_follow(path: &Path) -> std::io::Result<(fs::File, u64)> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path is not a regular file",
        ));
    }
    Ok((file, metadata.len()))
}

#[cfg(not(unix))]
fn open_regular_file_no_follow(path: &Path) -> std::io::Result<(fs::File, u64)> {
    let file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path is not a regular file",
        ));
    }
    Ok((file, metadata.len()))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn now_string() -> String {
    format!("{:?}", std::time::SystemTime::now())
}

pub fn logical_file_uri(logical_path: &str) -> String {
    format!("file://{logical_path}")
}

pub fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

pub fn temp_file_path(target: &Path) -> PathBuf {
    let file_name = format!(".tmp_{}.tmp", Uuid::new_v4().simple());
    target
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn state_ref_rejects_symlink_without_following_it() {
        let temp = tempfile::TempDir::new().unwrap();
        let outside = temp.path().join("outside.txt");
        let link = temp.path().join("link.txt");
        fs::write(&outside, "outside secret").unwrap();
        std::os::unix::fs::symlink(&outside, &link).unwrap();

        let err = state_ref_for_file(&link).unwrap_err();

        assert!(
            err.to_string()
                .contains("Too many levels of symbolic links")
                || err.to_string().contains("path is not a regular file")
        );
    }
}
