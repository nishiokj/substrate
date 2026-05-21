use crate::effects::now_string;
use crate::error::{ExecutionerError, Result};
use crate::protocol::{ResourceRef, Session, WorkspaceArtifact, WorkspaceArtifactEntry};
use crate::workspace::ensure_workspace_root_is_still_safe;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

const MAX_WORKSPACE_ARTIFACT_ENTRIES: usize = 10_000;
const MAX_WORKSPACE_ARTIFACT_DEPTH: usize = 256;
const MAX_WORKSPACE_ARTIFACT_MANIFEST_BYTES: usize = 10 * 1024 * 1024;
const MAX_WORKSPACE_ARTIFACT_BYTES: u64 = 100 * 1024 * 1024;
const TAR_BLOCK_BYTES: u64 = 512;
const TAR_EOF_BLOCK_BYTES: u64 = TAR_BLOCK_BYTES * 2;

pub fn export_workspace(session: &Session, output_dir: &Path) -> Result<WorkspaceArtifact> {
    export_workspace_excluding(session, output_dir, &[])
}

pub fn export_workspace_excluding(
    session: &Session,
    output_dir: &Path,
    excluded_roots: &[PathBuf],
) -> Result<WorkspaceArtifact> {
    let workspace_root_path = PathBuf::from(&session.workspace.root);
    ensure_workspace_root_is_still_safe(&workspace_root_path)?;
    let workspace_root = workspace_root_path.canonicalize()?;
    let output_parent = output_dir.parent().ok_or_else(|| {
        ExecutionerError::InvalidRequest(
            "workspace artifact output path must have a parent".to_string(),
        )
    })?;
    validate_no_symlinked_parent(output_parent, "workspace artifact output directory parent")?;
    fs::create_dir_all(output_dir)?;
    validate_export_output_dir(output_dir)?;

    let artifact_id = format!("workspace-{}", Uuid::new_v4().simple());
    let tar_path = output_dir.join(format!("{artifact_id}.tar"));
    let manifest_path = output_dir.join(format!("{artifact_id}.manifest.json"));
    let tmp_tar_path = output_dir.join(format!(".{artifact_id}.tar.tmp"));
    let tmp_manifest_path = output_dir.join(format!(".{artifact_id}.manifest.json.tmp"));
    let output_root = output_dir.canonicalize()?;
    if output_root == workspace_root {
        let _ = fs::remove_file(&tmp_tar_path);
        let _ = fs::remove_file(&tmp_manifest_path);
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact output directory must not be the workspace root".to_string(),
        ));
    }
    let mut excluded_roots = excluded_roots
        .iter()
        .filter_map(|path| path.canonicalize().ok())
        .collect::<Vec<_>>();
    if output_root.starts_with(&workspace_root) {
        excluded_roots.push(output_root);
    }

    let mut paths = Vec::<PathBuf>::new();
    collect_workspace_paths(&workspace_root, &excluded_roots, 0, &mut paths)?;
    paths.sort_by(|left, right| {
        left.strip_prefix(&workspace_root)
            .unwrap_or(left)
            .cmp(right.strip_prefix(&workspace_root).unwrap_or(right))
    });

    let tar_file = fs::File::create(&tmp_tar_path)?;
    let mut builder = tar::Builder::new(tar_file);
    let mut entries = Vec::<WorkspaceArtifactEntry>::new();
    let mut file_count = 0_usize;
    let mut directory_count = 0_usize;
    let mut symlink_count = 0_usize;
    let mut estimated_artifact_bytes = TAR_EOF_BLOCK_BYTES;

    let result: Result<()> = (|| {
        for path in paths {
            let metadata = fs::symlink_metadata(&path)?;
            let archive_path = archive_path(&workspace_root, &path)?;
            let logical_path = logical_path_for_archive_path(&archive_path);

            if metadata.file_type().is_symlink() {
                if let Some(link_target) = safe_symlink_target(&workspace_root, &path) {
                    symlink_count += 1;
                    entries.push(WorkspaceArtifactEntry {
                        logical_path,
                        archive_path,
                        kind: "symlink".to_string(),
                        link_target: Some(link_target),
                        bytes: None,
                        hash: None,
                    });
                }
                continue;
            }

            if metadata.is_dir() {
                estimated_artifact_bytes = checked_artifact_size_with_entry(
                    estimated_artifact_bytes,
                    tar_directory_entry_size(&archive_path),
                    &logical_path,
                )?;
                directory_count += 1;
                append_directory(&mut builder, &archive_path)?;
                entries.push(WorkspaceArtifactEntry {
                    logical_path,
                    archive_path,
                    kind: "directory".to_string(),
                    link_target: None,
                    bytes: None,
                    hash: None,
                });
                continue;
            }

            if metadata.is_file() {
                if metadata.len() > MAX_WORKSPACE_ARTIFACT_BYTES {
                    return Err(ExecutionerError::InvalidRequest(format!(
                        "workspace artifact file exceeds maximum size of {MAX_WORKSPACE_ARTIFACT_BYTES} bytes: {logical_path}"
                    )));
                }
                estimated_artifact_bytes = checked_artifact_size_with_entry(
                    estimated_artifact_bytes,
                    tar_file_entry_size(&archive_path, metadata.len()),
                    &logical_path,
                )?;
                file_count += 1;
                let hash = append_file(&mut builder, &archive_path, &path, metadata.len())?;
                entries.push(WorkspaceArtifactEntry {
                    logical_path,
                    archive_path,
                    kind: "file".to_string(),
                    link_target: None,
                    bytes: Some(metadata.len()),
                    hash: Some(hash),
                });
            }
        }
        builder.finish()?;
        Ok(())
    })();
    if let Err(err) = result {
        drop(builder);
        let _ = fs::remove_file(&tmp_tar_path);
        let _ = fs::remove_file(&tmp_manifest_path);
        return Err(err);
    }
    drop(builder);

    let (artifact_hash, artifact_bytes) = hash_file(&tmp_tar_path)?;
    if artifact_bytes > MAX_WORKSPACE_ARTIFACT_BYTES {
        let _ = fs::remove_file(&tmp_tar_path);
        let _ = fs::remove_file(&tmp_manifest_path);
        return Err(ExecutionerError::InvalidRequest(format!(
            "workspace artifact exceeds maximum size of {MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
        )));
    }
    let created_at = now_string();

    let artifact = WorkspaceArtifact {
        session_id: session.id.clone(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: file_uri(&tar_path),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: file_uri(&manifest_path),
        },
        format: "tar".to_string(),
        bytes: artifact_bytes,
        hash: artifact_hash,
        file_count,
        directory_count,
        symlink_count,
        entries,
        created_at,
    };

    let manifest_bytes = serde_json::to_vec_pretty(&artifact)?;
    fs::write(&tmp_manifest_path, manifest_bytes)?;
    fs::rename(&tmp_tar_path, &tar_path)?;
    fs::rename(&tmp_manifest_path, &manifest_path)?;

    Ok(artifact)
}

pub fn materialize_workspace_artifact(
    artifact: &WorkspaceArtifact,
    destination: &Path,
) -> Result<()> {
    validate_destination(destination)?;
    let destination_parent = destination.parent().ok_or_else(|| {
        ExecutionerError::InvalidRequest("materialize destination must have a parent".to_string())
    })?;
    validate_no_symlinked_parent(destination_parent, "materialize destination parent")?;
    let cleanup_parent = absolute_path(destination_parent)?;
    let cleanup_stop = nearest_existing_ancestor(&cleanup_parent);
    if let Err(err) = fs::create_dir_all(destination_parent) {
        cleanup_created_empty_parents(&cleanup_parent, cleanup_stop.as_deref());
        return Err(err.into());
    }
    let staging = destination_parent.join(format!(
        ".substrate-materialize-{}",
        Uuid::new_v4().simple()
    ));
    if let Err(err) = fs::create_dir(&staging) {
        cleanup_created_empty_parents(&cleanup_parent, cleanup_stop.as_deref());
        return Err(err.into());
    }

    let result = materialize_workspace_artifact_into(artifact, &staging);
    match result {
        Ok(()) => {
            if destination.exists() {
                fs::remove_dir(destination)?;
            }
            match fs::rename(&staging, destination) {
                Ok(()) => Ok(()),
                Err(err) => {
                    let _ = fs::remove_dir_all(&staging);
                    cleanup_created_empty_parents(&cleanup_parent, cleanup_stop.as_deref());
                    Err(err.into())
                }
            }
        }
        Err(err) => {
            let _ = fs::remove_dir_all(&staging);
            cleanup_created_empty_parents(&cleanup_parent, cleanup_stop.as_deref());
            Err(err)
        }
    }
}

fn materialize_workspace_artifact_into(
    artifact: &WorkspaceArtifact,
    destination: &Path,
) -> Result<()> {
    let destination = destination.canonicalize()?;
    validate_artifact_header(artifact)?;

    let tar_path = path_from_file_uri(&artifact.artifact.uri)?;
    let tar_bytes = read_workspace_artifact_bytes(&tar_path)?;
    let actual_hash = hash_bytes(&tar_bytes);
    let actual_bytes = tar_bytes.len() as u64;
    if actual_hash != artifact.hash || actual_bytes != artifact.bytes {
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact hash or byte length mismatch".to_string(),
        ));
    }
    validate_tar_end_of_archive(&tar_bytes)?;
    validate_manifest_resource_if_available(artifact)?;

    let entries = validate_manifest_entries(artifact, &destination)?;
    let mut seen_archive_entries = HashSet::<String>::new();
    let mut archive = tar::Archive::new(Cursor::new(tar_bytes));

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?;
        let archive_path = safe_archive_path(entry_path.as_ref())?;
        let manifest_entry = entries.get(&archive_path).ok_or_else(|| {
            ExecutionerError::InvalidRequest(format!(
                "artifact contains entry missing from manifest: {archive_path}"
            ))
        })?;
        if !seen_archive_entries.insert(archive_path.clone()) {
            return Err(ExecutionerError::InvalidRequest(format!(
                "duplicate artifact entry: {archive_path}"
            )));
        }

        let target_path = destination.join(&archive_path);
        let entry_type = entry.header().entry_type();
        match manifest_entry.kind.as_str() {
            "directory" if entry_type.is_dir() => fs::create_dir_all(&target_path)?,
            "file" if entry_type.is_file() => {
                if let Some(parent) = target_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let mut output = fs::File::create_new(&target_path)?;
                let mut reader = HashingReader::new(&mut entry);
                let bytes = std::io::copy(&mut reader, &mut output)?;
                output.flush()?;
                let hash = reader.finish();
                if manifest_entry.bytes != Some(bytes)
                    || manifest_entry.hash.as_ref() != Some(&hash)
                {
                    return Err(ExecutionerError::InvalidRequest(format!(
                        "artifact entry hash or byte length mismatch: {archive_path}"
                    )));
                }
            }
            _ => {
                return Err(ExecutionerError::InvalidRequest(format!(
                    "artifact entry type does not match manifest: {archive_path}"
                )));
            }
        }
    }

    for entry in &artifact.entries {
        match entry.kind.as_str() {
            "file" if !seen_archive_entries.contains(&entry.archive_path) => {
                return Err(ExecutionerError::InvalidRequest(format!(
                    "manifest file missing from artifact: {}",
                    entry.archive_path
                )));
            }
            "directory" if !seen_archive_entries.contains(&entry.archive_path) => {
                return Err(ExecutionerError::InvalidRequest(format!(
                    "manifest directory missing from artifact: {}",
                    entry.archive_path
                )));
            }
            _ => {}
        }
    }

    materialize_manifest_symlinks(artifact, &destination)?;
    Ok(())
}

fn collect_workspace_paths(
    root: &Path,
    excluded_roots: &[PathBuf],
    depth: usize,
    paths: &mut Vec<PathBuf>,
) -> Result<()> {
    if depth > MAX_WORKSPACE_ARTIFACT_DEPTH {
        return Err(ExecutionerError::InvalidRequest(format!(
            "workspace artifact exceeds maximum directory depth of {MAX_WORKSPACE_ARTIFACT_DEPTH}"
        )));
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if excluded_roots
            .iter()
            .any(|excluded| path.starts_with(excluded))
        {
            continue;
        }
        if paths.len() >= MAX_WORKSPACE_ARTIFACT_ENTRIES {
            return Err(ExecutionerError::InvalidRequest(format!(
                "workspace artifact exceeds maximum entry count of {MAX_WORKSPACE_ARTIFACT_ENTRIES}"
            )));
        }
        paths.push(path.clone());
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            collect_workspace_paths(&path, excluded_roots, depth + 1, paths)?;
        }
    }
    Ok(())
}

fn archive_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let archive_path = relative.to_str().ok_or_else(|| {
        ExecutionerError::InvalidRequest(
            "unsupported workspace path is not valid UTF-8".to_string(),
        )
    })?;
    if archive_path.contains('\\') {
        return Err(ExecutionerError::InvalidRequest(format!(
            "unsupported workspace path contains backslash: {}",
            archive_path
        )));
    }
    Ok(archive_path.to_string())
}

fn path_from_file_uri(uri: &str) -> Result<PathBuf> {
    let path_text = uri.strip_prefix("file://").ok_or_else(|| {
        ExecutionerError::InvalidRequest("artifact uri must be file://".to_string())
    })?;
    if !path_text.starts_with('/') {
        return Err(ExecutionerError::InvalidRequest(
            "artifact file uri must be absolute".to_string(),
        ));
    }
    if path_text.starts_with("//") || path_text.contains('?') || path_text.contains('#') {
        return Err(ExecutionerError::InvalidRequest(
            "artifact file uri must be a local file:/// absolute path without authority, query, or fragment".to_string(),
        ));
    }
    let path = PathBuf::from(path_text);
    if !path.is_absolute() {
        return Err(ExecutionerError::InvalidRequest(
            "artifact file uri must be absolute".to_string(),
        ));
    }
    Ok(path)
}

fn validate_destination(destination: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(destination) {
        if metadata.file_type().is_symlink() {
            return Err(ExecutionerError::InvalidRequest(
                "materialize destination must not be a symlink".to_string(),
            ));
        }
        if !metadata.is_dir() {
            return Err(ExecutionerError::InvalidRequest(
                "materialize destination must be a directory".to_string(),
            ));
        }
        if metadata.is_dir() && fs::read_dir(destination)?.next().is_some() {
            return Err(ExecutionerError::InvalidRequest(
                "materialize destination must be empty".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_export_output_dir(output_dir: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(output_dir)?;
    if metadata.file_type().is_symlink() {
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact output directory must not be a symlink".to_string(),
        ));
    }
    if !metadata.is_dir() {
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact output path must be a directory".to_string(),
        ));
    }
    Ok(())
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn nearest_existing_ancestor(path: &Path) -> Option<PathBuf> {
    let mut current = path.to_path_buf();
    loop {
        if current.exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn cleanup_created_empty_parents(parent: &Path, stop: Option<&Path>) {
    let mut current = parent.to_path_buf();
    loop {
        if stop.is_some_and(|stop| current == stop) {
            return;
        }
        if fs::remove_dir(&current).is_err() {
            return;
        }
        if !current.pop() {
            return;
        }
    }
}

fn validate_no_symlinked_parent(parent: &Path, label: &str) -> Result<()> {
    let mut current = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        std::env::current_dir()?.join(parent)
    };
    loop {
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                if !is_platform_root_symlink(&current) {
                    return Err(ExecutionerError::InvalidRequest(format!(
                        "{label} must not contain symlinks"
                    )));
                }
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        if !current.pop() {
            return Ok(());
        }
    }
}

fn is_platform_root_symlink(path: &Path) -> bool {
    matches!(path.to_str(), Some("/var" | "/tmp" | "/etc"))
}

fn validate_manifest_entries(
    artifact: &WorkspaceArtifact,
    destination: &Path,
) -> Result<HashMap<String, WorkspaceArtifactEntry>> {
    validate_artifact_header(artifact)?;
    validate_manifest_counts(artifact)?;

    let mut entries = HashMap::new();
    let mut total_file_bytes = 0_u64;
    for entry in &artifact.entries {
        let archive_path = safe_archive_path(Path::new(&entry.archive_path))?;
        if archive_path != entry.archive_path {
            return Err(ExecutionerError::InvalidRequest(format!(
                "manifest entry path is not canonical: {}",
                entry.archive_path
            )));
        }
        if !entry.logical_path.starts_with("/workspace/") {
            return Err(ExecutionerError::InvalidRequest(format!(
                "manifest logical path must be under /workspace: {}",
                entry.logical_path
            )));
        }
        let expected_logical_path = logical_path_for_archive_path(&archive_path);
        if entry.logical_path != expected_logical_path {
            return Err(ExecutionerError::InvalidRequest(format!(
                "manifest logical path does not match archive path: {}",
                entry.archive_path
            )));
        }
        match entry.kind.as_str() {
            "file" => {
                if entry.bytes.is_none() || entry.hash.is_none() || entry.link_target.is_some() {
                    return Err(ExecutionerError::InvalidRequest(format!(
                        "manifest file entry is incomplete: {}",
                        entry.archive_path
                    )));
                }
                let bytes = entry.bytes.unwrap();
                total_file_bytes =
                    total_file_bytes
                        .checked_add(bytes)
                        .ok_or_else(|| {
                            ExecutionerError::InvalidRequest(format!(
                                "workspace artifact manifest file bytes exceed maximum size of {MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
                            ))
                        })?;
                if bytes > MAX_WORKSPACE_ARTIFACT_BYTES
                    || total_file_bytes > MAX_WORKSPACE_ARTIFACT_BYTES
                {
                    return Err(ExecutionerError::InvalidRequest(format!(
                        "workspace artifact manifest file bytes exceed maximum size of {MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
                    )));
                }
            }
            "directory" => {
                if entry.bytes.is_some() || entry.hash.is_some() || entry.link_target.is_some() {
                    return Err(ExecutionerError::InvalidRequest(format!(
                        "manifest directory entry has file metadata: {}",
                        entry.archive_path
                    )));
                }
            }
            "symlink" => {
                let Some(target) = &entry.link_target else {
                    return Err(ExecutionerError::InvalidRequest(format!(
                        "manifest symlink entry is incomplete: {}",
                        entry.archive_path
                    )));
                };
                if entry.bytes.is_some() || entry.hash.is_some() {
                    return Err(ExecutionerError::InvalidRequest(format!(
                        "manifest symlink entry has file metadata: {}",
                        entry.archive_path
                    )));
                }
                validate_materialized_symlink_target(destination, &archive_path, target)?;
            }
            _ => {
                return Err(ExecutionerError::InvalidRequest(format!(
                    "unknown manifest entry kind: {}",
                    entry.kind
                )));
            }
        }
        if entries.insert(archive_path, entry.clone()).is_some() {
            return Err(ExecutionerError::InvalidRequest(format!(
                "duplicate manifest entry: {}",
                entry.archive_path
            )));
        }
    }
    validate_manifest_parent_directories(&entries)?;
    Ok(entries)
}

fn validate_manifest_parent_directories(
    entries: &HashMap<String, WorkspaceArtifactEntry>,
) -> Result<()> {
    for archive_path in entries.keys() {
        let path = Path::new(archive_path);
        let mut parent = path.parent();
        while let Some(parent_path) = parent {
            if parent_path.as_os_str().is_empty() {
                break;
            }
            let parent_archive_path = parent_path.to_string_lossy().into_owned();
            let Some(parent_entry) = entries.get(&parent_archive_path) else {
                return Err(ExecutionerError::InvalidRequest(format!(
                    "manifest parent directory missing for {archive_path}: {parent_archive_path}"
                )));
            };
            if parent_entry.kind != "directory" {
                return Err(ExecutionerError::InvalidRequest(format!(
                    "manifest parent path is not a directory for {archive_path}: {parent_archive_path}"
                )));
            }
            parent = parent_path.parent();
        }
    }
    Ok(())
}

fn validate_artifact_header(artifact: &WorkspaceArtifact) -> Result<()> {
    if artifact.format != "tar" {
        return Err(ExecutionerError::InvalidRequest(format!(
            "unsupported workspace artifact format: {}",
            artifact.format
        )));
    }
    if artifact.artifact.resource_type != "artifact" {
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact resource type must be artifact".to_string(),
        ));
    }
    if artifact.manifest.resource_type != "artifact_manifest" {
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact manifest resource type must be artifact_manifest".to_string(),
        ));
    }
    if artifact.bytes > MAX_WORKSPACE_ARTIFACT_BYTES {
        return Err(ExecutionerError::InvalidRequest(format!(
            "workspace artifact exceeds maximum size of {MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_manifest_resource_if_available(artifact: &WorkspaceArtifact) -> Result<()> {
    if !artifact.manifest.uri.starts_with("file://") {
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact manifest uri must be file://".to_string(),
        ));
    }
    let manifest_path = path_from_file_uri(&artifact.manifest.uri)?;
    if fs::symlink_metadata(&manifest_path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact manifest resource must be a regular file".to_string(),
        ));
    }
    let Some(manifest_bytes) = read_capped_file(
        &manifest_path,
        MAX_WORKSPACE_ARTIFACT_MANIFEST_BYTES,
        "workspace artifact manifest resource",
    )?
    else {
        return Ok(());
    };
    let manifest_artifact: WorkspaceArtifact = serde_json::from_slice(&manifest_bytes)?;
    if &manifest_artifact != artifact {
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact manifest resource does not match artifact metadata".to_string(),
        ));
    }
    Ok(())
}

fn read_capped_file(path: &Path, max_bytes: usize, label: &str) -> Result<Option<Vec<u8>>> {
    let Some(mut file) = open_regular_resource_file_no_follow_optional(path, label)? else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take((max_bytes + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} exceeds maximum size of {max_bytes} bytes"
        )));
    }
    Ok(Some(bytes))
}

fn validate_tar_end_of_archive(bytes: &[u8]) -> Result<()> {
    let block_bytes = TAR_BLOCK_BYTES as usize;
    if !bytes.len().is_multiple_of(block_bytes) {
        return Err(ExecutionerError::InvalidRequest(
            "artifact tar is missing end-of-archive marker".to_string(),
        ));
    }
    let mut first_zero_block = None;
    for (index, block) in bytes.chunks_exact(block_bytes).enumerate() {
        if block.iter().all(|byte| *byte == 0) {
            first_zero_block = Some(index);
            break;
        }
    }
    let Some(first_zero_block) = first_zero_block else {
        return Err(ExecutionerError::InvalidRequest(
            "artifact tar is missing end-of-archive marker".to_string(),
        ));
    };
    let blocks = bytes.len() / block_bytes;
    if first_zero_block + 1 >= blocks {
        return Err(ExecutionerError::InvalidRequest(
            "artifact tar is missing end-of-archive marker".to_string(),
        ));
    }
    if bytes[(first_zero_block * block_bytes)..]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(ExecutionerError::InvalidRequest(
            "artifact tar contains trailing data after end of archive".to_string(),
        ));
    }
    Ok(())
}

fn validate_manifest_counts(artifact: &WorkspaceArtifact) -> Result<()> {
    if artifact.entries.len() > MAX_WORKSPACE_ARTIFACT_ENTRIES {
        return Err(ExecutionerError::InvalidRequest(format!(
            "workspace artifact exceeds maximum entry count of {MAX_WORKSPACE_ARTIFACT_ENTRIES}"
        )));
    }
    let file_count = artifact
        .entries
        .iter()
        .filter(|entry| entry.kind == "file")
        .count();
    let directory_count = artifact
        .entries
        .iter()
        .filter(|entry| entry.kind == "directory")
        .count();
    let symlink_count = artifact
        .entries
        .iter()
        .filter(|entry| entry.kind == "symlink")
        .count();
    if file_count != artifact.file_count
        || directory_count != artifact.directory_count
        || symlink_count != artifact.symlink_count
    {
        return Err(ExecutionerError::InvalidRequest(
            "manifest counts do not match entries".to_string(),
        ));
    }
    Ok(())
}

fn safe_archive_path(path: &Path) -> Result<String> {
    let path_text = path.to_str().ok_or_else(|| {
        ExecutionerError::InvalidRequest(
            "unsafe artifact path: path is not valid UTF-8".to_string(),
        )
    })?;
    if path_text.contains('\\') || path_text.contains('\0') {
        return Err(ExecutionerError::InvalidRequest(format!(
            "unsafe artifact path: {}",
            path_text
        )));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                return Err(ExecutionerError::InvalidRequest(format!(
                    "unsafe artifact path: {}",
                    path_text
                )));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(ExecutionerError::InvalidRequest(
            "artifact path must not be empty".to_string(),
        ));
    }
    if normalized.components().count() > MAX_WORKSPACE_ARTIFACT_DEPTH {
        return Err(ExecutionerError::InvalidRequest(format!(
            "artifact path exceeds maximum path depth of {MAX_WORKSPACE_ARTIFACT_DEPTH}: {path_text}"
        )));
    }
    normalized.to_str().map(str::to_string).ok_or_else(|| {
        ExecutionerError::InvalidRequest(
            "unsafe artifact path: path is not valid UTF-8".to_string(),
        )
    })
}

fn materialize_manifest_symlinks(artifact: &WorkspaceArtifact, destination: &Path) -> Result<()> {
    for entry in artifact
        .entries
        .iter()
        .filter(|entry| entry.kind == "symlink")
    {
        let target = entry
            .link_target
            .as_ref()
            .expect("symlink entries are validated before materialization");
        let archive_path = safe_archive_path(Path::new(&entry.archive_path))?;
        validate_materialized_symlink_target(destination, &archive_path, target)?;
        let link_path = destination.join(&archive_path);
        if let Some(parent) = link_path.parent() {
            fs::create_dir_all(parent)?;
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, &link_path)?;
    }
    Ok(())
}

fn validate_materialized_symlink_target(
    destination: &Path,
    archive_path: &str,
    target: &str,
) -> Result<()> {
    if target.contains('\0') || target.contains('\\') {
        return Err(ExecutionerError::InvalidRequest(format!(
            "unsafe symlink target in manifest: {archive_path}"
        )));
    }
    let target_path = Path::new(target);
    if target_path.is_absolute() {
        return Err(ExecutionerError::InvalidRequest(format!(
            "unsafe symlink target in manifest: {archive_path}"
        )));
    }
    let link_parent = destination
        .join(archive_path)
        .parent()
        .unwrap_or(destination)
        .to_path_buf();
    let normalized_target =
        normalize_lexically(&link_parent.join(target_path)).ok_or_else(|| {
            ExecutionerError::InvalidRequest(format!(
                "unsafe symlink target in manifest: {archive_path}"
            ))
        })?;
    let normalized_destination = normalize_lexically(destination).ok_or_else(|| {
        ExecutionerError::InvalidRequest("invalid materialize destination".to_string())
    })?;
    if !normalized_target.starts_with(normalized_destination) {
        return Err(ExecutionerError::InvalidRequest(format!(
            "unsafe symlink target in manifest: {archive_path}"
        )));
    }
    Ok(())
}

fn logical_path_for_archive_path(archive_path: &str) -> String {
    format!("/workspace/{archive_path}")
}

fn safe_symlink_target(workspace_root: &Path, path: &Path) -> Option<String> {
    let target = fs::read_link(path).ok()?;
    if target.is_absolute() {
        return None;
    }
    let target = target.to_str()?;
    if target.contains('\\') {
        return None;
    }

    let parent = path.parent().unwrap_or(workspace_root);
    let normalized_target = normalize_lexically(&parent.join(target))?;
    if !normalized_target.starts_with(normalize_lexically(workspace_root)?) {
        return None;
    }

    Some(target.to_string())
}

fn normalize_lexically(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    Some(normalized)
}

fn append_directory(builder: &mut tar::Builder<fs::File>, archive_path: &str) -> Result<()> {
    let mut header = deterministic_header();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_size(0);
    header.set_mode(0o755);
    header.set_cksum();
    builder.append_data(&mut header, archive_path, std::io::empty())?;
    Ok(())
}

fn checked_artifact_size_with_entry(
    current_bytes: u64,
    entry_bytes: u64,
    logical_path: &str,
) -> Result<u64> {
    let next_bytes = current_bytes
        .checked_add(entry_bytes)
        .ok_or_else(|| artifact_size_error(logical_path))?;
    if next_bytes > MAX_WORKSPACE_ARTIFACT_BYTES {
        return Err(artifact_size_error(logical_path));
    }
    Ok(next_bytes)
}

fn artifact_size_error(logical_path: &str) -> ExecutionerError {
    ExecutionerError::InvalidRequest(format!(
        "workspace artifact exceeds maximum size of {MAX_WORKSPACE_ARTIFACT_BYTES} bytes before adding {logical_path}"
    ))
}

fn tar_directory_entry_size(archive_path: &str) -> u64 {
    tar_path_metadata_size(archive_path) + TAR_BLOCK_BYTES
}

fn tar_file_entry_size(archive_path: &str, bytes: u64) -> u64 {
    tar_path_metadata_size(archive_path) + TAR_BLOCK_BYTES + tar_padded_size(bytes)
}

fn tar_path_metadata_size(archive_path: &str) -> u64 {
    if archive_path.len() <= 100 {
        return 0;
    }
    TAR_BLOCK_BYTES + tar_padded_size(archive_path.len() as u64 + 1)
}

fn tar_padded_size(bytes: u64) -> u64 {
    bytes
        .checked_add(TAR_BLOCK_BYTES - 1)
        .map(|value| value / TAR_BLOCK_BYTES * TAR_BLOCK_BYTES)
        .unwrap_or(u64::MAX)
}

fn append_file(
    builder: &mut tar::Builder<fs::File>,
    archive_path: &str,
    source_path: &Path,
    bytes: u64,
) -> Result<String> {
    let file = open_regular_file_no_follow(source_path, bytes)?;
    let mut header = deterministic_header();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(bytes);
    header.set_mode(0o644);
    header.set_cksum();
    let mut reader = HashingReader::new(file);
    builder.append_data(&mut header, archive_path, &mut reader)?;
    Ok(reader.finish())
}

#[cfg(unix)]
fn open_regular_file_no_follow(path: &Path, expected_bytes: u64) -> Result<fs::File> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != expected_bytes
    {
        return Err(ExecutionerError::InvalidRequest(format!(
            "workspace file changed during artifact export: {}",
            path.display()
        )));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_regular_file_no_follow(path: &Path, expected_bytes: u64) -> Result<fs::File> {
    let file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || metadata.len() != expected_bytes {
        return Err(ExecutionerError::InvalidRequest(format!(
            "workspace file changed during artifact export: {}",
            path.display()
        )));
    }
    Ok(file)
}

fn deterministic_header() -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header
}

fn file_uri(path: &Path) -> String {
    format!("file://{}", path.to_string_lossy())
}

fn hash_file(path: &Path) -> Result<(String, u64)> {
    let file = open_regular_resource_file_no_follow(path, "workspace artifact resource")?;
    if file.metadata()?.len() > MAX_WORKSPACE_ARTIFACT_BYTES {
        return Err(ExecutionerError::InvalidRequest(format!(
            "workspace artifact exceeds maximum size of {MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
        )));
    }
    let mut reader = HashingReader::new(file);
    let bytes = std::io::copy(&mut reader, &mut std::io::sink())?;
    Ok((reader.finish(), bytes))
}

fn hash_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn read_workspace_artifact_bytes(path: &Path) -> Result<Vec<u8>> {
    let Some(bytes) = read_capped_file(
        path,
        MAX_WORKSPACE_ARTIFACT_BYTES as usize,
        "workspace artifact resource",
    )?
    else {
        return Err(ExecutionerError::InvalidRequest(
            "workspace artifact resource is missing".to_string(),
        ));
    };
    Ok(bytes)
}

#[cfg(unix)]
fn open_regular_resource_file_no_follow(path: &Path, label: &str) -> Result<fs::File> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} must be a regular file"
        )));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} must be a regular file"
        )));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_regular_resource_file_no_follow(path: &Path, label: &str) -> Result<fs::File> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} must be a regular file"
        )));
    }
    Ok(fs::File::open(path)?)
}

fn open_regular_resource_file_no_follow_optional(
    path: &Path,
    label: &str,
) -> Result<Option<fs::File>> {
    match open_regular_resource_file_no_follow(path, label) {
        Ok(file) => Ok(Some(file)),
        Err(ExecutionerError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn finish(self) -> String {
        format!("sha256:{:x}", self.hasher.finalize())
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.inner.read(buf)?;
        if bytes_read > 0 {
            self.hasher.update(&buf[..bytes_read]);
        }
        Ok(bytes_read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_artifact_uri_must_be_absolute() {
        let err = path_from_file_uri("file://relative.tar").unwrap_err();

        assert!(err.to_string().contains("absolute"));
    }

    #[test]
    fn file_artifact_uri_rejects_authority_like_or_decorated_paths() {
        for uri in [
            "file:////tmp/workspace.tar",
            "file:///tmp/workspace.tar?download=1",
            "file:///tmp/workspace.tar#fragment",
        ] {
            let err = path_from_file_uri(uri).unwrap_err();

            assert!(
                err.to_string().contains("without authority"),
                "{uri}: {err}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn append_file_rejects_symlink_source_without_following_it() {
        let temp = tempfile::TempDir::new().unwrap();
        let outside = temp.path().join("outside.txt");
        let link = temp.path().join("link.txt");
        let tar_path = temp.path().join("out.tar");
        fs::write(&outside, "outside secret").unwrap();
        std::os::unix::fs::symlink(&outside, &link).unwrap();

        let tar_file = fs::File::create(tar_path).unwrap();
        let mut builder = tar::Builder::new(tar_file);
        let err = append_file(
            &mut builder,
            "link.txt",
            &link,
            fs::symlink_metadata(&link).unwrap().len(),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Too many levels of symbolic links")
                || err.to_string().contains("workspace file changed")
        );
    }
}
