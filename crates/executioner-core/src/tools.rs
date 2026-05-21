use crate::effects::{state_ref_for_file, temp_file_path, EffectRecorder};
use crate::error::{ExecutionerError, Result};
use crate::host::{empty_metadata, validate_duration_limit, validate_output_limit};
use crate::protocol::{
    EnvPolicy, Session, ToolInvocationRequest, ToolInvocationResult, ToolResultStatus,
};
use crate::workspace::{AccessKind, ResolvedPath, WorkspaceResolver};
use regex::Regex;
use serde_json::{json, Map, Value};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use uuid::Uuid;
use wait_timeout::ChildExt;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

const DEFAULT_MAX_BYTES: usize = 100_000;
const DEFAULT_GREP_MAX_RESULTS: usize = 20;
const MAX_GREP_RESULTS: usize = 50;
const DEFAULT_GLOB_MAX_RESULTS: usize = 200;
const MAX_GLOB_RESULTS: usize = 1000;
const MAX_LIST_ENTRIES: usize = 1000;
const MAX_SEARCH_VISITED_ENTRIES: usize = 10_000;
const DEFAULT_MAX_DEPTH: usize = 20;
const MAX_GLOB_DEPTH: usize = 50;
const DEFAULT_BASH_TIMEOUT_SECS: u64 = 30;
const MAX_SEARCH_PATTERN_BYTES: usize = 16 * 1024;
const MAX_GREP_FILE_BYTES: u64 = 16 * 1024 * 1024;

pub fn read_file(
    session: &Session,
    request: ToolInvocationRequest,
) -> Result<ToolInvocationResult> {
    let started = Instant::now();
    let invocation_id = invocation_id(&request);
    if let Err(err) = reject_unexpected_args(
        &request.arguments,
        &["path", "maxBytes", "startLine", "endLine"],
    ) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let resolver = WorkspaceResolver::for_session(session)?;
    let mut effects = EffectRecorder::default();

    let path = match string_arg(&request.arguments, "path") {
        Ok(path) => path,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                started.elapsed().as_millis() as u64,
            ))
        }
    };
    let max_bytes_override = match optional_usize_arg(&request.arguments, "maxBytes") {
        Ok(value) => value,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                started.elapsed().as_millis() as u64,
            ))
        }
    };
    if let Some(max_bytes) = max_bytes_override {
        if let Err(err) = validate_output_limit("maxBytes", max_bytes) {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ));
        }
    }
    let max_bytes = effective_max_output_bytes(session, &request, max_bytes_override);
    let start_line = match optional_usize_arg(&request.arguments, "startLine") {
        Ok(value) => value,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                started.elapsed().as_millis() as u64,
            ))
        }
    };
    let end_line = match optional_usize_arg(&request.arguments, "endLine") {
        Ok(value) => value,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                started.elapsed().as_millis() as u64,
            ))
        }
    };
    if let Err(err) = validate_line_range(start_line, end_line) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            started.elapsed().as_millis() as u64,
        ));
    }

    let resolved = match resolver.resolve_read_target(request.cwd.as_deref(), &path) {
        Ok(resolved) => resolved,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                started.elapsed().as_millis() as u64,
            ))
        }
    };

    let (mut file, file_size) = match open_regular_file_no_follow(&resolved.host_path) {
        Ok(opened) => opened,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(tool_error(
                &request,
                &invocation_id,
                format!("File not found: {}", resolved.logical_path),
                started.elapsed().as_millis() as u64,
                empty_metadata(),
            ))
        }
        Err(err) => {
            return Ok(io_error_result(
                &request,
                &invocation_id,
                err,
                started.elapsed().as_millis() as u64,
            ))
        }
    };

    let before_state = state_ref_for_file(&resolved.host_path).ok();
    effects.record_file_read(&invocation_id, &resolved.logical_path, before_state);

    let file_size = file_size as usize;
    let mut content = if max_bytes == 0 {
        String::new()
    } else if file_size > max_bytes {
        let mut buffer = vec![0_u8; max_bytes];
        let bytes_read = file.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        let mut text = String::from_utf8_lossy(&buffer).into_owned();
        text.push_str(&format!("\n...[truncated, file size: {file_size} bytes]"));
        text
    } else {
        let mut bytes = Vec::with_capacity(file_size);
        file.read_to_end(&mut bytes)?;
        String::from_utf8_lossy(&bytes).into_owned()
    };

    let mut metadata_json = Map::new();
    metadata_json.insert("path".to_string(), json!(resolved.logical_path));
    metadata_json.insert("size".to_string(), json!(file_size));
    metadata_json.insert("action".to_string(), json!("read"));

    if start_line.is_some() || end_line.is_some() {
        let lines: Vec<&str> = content.split('\n').collect();
        let total_lines = lines.len();
        let start = start_line.unwrap_or(1).saturating_sub(1);
        let end = end_line.unwrap_or(total_lines).min(total_lines);
        let slice = if start < end { &lines[start..end] } else { &[] };
        content = format!(
            "// Lines {}-{} of {} total\n{}",
            start + 1,
            end,
            total_lines,
            slice.join("\n")
        );
        metadata_json.insert("totalLines".to_string(), json!(total_lines));
        if let Some(start_line) = start_line {
            metadata_json.insert("startLine".to_string(), json!(start_line));
        }
        if let Some(end_line) = end_line {
            metadata_json.insert("endLine".to_string(), json!(end_line));
        }
    }
    content = truncate_string(content, max_bytes);

    Ok(ToolInvocationResult {
        invocation_id,
        session_id: session.id.clone(),
        tool_name: "Read".to_string(),
        status: ToolResultStatus::Success,
        output: content,
        error: None,
        summary: Some(format!("Read {}", resolved.logical_path)),
        effects: effects.into_effects(),
        duration_ms: started.elapsed().as_millis() as u64,
        metadata: metadata_json,
    })
}

pub fn write_file(
    session: &Session,
    request: ToolInvocationRequest,
) -> Result<ToolInvocationResult> {
    let started = Instant::now();
    let invocation_id = invocation_id(&request);
    if let Err(err) = reject_unexpected_args(&request.arguments, &["path", "content"]) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let resolver = WorkspaceResolver::for_session(session)?;
    let mut effects = EffectRecorder::default();

    let path = match string_arg(&request.arguments, "path") {
        Ok(path) => path,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                started.elapsed().as_millis() as u64,
            ))
        }
    };
    let content = match request.arguments.get("content") {
        Some(Value::String(value)) => value.clone(),
        _ => {
            return Ok(tool_error(
                &request,
                &invocation_id,
                "content must be a string".to_string(),
                started.elapsed().as_millis() as u64,
                empty_metadata(),
            ))
        }
    };

    let resolved = match resolver.resolve_write_target(request.cwd.as_deref(), &path) {
        Ok(resolved) => resolved,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                started.elapsed().as_millis() as u64,
            ))
        }
    };

    if fs::symlink_metadata(&resolved.host_path).is_ok() {
        return Ok(tool_error(
            &request,
            &invocation_id,
            format!(
                "File already exists: {}. Use Edit to modify existing files.",
                resolved.logical_path
            ),
            started.elapsed().as_millis() as u64,
            empty_metadata(),
        ));
    }

    let parent = resolved.host_path.parent().ok_or_else(|| {
        ExecutionerError::InvalidRequest("write target has no parent".to_string())
    })?;
    if let Err(err) = resolver.ensure_parent_allowed_for_write(&resolved.host_path) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            started.elapsed().as_millis() as u64,
        ));
    }
    fs::create_dir_all(parent)?;
    if let Err(err) = resolver.ensure_existing_parent_for_write(&resolved.host_path) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            started.elapsed().as_millis() as u64,
        ));
    }
    atomic_create_new(&resolved.host_path, content.as_bytes())?;

    let after = state_ref_for_file(&resolved.host_path).ok();
    effects.record_file_write(&invocation_id, &resolved.logical_path, None, after, true);

    let line_count = content.split('\n').count();
    let preview = content
        .split('\n')
        .take(5)
        .collect::<Vec<&str>>()
        .join("\n");
    let suffix = if line_count > 5 {
        format!("\n... ({} more lines)", line_count - 5)
    } else {
        String::new()
    };

    let output = format!(
        "Created {} ({} bytes, {} lines)\n\nPreview:\n{}{}",
        resolved.logical_path,
        content.len(),
        line_count,
        preview,
        suffix
    );
    let output = truncate_string(output, effective_max_output_bytes(session, &request, None));

    let mut metadata_json = Map::new();
    metadata_json.insert("path".to_string(), json!(resolved.logical_path));
    metadata_json.insert("bytesWritten".to_string(), json!(content.len()));
    metadata_json.insert("action".to_string(), json!("write"));
    metadata_json.insert("atomic".to_string(), json!(true));

    Ok(ToolInvocationResult {
        invocation_id,
        session_id: session.id.clone(),
        tool_name: "Write".to_string(),
        status: ToolResultStatus::Success,
        output,
        error: None,
        summary: Some(format!("Created {}", resolved.logical_path)),
        effects: effects.into_effects(),
        duration_ms: started.elapsed().as_millis() as u64,
        metadata: metadata_json,
    })
}

pub fn edit_file(
    session: &Session,
    request: ToolInvocationRequest,
) -> Result<ToolInvocationResult> {
    let started = Instant::now();
    let invocation_id = invocation_id(&request);
    if let Err(err) = reject_unexpected_args(
        &request.arguments,
        &["path", "oldString", "newString", "replaceAll"],
    ) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let resolver = WorkspaceResolver::for_session(session)?;
    let mut effects = EffectRecorder::default();

    let path = match string_arg(&request.arguments, "path") {
        Ok(path) => path,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let old_string = match string_arg_allow_empty(&request.arguments, "oldString") {
        Ok(value) if !value.is_empty() => value,
        _ => {
            return Ok(tool_error(
                &request,
                &invocation_id,
                "Must provide 'oldString' and 'newString' for edit".to_string(),
                elapsed_ms(started),
                empty_metadata(),
            ))
        }
    };
    let new_string = match string_arg_allow_empty(&request.arguments, "newString") {
        Ok(value) => value,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let replace_all = match optional_bool_arg(&request.arguments, "replaceAll") {
        Ok(value) => value.unwrap_or(false),
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };

    let resolved = match resolver.resolve_existing(request.cwd.as_deref(), &path, AccessKind::Read)
    {
        Ok(resolved) => resolved,
        Err(ExecutionerError::Io(ref io)) if io.kind() == std::io::ErrorKind::NotFound => {
            return Ok(tool_error(
                &request,
                &invocation_id,
                format!("File not found for edit: {path}. Use Write to create new files."),
                elapsed_ms(started),
                empty_metadata(),
            ));
        }
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    if let Err(err) = resolver.resolve_existing(request.cwd.as_deref(), &path, AccessKind::Write) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }

    let (mut file, _) = match open_regular_file_no_follow(&resolved.host_path) {
        Ok(opened) => opened,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(tool_error(
                &request,
                &invocation_id,
                format!(
                    "File not found for edit: {}. Use Write to create new files.",
                    resolved.logical_path
                ),
                elapsed_ms(started),
                empty_metadata(),
            ))
        }
        Err(err) => {
            return Ok(io_error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let mut original = String::new();
    if let Err(err) = file.read_to_string(&mut original) {
        return Ok(io_error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let count = count_occurrences(&original, &old_string);
    if count == 0 {
        return Ok(tool_error(
            &request,
            &invocation_id,
            format!(
                "oldString not found in {}. Verify the exact text including whitespace.",
                resolved.logical_path
            ),
            elapsed_ms(started),
            metadata_with_path(&resolved.logical_path, "edit"),
        ));
    }
    if count > 1 && !replace_all {
        let first_idx = original.find(&old_string).unwrap_or(0);
        let snippet = context_snippet(&original, first_idx, old_string.len(), 30);
        return Ok(tool_error(
            &request,
            &invocation_id,
            format!(
                "oldString found {count} times - not unique. Add surrounding context to make unique, or use replaceAll=true. First occurrence near: ...{snippet}..."
            ),
            elapsed_ms(started),
            metadata_with_path(&resolved.logical_path, "edit"),
        ));
    }

    let before = state_ref_for_file(&resolved.host_path).ok();
    let new_content = if replace_all {
        original.replace(&old_string, &new_string)
    } else {
        original.replacen(&old_string, &new_string, 1)
    };
    atomic_write(&resolved.host_path, new_content.as_bytes())?;
    let after = state_ref_for_file(&resolved.host_path).ok();
    effects.record_file_write(&invocation_id, &resolved.logical_path, before, after, false);

    let replacements = if replace_all { count } else { 1 };
    let mut metadata_json = metadata_with_path(&resolved.logical_path, "edit");
    metadata_json.insert("bytesWritten".to_string(), json!(new_content.len()));
    metadata_json.insert("replacements".to_string(), json!(replacements));
    metadata_json.insert("atomic".to_string(), json!(true));

    let output = format!(
        "Edited {}\nReplaced {} occurrence(s)",
        resolved.logical_path, replacements
    );
    let output = truncate_string(output, effective_max_output_bytes(session, &request, None));

    Ok(ToolInvocationResult {
        invocation_id,
        session_id: session.id.clone(),
        tool_name: "Edit".to_string(),
        status: ToolResultStatus::Success,
        output,
        error: None,
        summary: Some(format!("Edited {}", resolved.logical_path)),
        effects: effects.into_effects(),
        duration_ms: elapsed_ms(started),
        metadata: metadata_json,
    })
}

pub fn bash(session: &Session, request: ToolInvocationRequest) -> Result<ToolInvocationResult> {
    let started = Instant::now();
    let invocation_id = invocation_id(&request);
    if let Err(err) = reject_unexpected_args(&request.arguments, &["command", "timeout"]) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let mut effects = EffectRecorder::default();
    let command = match string_arg(&request.arguments, "command") {
        Ok(command) => command,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    if !session.policy.process.allow_exec {
        return Ok(policy_denied_result(
            &request,
            &invocation_id,
            "process execution is disabled by session policy".to_string(),
            elapsed_ms(started),
        ));
    }
    if session.policy.process.allowed_commands.is_empty() {
        return Ok(policy_denied_result(
            &request,
            &invocation_id,
            "process execution requires a non-empty allowedCommands policy".to_string(),
            elapsed_ms(started),
        ));
    }
    if session
        .policy
        .process
        .denied_commands
        .iter()
        .map(|denied| denied.trim())
        .any(|denied| !denied.is_empty() && command.contains(denied))
    {
        return Ok(policy_denied_result(
            &request,
            &invocation_id,
            "command denied by session policy".to_string(),
            elapsed_ms(started),
        ));
    }
    let resolver = WorkspaceResolver::for_session(session)?;
    let cwd = match resolver.resolve_readable_cwd(request.cwd.as_deref()) {
        Ok(cwd) => cwd,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    if let Err(err) = resolver.resolve_writable_cwd(request.cwd.as_deref()) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    if !session
        .policy
        .process
        .allowed_commands
        .iter()
        .any(|allowed| command_matches_policy_entry(&command, allowed, &resolver, &cwd))
    {
        return Ok(policy_denied_result(
            &request,
            &invocation_id,
            "command is not allowed by session policy".to_string(),
            elapsed_ms(started),
        ));
    }
    if let Some(max_processes) = session.policy.process.max_processes {
        return Ok(policy_denied_result(
            &request,
            &invocation_id,
            if max_processes == 0 {
                "process limit exceeded by session policy".to_string()
            } else {
                "positive process limits are not enforceable yet".to_string()
            },
            elapsed_ms(started),
        ));
    }

    let tool_timeout = match optional_u64_arg(&request.arguments, "timeout") {
        Ok(value) => match value {
            Some(value) => match value.checked_mul(1000) {
                Some(timeout_ms) => {
                    if let Err(err) = validate_duration_limit("timeout", timeout_ms) {
                        return Ok(error_result(
                            &request,
                            &invocation_id,
                            err,
                            elapsed_ms(started),
                        ));
                    }
                    Some(Duration::from_millis(timeout_ms))
                }
                None => {
                    return Ok(error_result(
                        &request,
                        &invocation_id,
                        ExecutionerError::InvalidRequest(
                            "timeout exceeds maximum supported tool timeout".to_string(),
                        ),
                        elapsed_ms(started),
                    ))
                }
            },
            None => None,
        },
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let timeout = effective_timeout(tool_timeout, &request, session);
    let max_output_bytes = effective_max_output_bytes(session, &request, None);
    let mut command_builder = Command::new("bash");
    command_builder
        .arg("-c")
        .arg(&command)
        .current_dir(&cwd.host_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_command_environment(&mut command_builder, &session.policy.env);
    configure_process_group(&mut command_builder);
    let mut child = command_builder.spawn()?;
    let child_pid = child.id();
    let stdout_reader = child
        .stdout
        .take()
        .map(|stdout| spawn_capped_reader(stdout, max_output_bytes));
    let stderr_reader = child
        .stderr
        .take()
        .map(|stderr| spawn_capped_reader(stderr, max_output_bytes));

    let status = match child.wait_timeout(timeout)? {
        Some(status) => {
            cleanup_process_group(child_pid);
            status
        }
        None => {
            terminate_process_group(&mut child);
            let _ = child.wait();
            let _ = join_capped_reader(stdout_reader);
            let _ = join_capped_reader(stderr_reader);
            effects.record_process_exec(&invocation_id, &command, None);
            return Ok(ToolInvocationResult {
                invocation_id,
                session_id: session.id.clone(),
                tool_name: "Bash".to_string(),
                status: ToolResultStatus::Timeout,
                output: String::new(),
                error: Some(format!("Bash timed out after {}ms", timeout.as_millis())),
                summary: None,
                effects: effects.into_effects(),
                duration_ms: elapsed_ms(started),
                metadata: empty_metadata(),
            });
        }
    };
    let stdout = join_capped_reader(stdout_reader)?;
    let stderr = join_capped_reader(stderr_reader)?;
    let exit_code = status.code();
    effects.record_process_exec(&invocation_id, &command, exit_code);
    let mut output = stdout.output;
    if !stderr.output.is_empty() {
        output.push_str("\n[stderr]: ");
        output.push_str(&stderr.output);
    }
    output = truncate_string(output, max_output_bytes);
    if max_output_bytes > 0
        && (stdout.truncated || stderr.truncated)
        && !output.ends_with("\n...[truncated]")
    {
        output.push_str("\n...[truncated]");
    }
    let mut metadata = empty_metadata();
    metadata.insert("returnCode".to_string(), json!(exit_code));
    metadata.insert("cwd".to_string(), json!(cwd.logical_path));

    Ok(ToolInvocationResult {
        invocation_id,
        session_id: session.id.clone(),
        tool_name: "Bash".to_string(),
        status: if status.success() {
            ToolResultStatus::Success
        } else {
            ToolResultStatus::Error
        },
        output,
        error: if status.success() {
            None
        } else {
            Some(format!("Command exited with code {:?}", exit_code))
        },
        summary: Some(format!("Executed command in {}", cwd.logical_path)),
        effects: effects.into_effects(),
        duration_ms: elapsed_ms(started),
        metadata,
    })
}

pub fn glob_files(
    session: &Session,
    request: ToolInvocationRequest,
) -> Result<ToolInvocationResult> {
    let started = Instant::now();
    let invocation_id = invocation_id(&request);
    if let Err(err) = reject_unexpected_args(
        &request.arguments,
        &["pattern", "maxResults", "maxDepth", "includeHidden"],
    ) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let resolver = WorkspaceResolver::for_session(session)?;
    let pattern = match string_arg(&request.arguments, "pattern") {
        Ok(pattern) => pattern,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    if let Err(err) = validate_search_pattern("pattern", &pattern) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let max_results = match optional_usize_arg(&request.arguments, "maxResults") {
        Ok(value) => value,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    }
    .unwrap_or(DEFAULT_GLOB_MAX_RESULTS)
    .clamp(1, MAX_GLOB_RESULTS);
    let max_depth = match optional_usize_arg(&request.arguments, "maxDepth") {
        Ok(value) => value,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    }
    .unwrap_or(DEFAULT_MAX_DEPTH)
    .min(MAX_GLOB_DEPTH);
    let include_hidden = match optional_bool_arg(&request.arguments, "includeHidden") {
        Ok(value) => value.unwrap_or(false),
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let cwd = match resolver.resolve_readable_cwd(request.cwd.as_deref()) {
        Ok(cwd) => cwd,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let mut matches = Vec::<String>::new();
    let mut total_matches = 0_usize;
    let pattern_regex = glob_to_regex(&pattern);
    let traversal_complete = collect_paths(
        &cwd.host_path,
        &cwd.host_path,
        max_depth,
        include_hidden,
        MAX_SEARCH_VISITED_ENTRIES,
        &mut |relative, _path, is_dir| {
            if pattern_regex.is_match(relative) {
                total_matches += 1;
                if matches.len() < max_results {
                    matches.push(if is_dir {
                        format!("{relative}/")
                    } else {
                        relative.to_string()
                    });
                }
            }
            true
        },
    );
    matches.sort();
    matches.dedup();
    let truncated = total_matches > max_results || !traversal_complete;
    let mut output = if matches.is_empty() {
        format!("No files found matching pattern: {pattern} (try ../pattern or ../../pattern for sibling directories)")
    } else {
        let mut output = matches.join("\n");
        if truncated {
            output.push_str(&format!(
                "\n...[truncated at {max_results} results, {total_matches} total]"
            ));
        }
        output
    };
    output = truncate_string(output, effective_max_output_bytes(session, &request, None));
    let mut metadata = empty_metadata();
    metadata.insert("pattern".to_string(), json!(pattern));
    metadata.insert("matchCount".to_string(), json!(matches.len()));
    metadata.insert("totalMatches".to_string(), json!(total_matches));
    metadata.insert("truncated".to_string(), json!(truncated));
    metadata.insert(
        "traversalLimitExceeded".to_string(),
        json!(!traversal_complete),
    );
    Ok(success_tool_result(ToolSuccess {
        session,
        request: &request,
        invocation_id,
        tool_name: "Glob",
        output,
        duration_ms: elapsed_ms(started),
        metadata,
        effects: vec![],
    }))
}

pub fn list_files(
    session: &Session,
    request: ToolInvocationRequest,
) -> Result<ToolInvocationResult> {
    let started = Instant::now();
    let invocation_id = invocation_id(&request);
    if let Err(err) = reject_unexpected_args(&request.arguments, &[]) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let resolver = WorkspaceResolver::for_session(session)?;
    let cwd = match resolver.resolve_readable_cwd(request.cwd.as_deref()) {
        Ok(cwd) => cwd,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };

    let mut entries = Vec::<String>::new();
    let mut total_entries = 0_usize;
    let traversal_complete = collect_paths(
        &cwd.host_path,
        &cwd.host_path,
        1,
        false,
        MAX_SEARCH_VISITED_ENTRIES,
        &mut |relative, _path, is_dir| {
            total_entries += 1;
            if entries.len() < MAX_LIST_ENTRIES {
                entries.push(if is_dir {
                    format!("{relative}/")
                } else {
                    relative.to_string()
                });
            }
            true
        },
    );
    entries.sort();
    entries.dedup();
    let truncated = total_entries > MAX_LIST_ENTRIES || !traversal_complete;

    let mut output = entries.join("\n");
    if truncated {
        output.push_str(&format!(
            "\n...[truncated at {MAX_LIST_ENTRIES} entries, {total_entries} total]"
        ));
    }
    output = truncate_string(output, effective_max_output_bytes(session, &request, None));
    let mut metadata = empty_metadata();
    metadata.insert("entryCount".to_string(), json!(entries.len()));
    metadata.insert("totalEntries".to_string(), json!(total_entries));
    metadata.insert("truncated".to_string(), json!(truncated));
    metadata.insert(
        "traversalLimitExceeded".to_string(),
        json!(!traversal_complete),
    );
    metadata.insert("entries".to_string(), json!(entries));
    Ok(success_tool_result(ToolSuccess {
        session,
        request: &request,
        invocation_id,
        tool_name: "List",
        output,
        duration_ms: elapsed_ms(started),
        metadata,
        effects: vec![],
    }))
}

pub fn grep_files(
    session: &Session,
    request: ToolInvocationRequest,
) -> Result<ToolInvocationResult> {
    let started = Instant::now();
    let invocation_id = invocation_id(&request);
    if let Err(err) = reject_unexpected_args(
        &request.arguments,
        &[
            "pattern",
            "caseSensitive",
            "maxResults",
            "path",
            "glob",
            "type",
        ],
    ) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let resolver = WorkspaceResolver::for_session(session)?;
    let pattern = match string_arg(&request.arguments, "pattern") {
        Ok(pattern) => pattern,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    if let Err(err) = validate_search_pattern("pattern", &pattern) {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let case_sensitive = match optional_bool_arg(&request.arguments, "caseSensitive") {
        Ok(value) => value.unwrap_or(false),
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let regex_pattern = if case_sensitive {
        pattern.clone()
    } else {
        format!("(?i){pattern}")
    };
    let regex = match Regex::new(&regex_pattern) {
        Ok(regex) => regex,
        Err(_) => {
            return Ok(tool_error(
                &request,
                &invocation_id,
                format!("Invalid regex pattern: {pattern}"),
                elapsed_ms(started),
                empty_metadata(),
            ))
        }
    };
    let max_results = match optional_usize_arg(&request.arguments, "maxResults") {
        Ok(value) => value,
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    }
    .unwrap_or(DEFAULT_GREP_MAX_RESULTS)
    .clamp(1, MAX_GREP_RESULTS);
    let search_path = match optional_string_arg_allow_empty(&request.arguments, "path") {
        Ok(value) => value.unwrap_or_else(|| ".".to_string()),
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let glob_filter = match optional_string_arg(&request.arguments, "glob") {
        Ok(value) => match value {
            Some(glob) => {
                if let Err(err) = validate_search_pattern("glob", &glob) {
                    return Ok(error_result(
                        &request,
                        &invocation_id,
                        err,
                        elapsed_ms(started),
                    ));
                }
                Some(glob_to_regex(&glob))
            }
            None => None,
        },
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let type_filter = match optional_string_arg(&request.arguments, "type") {
        Ok(value) => value.map(|file_type| type_extensions(&file_type)),
        Err(err) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
    };
    let resolved = if search_path == "." {
        resolver.resolve_readable_cwd(request.cwd.as_deref())
    } else {
        resolver.resolve_existing(request.cwd.as_deref(), &search_path, AccessKind::Read)
    };
    let resolved = match resolved {
        Ok(resolved) => resolved,
        Err(err @ ExecutionerError::PolicyDenied(_)) => {
            return Ok(error_result(
                &request,
                &invocation_id,
                err,
                elapsed_ms(started),
            ))
        }
        Err(_) => {
            return Ok(success_tool_result(ToolSuccess {
                session,
                request: &request,
                invocation_id,
                tool_name: "Grep",
                output: format!(
                "Path not found: {search_path} (try ../path or ../../path for sibling directories)"
            ),
                duration_ms: elapsed_ms(started),
                metadata: empty_metadata(),
                effects: vec![],
            }))
        }
    };
    let mut matches = Vec::<String>::new();
    let root = if resolved.host_path.is_file() {
        resolved
            .host_path
            .parent()
            .unwrap_or(&resolved.host_path)
            .to_path_buf()
    } else {
        resolved.host_path.clone()
    };
    let mut search_error: Option<ExecutionerError> = None;
    let mut search_file = |relative: &str, path: &Path| {
        if search_error.is_some() || matches.len() >= max_results {
            return;
        }
        let Ok(metadata) = fs::symlink_metadata(path) else {
            return;
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return;
        }
        if let Some(glob) = &glob_filter {
            if !glob.is_match(relative) {
                return;
            }
        }
        if let Some(extensions) = &type_filter {
            let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
            if !extensions
                .iter()
                .any(|candidate| candidate.trim_start_matches('.') == ext)
            {
                return;
            }
        }
        if metadata.len() > MAX_GREP_FILE_BYTES {
            search_error = Some(ExecutionerError::InvalidRequest(format!(
                "grep candidate exceeds maximum searchable size of {MAX_GREP_FILE_BYTES} bytes: {relative}"
            )));
            return;
        }
        let file = match open_regular_file_no_follow(path) {
            Ok((file, file_size)) if file_size <= MAX_GREP_FILE_BYTES => file,
            Ok((_file, _file_size)) => {
                search_error = Some(ExecutionerError::InvalidRequest(format!(
                    "grep candidate exceeds maximum searchable size of {MAX_GREP_FILE_BYTES} bytes: {relative}"
                )));
                return;
            }
            Err(_) => return,
        };
        let reader = BufReader::new(file);
        for (line_idx, line) in reader.lines().enumerate() {
            if matches.len() >= max_results {
                break;
            }
            let Ok(line) = line else {
                break;
            };
            if regex.is_match(&line) {
                matches.push(format!(
                    "{relative}:{}: {}",
                    line_idx + 1,
                    truncate_string(line, 200)
                ));
            }
        }
    };
    if resolved.host_path.is_file() {
        let relative = resolved
            .host_path
            .strip_prefix(&root)
            .unwrap_or(&resolved.host_path)
            .to_string_lossy()
            .replace('\\', "/");
        search_file(&relative, &resolved.host_path);
    } else {
        let traversal_complete = collect_paths(
            &resolved.host_path,
            &resolved.host_path,
            DEFAULT_MAX_DEPTH,
            false,
            MAX_SEARCH_VISITED_ENTRIES,
            &mut |relative, path, is_dir| {
                if !is_dir {
                    search_file(relative, path);
                }
                true
            },
        );
        if !traversal_complete && search_error.is_none() && matches.len() < max_results {
            search_error = Some(ExecutionerError::InvalidRequest(format!(
                "search exceeded maximum traversal count of {MAX_SEARCH_VISITED_ENTRIES} entries"
            )));
        }
    }
    if let Some(err) = search_error {
        return Ok(error_result(
            &request,
            &invocation_id,
            err,
            elapsed_ms(started),
        ));
    }
    let mut output = if matches.is_empty() {
        format!("No matches found for pattern: {pattern}")
    } else {
        let mut output = matches.join("\n");
        if matches.len() >= max_results {
            output.push_str(&format!("\n...[truncated at {max_results} results]"));
        }
        output
    };
    output = truncate_string(output, effective_max_output_bytes(session, &request, None));
    let mut metadata = empty_metadata();
    metadata.insert("pattern".to_string(), json!(pattern));
    metadata.insert("matchCount".to_string(), json!(matches.len()));
    metadata.insert("truncated".to_string(), json!(matches.len() >= max_results));
    Ok(success_tool_result(ToolSuccess {
        session,
        request: &request,
        invocation_id,
        tool_name: "Grep",
        output,
        duration_ms: elapsed_ms(started),
        metadata,
        effects: vec![],
    }))
}

fn atomic_write(target: &std::path::Path, bytes: &[u8]) -> Result<()> {
    let tmp = temp_file_path(target);
    let result = (|| -> std::io::Result<()> {
        let mut file = fs::File::create_new(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, target)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }

    result.map_err(ExecutionerError::Io)
}

fn atomic_create_new(target: &std::path::Path, bytes: &[u8]) -> Result<()> {
    let tmp = temp_file_path(target);
    let result = (|| -> std::io::Result<()> {
        let mut file = fs::File::create_new(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::hard_link(&tmp, target)?;
        let _ = fs::remove_file(&tmp);
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }

    result.map_err(ExecutionerError::Io)
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

fn configure_command_environment(command: &mut Command, policy: &EnvPolicy) {
    command.env_clear();
    for name in &policy.allowlist {
        if !valid_env_name(name) {
            continue;
        }
        if dangerous_process_env_name(name) {
            continue;
        }
        if policy.denylist.iter().any(|denied| denied == name) {
            continue;
        }
        if let Ok(value) = std::env::var(name) {
            if !valid_env_value(&value) {
                continue;
            }
            command.env(name, value);
        }
    }
    for (name, value) in &policy.injected {
        if !valid_env_name(name) || !valid_env_value(value) {
            continue;
        }
        if dangerous_process_env_name(name) {
            continue;
        }
        if policy.denylist.iter().any(|denied| denied == name) {
            continue;
        }
        command.env(name, value);
    }
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if first != '_' && !first.is_ascii_alphabetic() {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn valid_env_value(value: &str) -> bool {
    !value.contains('\0')
}

fn dangerous_process_env_name(name: &str) -> bool {
    matches!(
        name,
        "BASH_ENV"
            | "ENV"
            | "SHELLOPTS"
            | "BASHOPTS"
            | "CDPATH"
            | "GLOBIGNORE"
            | "PATH"
            | "LD_PRELOAD"
            | "LD_LIBRARY_PATH"
            | "LD_AUDIT"
            | "LD_INSERT_LIBRARIES"
    ) || name.starts_with("DYLD_")
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_process_group(child: &mut std::process::Child) {
    let pgid = child.id() as libc::pid_t;
    unsafe {
        libc::kill(-pgid, libc::SIGKILL);
    }
}

#[cfg(not(unix))]
fn terminate_process_group(child: &mut std::process::Child) {
    let _ = child.kill();
}

#[cfg(unix)]
fn cleanup_process_group(pid: u32) {
    unsafe {
        libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
    }
}

#[cfg(not(unix))]
fn cleanup_process_group(_pid: u32) {}

struct CappedOutput {
    output: String,
    truncated: bool,
}

fn spawn_capped_reader<R>(
    mut reader: R,
    max_bytes: usize,
) -> std::thread::JoinHandle<std::io::Result<CappedOutput>>
where
    R: Read + Send + 'static,
{
    std::thread::spawn(move || {
        let mut retained = Vec::<u8>::new();
        let mut truncated = false;
        let mut buffer = [0_u8; 8192];
        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            let remaining = max_bytes.saturating_sub(retained.len());
            if remaining > 0 {
                retained.extend_from_slice(&buffer[..bytes_read.min(remaining)]);
            }
            if bytes_read > remaining {
                truncated = true;
            }
        }
        Ok(CappedOutput {
            output: String::from_utf8_lossy(&retained).into_owned(),
            truncated,
        })
    })
}

fn join_capped_reader(
    reader: Option<std::thread::JoinHandle<std::io::Result<CappedOutput>>>,
) -> Result<CappedOutput> {
    let Some(reader) = reader else {
        return Ok(CappedOutput {
            output: String::new(),
            truncated: false,
        });
    };
    reader
        .join()
        .map_err(|_| ExecutionerError::InvalidRequest("output reader panicked".to_string()))?
        .map_err(ExecutionerError::Io)
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

fn count_occurrences(content: &str, search: &str) -> usize {
    if search.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut start = 0;
    while let Some(offset) = content[start..].find(search) {
        count += 1;
        start += offset + search.len();
    }
    count
}

fn metadata_with_path(path: &str, action: &str) -> Map<String, Value> {
    let mut metadata = empty_metadata();
    metadata.insert("path".to_string(), json!(path));
    metadata.insert("action".to_string(), json!(action));
    metadata
}

struct ToolSuccess<'a> {
    session: &'a Session,
    request: &'a ToolInvocationRequest,
    invocation_id: String,
    tool_name: &'a str,
    output: String,
    duration_ms: u64,
    metadata: Map<String, Value>,
    effects: Vec<crate::protocol::Effect>,
}

fn success_tool_result(success: ToolSuccess<'_>) -> ToolInvocationResult {
    ToolInvocationResult {
        invocation_id: success.invocation_id,
        session_id: success.session.id.clone(),
        tool_name: success.tool_name.to_string(),
        status: ToolResultStatus::Success,
        output: success.output,
        error: None,
        summary: Some(format!("Executed {}", success.request.tool_name)),
        effects: success.effects,
        duration_ms: success.duration_ms,
        metadata: success.metadata,
    }
}

fn policy_denied_result(
    request: &ToolInvocationRequest,
    invocation_id: &str,
    message: String,
    duration_ms: u64,
) -> ToolInvocationResult {
    ToolInvocationResult {
        invocation_id: invocation_id.to_string(),
        session_id: request.session_id.clone(),
        tool_name: request.tool_name.clone(),
        status: ToolResultStatus::PolicyDenied,
        output: String::new(),
        error: Some(message),
        summary: None,
        effects: vec![],
        duration_ms,
        metadata: empty_metadata(),
    }
}

fn truncate_string(value: String, max_len: usize) -> String {
    if max_len == 0 {
        return String::new();
    }
    if value.len() <= max_len {
        value
    } else {
        let boundary = value
            .char_indices()
            .map(|(idx, _)| idx)
            .take_while(|idx| *idx <= max_len)
            .last()
            .unwrap_or(0);
        format!("{}\n...[truncated]", &value[..boundary])
    }
}

fn effective_max_output_bytes(
    session: &Session,
    request: &ToolInvocationRequest,
    tool_override: Option<usize>,
) -> usize {
    [
        tool_override,
        request.max_output_bytes,
        session.policy.max_output_bytes,
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(DEFAULT_MAX_BYTES)
}

fn effective_timeout(
    tool_timeout: Option<Duration>,
    request: &ToolInvocationRequest,
    session: &Session,
) -> Duration {
    let request_timeout = request.timeout_ms.map(Duration::from_millis);
    let session_timeout = session.policy.max_duration_ms.map(Duration::from_millis);
    [tool_timeout, request_timeout, session_timeout]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_BASH_TIMEOUT_SECS))
}

fn validate_line_range(start_line: Option<usize>, end_line: Option<usize>) -> Result<()> {
    if start_line == Some(0) {
        return Err(ExecutionerError::InvalidRequest(
            "startLine must be a positive integer".to_string(),
        ));
    }
    if end_line == Some(0) {
        return Err(ExecutionerError::InvalidRequest(
            "endLine must be a positive integer".to_string(),
        ));
    }
    if let (Some(start_line), Some(end_line)) = (start_line, end_line) {
        if end_line < start_line {
            return Err(ExecutionerError::InvalidRequest(
                "endLine must be greater than or equal to startLine".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_search_pattern(label: &str, pattern: &str) -> Result<()> {
    if pattern.len() > MAX_SEARCH_PATTERN_BYTES {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} exceeds maximum size of {MAX_SEARCH_PATTERN_BYTES} bytes"
        )));
    }
    Ok(())
}

fn context_snippet(content: &str, start: usize, len: usize, context_chars: usize) -> String {
    let prefix_start = content[..start]
        .char_indices()
        .rev()
        .nth(context_chars.saturating_sub(1))
        .map(|(idx, _)| idx)
        .unwrap_or(0);
    let target_end = start + len;
    let suffix_end = content[target_end..]
        .char_indices()
        .nth(context_chars)
        .map(|(idx, _)| target_end + idx)
        .unwrap_or(content.len());
    content[prefix_start..suffix_end].to_string()
}

fn command_matches_policy_entry(
    command: &str,
    entry: &str,
    resolver: &WorkspaceResolver,
    cwd: &ResolvedPath,
) -> bool {
    let entry = entry.trim();
    if entry.is_empty() {
        return false;
    }
    if command == entry {
        return command_path_arguments_do_not_escape_workspace(command, resolver, cwd);
    }
    if entry.contains(char::is_whitespace) {
        return false;
    }
    command
        .strip_prefix(entry)
        .is_some_and(|remaining| remaining.starts_with(char::is_whitespace))
        && !contains_shell_control_syntax(command)
        && command_executable_stays_in_workspace(command, resolver, cwd)
        && command_path_arguments_stay_in_workspace(command, resolver, cwd)
}

fn contains_shell_control_syntax(command: &str) -> bool {
    command.chars().any(|ch| {
        matches!(
            ch,
            ';' | '&'
                | '|'
                | '<'
                | '>'
                | '$'
                | '`'
                | '\n'
                | '\r'
                | '('
                | ')'
                | '{'
                | '}'
                | '\''
                | '"'
                | '\\'
        )
    })
}

fn command_path_arguments_stay_in_workspace(
    command: &str,
    resolver: &WorkspaceResolver,
    cwd: &ResolvedPath,
) -> bool {
    command.split_whitespace().skip(1).all(|token| {
        let token = clean_command_token(token);
        let path_fragment = shell_path_fragment(&token);
        if token_references_host_path_escape(path_fragment) {
            return false;
        }
        if token_contains_shell_glob(&token) {
            return false;
        }
        if token.starts_with('-') && token.contains('/') {
            return false;
        }
        if token.is_empty() || token.starts_with('-') {
            return true;
        }
        if token_has_shell_path_prefix(&token) && path_fragment.is_empty() {
            return true;
        }
        if token_has_shell_path_prefix(&token) && !path_fragment.is_empty() {
            return redirection_target_stays_in_workspace(path_fragment, resolver, cwd);
        }
        if !token_looks_path_like(path_fragment, cwd) {
            return true;
        }
        resolver
            .resolve_existing(Some(&cwd.logical_path), path_fragment, AccessKind::Read)
            .is_ok()
    })
}

fn command_executable_stays_in_workspace(
    command: &str,
    resolver: &WorkspaceResolver,
    cwd: &ResolvedPath,
) -> bool {
    let Some(token) = command.split_whitespace().next() else {
        return false;
    };
    let token = clean_command_token(token);
    if token.is_empty()
        || token_references_host_path_escape(&token)
        || token_contains_shell_glob(&token)
    {
        return false;
    }
    if token.contains('/') || token == "." {
        return resolver
            .resolve_existing(Some(&cwd.logical_path), &token, AccessKind::Read)
            .is_ok();
    }
    true
}

fn command_path_arguments_do_not_escape_workspace(
    command: &str,
    resolver: &WorkspaceResolver,
    cwd: &ResolvedPath,
) -> bool {
    command.split_whitespace().all(|token| {
        let token = clean_command_token(token);
        let path_fragment = shell_path_fragment(&token);
        if token_references_host_path_escape(path_fragment) {
            return false;
        }
        if token_contains_shell_glob(&token) {
            return false;
        }
        if token.starts_with('-') && token.contains('/') {
            return false;
        }
        if token.is_empty() || token.starts_with('-') {
            return true;
        }
        if token_has_shell_path_prefix(&token) && path_fragment.is_empty() {
            return true;
        }
        if token_has_shell_path_prefix(&token) && !path_fragment.is_empty() {
            return redirection_target_stays_in_workspace(path_fragment, resolver, cwd);
        }
        if token_looks_path_like(path_fragment, cwd) && cwd.host_path.join(path_fragment).exists() {
            return resolver
                .resolve_existing(Some(&cwd.logical_path), path_fragment, AccessKind::Read)
                .is_ok();
        }
        true
    })
}

fn clean_command_token(token: &str) -> String {
    token
        .trim_matches(|ch| matches!(ch, '"' | '\''))
        .to_string()
}

fn token_looks_path_like(token: &str, cwd: &ResolvedPath) -> bool {
    token == "."
        || token.starts_with("./")
        || token.contains('/')
        || std::path::Path::new(token).extension().is_some()
        || cwd.host_path.join(token).exists()
}

fn token_references_host_path_escape(token: &str) -> bool {
    token.starts_with('/')
        || token.starts_with('~')
        || token == ".."
        || token.contains("../")
        || token.contains("/../")
        || token.ends_with("/..")
}

fn shell_path_fragment(token: &str) -> &str {
    let token = token.trim_start_matches(|ch: char| ch.is_ascii_digit());
    let Some(operator_start) = token.find(['<', '>']) else {
        return token;
    };
    token[operator_start..].trim_start_matches(['<', '>', '&'])
}

fn token_has_shell_path_prefix(token: &str) -> bool {
    shell_path_fragment(token) != token
}

fn redirection_target_stays_in_workspace(
    target: &str,
    resolver: &WorkspaceResolver,
    cwd: &ResolvedPath,
) -> bool {
    let Ok(resolved) = resolver.resolve_write_target(Some(&cwd.logical_path), target) else {
        return false;
    };
    !fs::symlink_metadata(&resolved.host_path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
}

fn token_contains_shell_glob(token: &str) -> bool {
    token.chars().any(|ch| matches!(ch, '*' | '?' | '[' | ']'))
}

fn should_skip_name(name: &str, include_hidden: bool) -> bool {
    const SKIP_DIRS: &[&str] = &[
        "node_modules",
        ".git",
        "dist",
        "build",
        ".next",
        ".turbo",
        ".cache",
        "coverage",
        ".venv",
        "venv",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "site-packages",
        "htmlcov",
        ".tox",
        ".eggs",
        "logs",
        "log",
    ];
    if !include_hidden && name.starts_with('.') {
        return true;
    }
    SKIP_DIRS.contains(&name) || name.ends_with(".log") || name.ends_with(".pyc")
}

fn collect_paths<F>(
    root: &Path,
    current: &Path,
    depth: usize,
    include_hidden: bool,
    max_entries: usize,
    visitor: &mut F,
) -> bool
where
    F: FnMut(&str, &Path, bool) -> bool,
{
    let mut visited = 0_usize;
    collect_paths_inner(
        root,
        current,
        depth,
        include_hidden,
        max_entries,
        &mut visited,
        visitor,
    )
}

fn collect_paths_inner<F>(
    root: &Path,
    current: &Path,
    depth: usize,
    include_hidden: bool,
    max_entries: usize,
    visited: &mut usize,
    visitor: &mut F,
) -> bool
where
    F: FnMut(&str, &Path, bool) -> bool,
{
    if depth == 0 {
        return true;
    }
    let Ok(entries) = fs::read_dir(current) else {
        return true;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if should_skip_name(&name, include_hidden) {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if *visited >= max_entries {
            return false;
        }
        *visited += 1;
        let relative = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        if !visitor(&relative, &path, file_type.is_dir()) {
            return false;
        }
        if file_type.is_dir()
            && !collect_paths_inner(
                root,
                &path,
                depth - 1,
                include_hidden,
                max_entries,
                visited,
                visitor,
            )
        {
            return false;
        }
    }
    true
}

fn glob_to_regex(pattern: &str) -> Regex {
    let mut regex = String::from("^");
    let chars = pattern.chars().collect::<Vec<_>>();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' if i + 2 < chars.len() && chars[i + 1] == '*' && chars[i + 2] == '/' => {
                regex.push_str("(?:.*/)?");
                i += 3;
            }
            '*' if i + 1 < chars.len() && chars[i + 1] == '*' => {
                regex.push_str(".*");
                i += 2;
            }
            '*' => {
                regex.push_str("[^/]*");
                i += 1;
            }
            '?' => {
                regex.push_str("[^/]");
                i += 1;
            }
            '.' | '+' | '(' | ')' | '|' | '^' | '$' | '[' | ']' | '{' | '}' | '\\' => {
                regex.push('\\');
                regex.push(chars[i]);
                i += 1;
            }
            ch => {
                regex.push(ch);
                i += 1;
            }
        }
    }
    regex.push('$');
    Regex::new(&regex).unwrap_or_else(|_| Regex::new("$^").expect("valid fallback regex"))
}

fn type_extensions(file_type: &str) -> Vec<String> {
    match file_type.to_lowercase().as_str() {
        "ts" => vec!["ts", "tsx"],
        "js" => vec!["js", "jsx", "mjs", "cjs"],
        "py" => vec!["py", "pyi"],
        "rust" | "rs" => vec!["rs"],
        "go" => vec!["go"],
        "java" => vec!["java"],
        "json" => vec!["json"],
        "yaml" => vec!["yaml", "yml"],
        "md" => vec!["md", "markdown"],
        "sh" => vec!["sh", "bash", "zsh"],
        other => vec![other],
    }
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn invocation_id(request: &ToolInvocationRequest) -> String {
    request
        .invocation_id
        .clone()
        .unwrap_or_else(|| format!("inv_{}", Uuid::new_v4().simple()))
}

fn string_arg(args: &Map<String, Value>, name: &str) -> Result<String> {
    match args.get(name) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        _ => Err(ExecutionerError::InvalidRequest(format!(
            "{name} is required"
        ))),
    }
}

fn string_arg_allow_empty(args: &Map<String, Value>, name: &str) -> Result<String> {
    match args.get(name) {
        Some(Value::String(value)) => Ok(value.clone()),
        _ => Err(ExecutionerError::InvalidRequest(format!(
            "{name} is required"
        ))),
    }
}

fn reject_unexpected_args(args: &Map<String, Value>, allowed: &[&str]) -> Result<()> {
    if let Some(name) = args.keys().find(|name| !allowed.contains(&name.as_str())) {
        return Err(ExecutionerError::InvalidRequest(format!(
            "unexpected argument: {name}"
        )));
    }
    Ok(())
}

fn optional_string_arg(args: &Map<String, Value>, name: &str) -> Result<Option<String>> {
    match args.get(name) {
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        Some(Value::String(_)) => Err(ExecutionerError::InvalidRequest(format!(
            "{name} must be a non-empty string"
        ))),
        Some(_) => Err(ExecutionerError::InvalidRequest(format!(
            "{name} must be a string"
        ))),
        None => Ok(None),
    }
}

fn optional_string_arg_allow_empty(
    args: &Map<String, Value>,
    name: &str,
) -> Result<Option<String>> {
    match args.get(name) {
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ExecutionerError::InvalidRequest(format!(
            "{name} must be a string"
        ))),
        None => Ok(None),
    }
}

fn optional_bool_arg(args: &Map<String, Value>, name: &str) -> Result<Option<bool>> {
    match args.get(name) {
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(ExecutionerError::InvalidRequest(format!(
            "{name} must be a boolean"
        ))),
        None => Ok(None),
    }
}

fn optional_usize_arg(args: &Map<String, Value>, name: &str) -> Result<Option<usize>> {
    optional_u64_arg(args, name).and_then(|value| {
        value
            .map(|value| {
                usize::try_from(value).map_err(|_| {
                    ExecutionerError::InvalidRequest(format!(
                        "{name} must be a non-negative integer"
                    ))
                })
            })
            .transpose()
    })
}

fn optional_u64_arg(args: &Map<String, Value>, name: &str) -> Result<Option<u64>> {
    match args.get(name) {
        Some(Value::Number(value)) => value.as_u64().map(Some).ok_or_else(|| {
            ExecutionerError::InvalidRequest(format!("{name} must be a non-negative integer"))
        }),
        Some(_) => Err(ExecutionerError::InvalidRequest(format!(
            "{name} must be a non-negative integer"
        ))),
        None => Ok(None),
    }
}

fn error_result(
    request: &ToolInvocationRequest,
    invocation_id: &str,
    err: ExecutionerError,
    duration_ms: u64,
) -> ToolInvocationResult {
    let status = if matches!(err, ExecutionerError::PolicyDenied(_)) {
        ToolResultStatus::PolicyDenied
    } else {
        ToolResultStatus::Error
    };
    ToolInvocationResult {
        invocation_id: invocation_id.to_string(),
        session_id: request.session_id.clone(),
        tool_name: request.tool_name.clone(),
        status,
        output: String::new(),
        error: Some(err.to_string()),
        summary: None,
        effects: vec![],
        duration_ms,
        metadata: empty_metadata(),
    }
}

fn io_error_result(
    request: &ToolInvocationRequest,
    invocation_id: &str,
    err: std::io::Error,
    duration_ms: u64,
) -> ToolInvocationResult {
    tool_error(
        request,
        invocation_id,
        format!("File read failed: {err}"),
        duration_ms,
        empty_metadata(),
    )
}

fn tool_error(
    request: &ToolInvocationRequest,
    invocation_id: &str,
    message: String,
    duration_ms: u64,
    metadata: Map<String, Value>,
) -> ToolInvocationResult {
    ToolInvocationResult {
        invocation_id: invocation_id.to_string(),
        session_id: request.session_id.clone(),
        tool_name: request.tool_name.clone(),
        status: ToolResultStatus::Error,
        output: String::new(),
        error: Some(message),
        summary: None,
        effects: vec![],
        duration_ms,
        metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_create_new_does_not_overwrite_existing_target() {
        let temp = tempfile::TempDir::new().unwrap();
        let target = temp.path().join("target.txt");
        fs::write(&target, "original").unwrap();

        let err = atomic_create_new(&target, b"replacement").unwrap_err();

        assert!(err.to_string().contains("File exists") || err.to_string().contains("exists"));
        assert_eq!(fs::read_to_string(&target).unwrap(), "original");
        assert_eq!(
            fs::read_dir(temp.path()).unwrap().count(),
            1,
            "failed atomic writes should clean up temporary files"
        );
    }

    #[cfg(unix)]
    #[test]
    fn regular_file_opener_rejects_symlink_without_following_it() {
        let temp = tempfile::TempDir::new().unwrap();
        let outside = temp.path().join("outside.txt");
        let link = temp.path().join("link.txt");
        fs::write(&outside, "outside secret").unwrap();
        std::os::unix::fs::symlink(&outside, &link).unwrap();

        let err = open_regular_file_no_follow(&link).unwrap_err();

        assert!(
            err.to_string()
                .contains("Too many levels of symbolic links")
                || err.to_string().contains("path is not a regular file")
        );
    }
}
