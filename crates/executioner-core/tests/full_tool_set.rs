use executioner_core::{
    CreateSessionRequest, ExecutionPolicy, HostState, NetworkPolicy, ProcessPolicy, ToolCapability,
    ToolInvocationRequest, ToolResultStatus, WorkspaceMode, WorkspaceSpec,
};
use serde_json::{json, Map, Value};
use std::fs;
use tempfile::TempDir;

fn policy(allow_exec: bool, allow_network: bool) -> ExecutionPolicy {
    ExecutionPolicy {
        read_roots: vec!["/workspace".to_string()],
        write_roots: vec!["/workspace".to_string()],
        process: ProcessPolicy {
            allow_exec,
            allowed_commands: vec![],
            denied_commands: vec!["rm -rf /".to_string()],
            max_processes: None,
        },
        network: NetworkPolicy {
            enabled: allow_network,
            allow_hosts: vec![],
            deny_hosts: vec![],
        },
        ..ExecutionPolicy::default()
    }
}

fn session(host: &HostState, allow_exec: bool, allow_network: bool) -> executioner_core::Session {
    session_with_policy(host, policy(allow_exec, allow_network))
}

fn session_with_policy(host: &HostState, policy: ExecutionPolicy) -> executioner_core::Session {
    host.create_session(CreateSessionRequest {
        session_id: Some("sess".to_string()),
        workspace: WorkspaceSpec {
            mode: WorkspaceMode::New,
            root: None,
            snapshot_ref: None,
            template_ref: None,
            mount_as_workspace: true,
        },
        policy,
        ttl_ms: None,
        metadata: Map::new(),
    })
    .unwrap()
    .session
}

fn session_with_allowed_command(host: &HostState, command: &str) -> executioner_core::Session {
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec![command.to_string()];
    session_with_policy(host, policy)
}

fn invoke(session_id: &str, tool_name: &str, args: Value) -> ToolInvocationRequest {
    ToolInvocationRequest {
        invocation_id: Some(format!("inv_{tool_name}")),
        session_id: session_id.to_string(),
        tool_name: tool_name.to_string(),
        arguments: args.as_object().cloned().unwrap(),
        cwd: Some("/workspace".to_string()),
        timeout_ms: None,
        max_output_bytes: None,
        idempotency_key: None,
        required_capabilities: vec![],
        metadata: Map::new(),
    }
}

#[test]
fn edit_replaces_single_occurrence_and_records_effect() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::write(
        format!("{}/file.txt", session.workspace.root),
        "Hello, World!",
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Edit",
            json!({ "path": "file.txt", "oldString": "World", "newString": "Universe" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(
        fs::read_to_string(format!("{}/file.txt", session.workspace.root)).unwrap(),
        "Hello, Universe!"
    );
    assert_eq!(result.effects[0].kind, "file.write");
}

#[test]
fn edit_rejects_non_unique_without_replace_all() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::write(format!("{}/file.txt", session.workspace.root), "foo foo").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Edit",
            json!({ "path": "file.txt", "oldString": "foo", "newString": "bar" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("not unique"));
    assert_eq!(
        fs::read_to_string(format!("{}/file.txt", session.workspace.root)).unwrap(),
        "foo foo"
    );
}

#[test]
fn edit_checks_write_policy_on_symlink_target_not_link_path() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let mut policy = policy(false, false);
        policy.write_roots = vec!["/workspace/public".to_string()];
        let session = session_with_policy(&host, policy);
        fs::create_dir_all(format!("{}/public", session.workspace.root)).unwrap();
        fs::write(format!("{}/secret.txt", session.workspace.root), "secret").unwrap();
        std::os::unix::fs::symlink(
            format!("{}/secret.txt", session.workspace.root),
            format!("{}/public/link", session.workspace.root),
        )
        .unwrap();

        let result = host
            .execute_invocation(invoke(
                &session.id,
                "Edit",
                json!({ "path": "public/link", "oldString": "secret", "newString": "leaked" }),
            ))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::PolicyDenied);
        assert_eq!(
            fs::read_to_string(format!("{}/secret.txt", session.workspace.root)).unwrap(),
            "secret"
        );
        assert!(result.effects.is_empty());
    }
}

#[test]
fn edit_respects_zero_session_output_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy(false, false);
    limited_policy.max_output_bytes = Some(0);
    let session = session_with_policy(&host, limited_policy);
    fs::write(format!("{}/file.txt", session.workspace.root), "before").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Edit",
            json!({ "path": "file.txt", "oldString": "before", "newString": "after" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "");
    assert_eq!(
        fs::read_to_string(format!("{}/file.txt", session.workspace.root)).unwrap(),
        "after"
    );
}

#[test]
fn edit_non_unique_error_handles_unicode_context_without_panic() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::write(
        format!("{}/file.txt", session.workspace.root),
        "🙂🙂🙂🙂🙂🙂🙂🙂🙂🙂foo foo",
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Edit",
            json!({ "path": "file.txt", "oldString": "foo", "newString": "bar" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("not unique"));
}

#[test]
fn bash_obeys_process_policy_and_records_process_effect() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session_with_allowed_command(&host, "printf hello");

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf hello", "timeout": 2 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "hello");
    assert_eq!(result.effects[0].kind, "process.exec");
}

#[test]
fn bash_denied_when_exec_disabled() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf hello" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
}

#[test]
fn bash_allow_exec_requires_an_allowed_command_policy() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, true, false);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "cat /etc/passwd" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.effects.is_empty());
}

#[test]
fn bash_command_name_allowlist_rejects_absolute_host_path_arguments() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let secret = temp.path().join("secret.txt");
    fs::write(&secret, "secret").unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["cat".to_string()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": format!("cat {}", secret.display()) }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.output.is_empty());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_exact_allowlist_rejects_absolute_host_path_arguments() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let secret = temp.path().join("secret.txt");
    fs::write(&secret, "secret").unwrap();
    let command = format!("cat {}", secret.display());
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec![command.clone()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.output.is_empty());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_exact_allowlist_rejects_embedded_redirection_host_path() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let outside = temp.path().join("outside.txt");
    let command = format!("printf leaked >{}", outside.display());
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec![command.clone()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(!outside.exists());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_exact_allowlist_rejects_redirection_host_path_without_whitespace() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let outside = temp.path().join("outside.txt");
    let command = format!("printf>{}", outside.display());
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec![command.clone()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(!outside.exists());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_exact_allowlist_rejects_input_redirection_host_path_without_whitespace() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let outside = temp.path().join("outside.txt");
    fs::write(&outside, "secret").unwrap();
    let command = format!("cat<{}", outside.display());
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec![command.clone()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.output.is_empty());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_exact_allowlist_rejects_fd_redirection_host_path() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let outside = temp.path().join("stderr.txt");
    let command = format!("printf leaked 2>{}", outside.display());
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec![command.clone()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(!outside.exists());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_exact_allowlist_rejects_combined_redirection_host_path() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let outside = temp.path().join("combined.txt");
    let command = format!("printf leaked &>{}", outside.display());
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec![command.clone()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(!outside.exists());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_exact_allowlist_rejects_redirection_symlink_escape() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let outside = temp.path().join("outside.txt");
        let mut policy = policy(true, false);
        let command = "printf leaked >outside-link".to_string();
        policy.process.allowed_commands = vec![command.clone()];
        let session = session_with_policy(&host, policy);
        std::os::unix::fs::symlink(&outside, format!("{}/outside-link", session.workspace.root))
            .unwrap();

        let result = host
            .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::PolicyDenied);
        assert!(!outside.exists());
        assert!(result.effects.is_empty());
    }
}

#[test]
fn bash_command_name_allowlist_rejects_symlink_path_escape_arguments() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let outside = temp.path().join("outside.txt");
        fs::write(&outside, "secret").unwrap();
        let mut policy = policy(true, false);
        policy.process.allowed_commands = vec!["cat".to_string()];
        let session = session_with_policy(&host, policy);
        std::os::unix::fs::symlink(&outside, format!("{}/outside-link", session.workspace.root))
            .unwrap();

        let result = host
            .execute_invocation(invoke(
                &session.id,
                "Bash",
                json!({ "command": "cat outside-link" }),
            ))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::PolicyDenied);
        assert!(result.output.is_empty());
        assert!(result.effects.is_empty());
    }
}

#[test]
fn bash_command_name_allowlist_rejects_symlink_executable_escape_with_arguments() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let outside_tool = temp.path().join("outside-tool.sh");
        fs::write(&outside_tool, "#!/bin/sh\nprintf escaped\n").unwrap();
        fs::set_permissions(&outside_tool, fs::Permissions::from_mode(0o700)).unwrap();
        let mut policy = policy(true, false);
        policy.process.allowed_commands = vec!["./tool".to_string()];
        let session = session_with_policy(&host, policy);
        std::os::unix::fs::symlink(&outside_tool, format!("{}/tool", session.workspace.root))
            .unwrap();

        let result = host
            .execute_invocation(invoke(
                &session.id,
                "Bash",
                json!({ "command": "./tool ignored-arg" }),
            ))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::PolicyDenied);
        assert!(result.output.is_empty());
        assert!(result.effects.is_empty());
    }
}

#[test]
fn bash_command_name_allowlist_rejects_absolute_executable_escape_with_arguments() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let outside_tool = temp.path().join("outside-tool.sh");
        fs::write(&outside_tool, "#!/bin/sh\nprintf escaped\n").unwrap();
        fs::set_permissions(&outside_tool, fs::Permissions::from_mode(0o700)).unwrap();
        let mut policy = policy(true, false);
        policy.process.allowed_commands = vec![outside_tool.display().to_string()];
        let session = session_with_policy(&host, policy);

        let result = host
            .execute_invocation(invoke(
                &session.id,
                "Bash",
                json!({ "command": format!("{} ignored-arg", outside_tool.display()) }),
            ))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::PolicyDenied);
        assert!(result.output.is_empty());
        assert!(result.effects.is_empty());
    }
}

#[test]
fn bash_command_name_allowlist_checks_assignment_like_path_arguments() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let outside_dir = temp.path().join("outside");
        fs::create_dir_all(&outside_dir).unwrap();
        fs::write(outside_dir.join("secret.txt"), "secret").unwrap();
        let mut policy = policy(true, false);
        policy.process.allowed_commands = vec!["cat".to_string()];
        let session = session_with_policy(&host, policy);
        std::os::unix::fs::symlink(&outside_dir, format!("{}/X=", session.workspace.root)).unwrap();

        let result = host
            .execute_invocation(invoke(
                &session.id,
                "Bash",
                json!({ "command": "cat X=/secret.txt" }),
            ))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::PolicyDenied);
        assert!(result.output.is_empty());
        assert!(result.effects.is_empty());
    }
}

#[test]
fn bash_command_name_allowlist_rejects_glob_that_could_expand_to_symlink_escape() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let outside = temp.path().join("outside.txt");
        fs::write(&outside, "secret").unwrap();
        let mut policy = policy(true, false);
        policy.process.allowed_commands = vec!["cat".to_string()];
        let session = session_with_policy(&host, policy);
        std::os::unix::fs::symlink(&outside, format!("{}/outside-link", session.workspace.root))
            .unwrap();

        let result = host
            .execute_invocation(invoke(&session.id, "Bash", json!({ "command": "cat *" })))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::PolicyDenied);
        assert!(result.output.is_empty());
        assert!(result.effects.is_empty());
    }
}

#[test]
fn bash_command_name_allowlist_rejects_option_embedded_parent_path_arguments() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["awk".to_string()];
    let session = session_with_policy(&host, policy);
    let outside_program = std::path::Path::new(&session.workspace.root)
        .parent()
        .unwrap()
        .join("program.awk");
    fs::write(&outside_program, "{ print \"leaked\" }\n").unwrap();
    fs::write(format!("{}/data.txt", session.workspace.root), "input\n").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "awk -f../program.awk data.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.output.is_empty());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_command_name_allowlist_rejects_option_embedded_absolute_path_arguments() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["awk".to_string()];
    let session = session_with_policy(&host, policy);
    let outside_program = std::path::Path::new(&session.workspace.root)
        .parent()
        .unwrap()
        .join("absolute-program.awk");
    fs::write(&outside_program, "{ print \"leaked\" }\n").unwrap();
    fs::write(format!("{}/data.txt", session.workspace.root), "input\n").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": format!("awk -f{} data.txt", outside_program.display()) }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.output.is_empty());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_command_name_allowlist_rejects_shell_escaped_parent_path_arguments() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["awk".to_string()];
    let session = session_with_policy(&host, policy);
    let outside_program = std::path::Path::new(&session.workspace.root)
        .parent()
        .unwrap()
        .join("program.awk");
    fs::write(&outside_program, "{ print \"leaked\" }\n").unwrap();
    fs::write(format!("{}/data.txt", session.workspace.root), "input\n").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "awk -f..\\/program.awk data.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.output.is_empty());
    assert!(result.effects.is_empty());
}

#[test]
fn bash_command_name_allowlist_allows_workspace_file_arguments() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["cat".to_string()];
    let session = session_with_policy(&host, policy);
    fs::write(format!("{}/workspace.txt", session.workspace.root), "ok").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "cat workspace.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "ok");
}

#[test]
fn bash_does_not_inherit_host_stdin() {
    #[cfg(unix)]
    {
        struct StdinRestore {
            original_fd: libc::c_int,
        }

        impl Drop for StdinRestore {
            fn drop(&mut self) {
                unsafe {
                    libc::dup2(self.original_fd, libc::STDIN_FILENO);
                    libc::close(self.original_fd);
                }
            }
        }

        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let mut policy = policy(true, false);
        policy.process.allowed_commands = vec!["cat".to_string()];
        let session = session_with_policy(&host, policy);

        unsafe {
            let mut pipe_fds = [0; 2];
            assert_eq!(libc::pipe(pipe_fds.as_mut_ptr()), 0);
            let original_fd = libc::dup(libc::STDIN_FILENO);
            assert!(original_fd >= 0);
            let _restore = StdinRestore { original_fd };
            let secret = b"host-stdin-secret\n";
            assert_eq!(
                libc::write(pipe_fds[1], secret.as_ptr().cast(), secret.len()),
                secret.len() as isize
            );
            libc::close(pipe_fds[1]);
            assert_eq!(
                libc::dup2(pipe_fds[0], libc::STDIN_FILENO),
                libc::STDIN_FILENO
            );
            libc::close(pipe_fds[0]);

            let result = host
                .execute_invocation(invoke(&session.id, "Bash", json!({ "command": "cat" })))
                .unwrap();

            assert_eq!(result.status, ToolResultStatus::Success);
            assert_eq!(result.output, "");
            assert!(result.effects[0]
                .summary
                .as_deref()
                .unwrap_or("")
                .contains("cat"));
        }
    }
}

#[test]
fn bash_does_not_inherit_host_environment_by_default() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let env_name = format!("SUBSTRATE_SECRET_{}", std::process::id());
    std::env::set_var(&env_name, "host-secret");
    let command = format!("printf \"${env_name}\"");
    let session = session_with_allowed_command(&host, &command);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
        .unwrap();

    std::env::remove_var(&env_name);
    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "");
}

#[test]
fn bash_uses_policy_injected_environment() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["printf \"$SUBSTRATE_INJECTED\"".to_string()];
    policy
        .env
        .injected
        .insert("SUBSTRATE_INJECTED".to_string(), "injected".to_string());
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf \"$SUBSTRATE_INJECTED\"" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "injected");
}

#[test]
fn bash_env_denylist_overrides_allowlist_and_injected_values() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let env_name = format!("SUBSTRATE_DENIED_{}", std::process::id());
    std::env::set_var(&env_name, "host-secret");
    let command = format!("printf \"${env_name}\"");
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec![command.clone()];
    policy.env.allowlist = vec![env_name.clone()];
    policy
        .env
        .injected
        .insert(env_name.clone(), "injected-secret".to_string());
    policy.env.denylist = vec![env_name.clone()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
        .unwrap();

    std::env::remove_var(&env_name);
    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "");
}

#[test]
fn bash_does_not_propagate_shell_startup_environment() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["printf ok".to_string()];
    policy.env.allowlist = vec!["BASH_ENV".to_string()];
    let session = session_with_policy(&host, policy);
    let startup_script = temp.path().join("bash-env.sh");
    fs::write(
        &startup_script,
        format!(
            "printf startup > {}/startup-ran.txt\n",
            session.workspace.root
        ),
    )
    .unwrap();
    std::env::set_var("BASH_ENV", startup_script);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf ok" }),
        ))
        .unwrap();

    std::env::remove_var("BASH_ENV");
    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "ok");
    assert!(!std::path::Path::new(&format!("{}/startup-ran.txt", session.workspace.root)).exists());
}

#[test]
fn bash_does_not_use_injected_path_to_resolve_allowed_command_names() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let fake_cat = workspace.join("cat");
        fs::write(&fake_cat, "#!/bin/sh\nprintf fake\n").unwrap();
        fs::set_permissions(&fake_cat, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(workspace.join("input.txt"), "real").unwrap();
        let mut policy = policy(true, false);
        policy.process.allowed_commands = vec!["cat".to_string()];
        policy
            .env
            .injected
            .insert("PATH".to_string(), workspace.display().to_string());
        let session = host
            .create_session(CreateSessionRequest {
                session_id: Some("path_env".to_string()),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::Existing,
                    root: Some(workspace.display().to_string()),
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy,
                ttl_ms: None,
                metadata: Map::new(),
            })
            .unwrap()
            .session;

        let result = host
            .execute_invocation(invoke(
                &session.id,
                "Bash",
                json!({ "command": "cat input.txt" }),
            ))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::Success);
        assert_eq!(result.output, "real");
    }
}

#[test]
fn bash_strips_dynamic_loader_environment() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["env".to_string()];
    policy.env.injected.insert(
        "LD_PRELOAD".to_string(),
        "/tmp/not-a-library.so".to_string(),
    );
    policy.env.injected.insert(
        "DYLD_INSERT_LIBRARIES".to_string(),
        "/tmp/not-a-library.dylib".to_string(),
    );
    policy
        .env
        .injected
        .insert("GOOD_NAME_2".to_string(), "visible".to_string());
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": "env" })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("GOOD_NAME_2=visible"));
    assert!(!result.output.contains("LD_PRELOAD="));
    assert!(!result.output.contains("DYLD_INSERT_LIBRARIES="));
}

#[test]
fn bash_ignores_empty_denied_command_policy_entries() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let command = "printf ok".to_string();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec![command.clone()];
    policy.process.denied_commands = vec!["".to_string(), "   ".to_string()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": command })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "ok");
}

#[test]
fn bash_ignores_invalid_environment_variable_names_without_panicking() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["env".to_string()];
    policy.env.allowlist = vec!["BAD\0ALLOW".to_string()];
    policy
        .env
        .injected
        .insert("BAD\0INJECT".to_string(), "secret".to_string());
    policy
        .env
        .injected
        .insert("BAD-NAME".to_string(), "hyphen-secret".to_string());
    policy
        .env
        .injected
        .insert("1BAD".to_string(), "numeric-secret".to_string());
    policy
        .env
        .injected
        .insert("GOOD_NAME_1".to_string(), "visible".to_string());
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(&session.id, "Bash", json!({ "command": "env" })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("GOOD_NAME_1=visible"));
    assert!(!result.output.contains("secret"));
}

#[test]
fn bash_enforces_allowed_command_allowlist() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["printf".to_string()];
    let session = session_with_policy(&host, policy);

    let allowed = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf ok" }),
        ))
        .unwrap();
    let denied = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "echo nope" }),
        ))
        .unwrap();

    assert_eq!(allowed.status, ToolResultStatus::Success);
    assert_eq!(allowed.output, "ok");
    assert_eq!(denied.status, ToolResultStatus::PolicyDenied);
    assert!(denied.effects.is_empty());
}

#[test]
fn bash_allowed_command_prefix_requires_token_boundary() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["printf".to_string()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printfx should-not-run" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.effects.is_empty());
}

#[test]
fn bash_allowed_command_does_not_allow_shell_control_bypass() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["printf".to_string()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf ok; echo bypass > bypass.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.effects.is_empty());
    assert!(!std::path::Path::new(&format!("{}/bypass.txt", session.workspace.root)).exists());
}

#[test]
fn bash_denies_when_process_limit_is_zero_without_side_effects() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.max_processes = Some(0);
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf bad > limit.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.effects.is_empty());
    assert!(!std::path::Path::new(&format!("{}/limit.txt", session.workspace.root)).exists());
}

#[test]
fn bash_rejects_positive_process_limit_until_enforced() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.max_processes = Some(1);
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf should-not-run" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.effects.is_empty());
}

#[test]
fn bash_rejects_symlink_cwd_escape_without_running() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = session_with_allowed_command(&host, "printf escaped > wrote.txt");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, format!("{}/link", session.workspace.root)).unwrap();
        let mut request = invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf escaped > wrote.txt" }),
        );
        request.cwd = Some("/workspace/link".to_string());

        let result = host.execute_invocation(request).unwrap();

        assert_eq!(result.status, ToolResultStatus::PolicyDenied);
        assert!(result.effects.is_empty());
        assert!(!outside.join("wrote.txt").exists());
    }
}

#[test]
fn bash_enforces_read_roots_on_cwd_before_running() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy(true, false);
    limited_policy.read_roots = vec!["/workspace/public".to_string()];
    limited_policy.process.allowed_commands = vec!["ls".to_string()];
    let session = session_with_policy(&host, limited_policy);
    fs::create_dir_all(format!("{}/public", session.workspace.root)).unwrap();
    fs::create_dir_all(format!("{}/private", session.workspace.root)).unwrap();
    fs::write(format!("{}/private/secret.txt", session.workspace.root), "").unwrap();
    let mut request = invoke(&session.id, "Bash", json!({ "command": "ls" }));
    request.cwd = Some("/workspace/private".to_string());

    let result = host.execute_invocation(request).unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(!result.output.contains("secret.txt"));
    assert!(result.error.unwrap().contains("Read denied"));
}

#[test]
fn bash_enforces_write_roots_on_cwd_before_running() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy(true, false);
    limited_policy.read_roots = vec!["/workspace".to_string()];
    limited_policy.write_roots = vec!["/workspace/public".to_string()];
    limited_policy.process.allowed_commands = vec!["touch should-not-exist".to_string()];
    let session = session_with_policy(&host, limited_policy);
    fs::create_dir_all(format!("{}/public", session.workspace.root)).unwrap();
    fs::create_dir_all(format!("{}/private", session.workspace.root)).unwrap();
    let mut request = invoke(
        &session.id,
        "Bash",
        json!({ "command": "touch should-not-exist" }),
    );
    request.cwd = Some("/workspace/private".to_string());

    let result = host.execute_invocation(request).unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.effects.is_empty());
    assert!(!std::path::Path::new(&format!(
        "{}/private/should-not-exist",
        session.workspace.root
    ))
    .exists());
    assert!(result.error.unwrap().contains("Write denied"));
}

#[test]
fn bash_timeout_stops_command_before_late_side_effect() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session_with_allowed_command(&host, "sleep 2; printf late > timed_out.txt");

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "sleep 2; printf late > timed_out.txt", "timeout": 1 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Timeout);
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, "process.exec");
    assert!(!std::path::Path::new(&format!("{}/timed_out.txt", session.workspace.root)).exists());
}

#[test]
fn bash_timeout_records_process_effect_even_after_early_side_effect() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session =
        session_with_allowed_command(&host, "printf early > early.txt; sleep 2; printf late");

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf early > early.txt; sleep 2; printf late", "timeout": 1 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Timeout);
    assert_eq!(
        fs::read_to_string(format!("{}/early.txt", session.workspace.root)).unwrap(),
        "early"
    );
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, "process.exec");
    assert_eq!(result.effects[0].resource.resource_type, "process");
    assert!(result.effects[0]
        .summary
        .as_ref()
        .unwrap()
        .contains("exit code None"));
}

#[test]
fn bash_session_max_duration_caps_tool_timeout_argument() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.max_duration_ms = Some(200);
    policy.process.allowed_commands = vec!["sleep 3; printf late".to_string()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "sleep 3; printf late", "timeout": 5 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Timeout);
    assert!(result.output.is_empty());
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, "process.exec");
    assert!(result.duration_ms < 2_500);
}

#[test]
fn bash_request_timeout_ms_caps_tool_timeout_argument() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session_with_allowed_command(&host, "sleep 1; printf late");
    let mut request = invoke(
        &session.id,
        "Bash",
        json!({ "command": "sleep 1; printf late", "timeout": 5 }),
    );
    request.timeout_ms = Some(200);

    let result = host.execute_invocation(request).unwrap();

    assert_eq!(result.status, ToolResultStatus::Timeout);
    assert!(result.output.is_empty());
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, "process.exec");
    assert!(result.duration_ms < 2_500);
}

#[test]
fn bash_timeout_kills_background_process_group_before_late_side_effect() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session =
        session_with_allowed_command(&host, "(sleep 2; printf late > orphaned.txt) & sleep 5");

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({
                "command": "(sleep 2; printf late > orphaned.txt) & sleep 5",
                "timeout": 1
            }),
        ))
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(2500));

    assert_eq!(result.status, ToolResultStatus::Timeout);
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, "process.exec");
    assert!(!std::path::Path::new(&format!("{}/orphaned.txt", session.workspace.root)).exists());
}

#[test]
fn bash_success_kills_background_process_group_before_late_side_effect() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session =
        session_with_allowed_command(&host, "(sleep 1; printf late > orphaned.txt) & printf done");

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "(sleep 1; printf late > orphaned.txt) & printf done" }),
        ))
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(1500));

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "done");
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, "process.exec");
    assert!(!std::path::Path::new(&format!("{}/orphaned.txt", session.workspace.root)).exists());
}

#[test]
fn bash_background_stdout_does_not_block_result_collection() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session_with_allowed_command(&host, "sleep 2 & printf done");
    let started = std::time::Instant::now();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "sleep 2 & printf done", "timeout": 5 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "done");
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[test]
fn bash_nonzero_exit_records_process_effect_and_stderr() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session_with_allowed_command(&host, "printf err >&2; exit 7");

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf err >&2; exit 7" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.output.contains("[stderr]: err"));
    assert_eq!(result.metadata["returnCode"], 7);
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, "process.exec");
    assert_eq!(result.effects[0].resource.resource_type, "process");
}

#[test]
fn bash_truncates_output_to_policy_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.max_output_bytes = Some(64);
    policy.process.allowed_commands =
        vec!["for i in {1..200}; do printf xxxxxxxxxx; done".to_string()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "for i in {1..200}; do printf xxxxxxxxxx; done" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.ends_with("\n...[truncated]"));
    assert!(result.output.len() < 100);
}

#[test]
fn bash_respects_request_output_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands =
        vec!["for i in {1..200}; do printf xxxxxxxxxx; done".to_string()];
    let session = session_with_policy(&host, policy);
    let mut request = invoke(
        &session.id,
        "Bash",
        json!({ "command": "for i in {1..200}; do printf xxxxxxxxxx; done" }),
    );
    request.max_output_bytes = Some(16);

    let result = host.execute_invocation(request).unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.starts_with("xxxxxxxxxxxxxxxx"));
    assert!(!result.output.contains("xxxxxxxxxxxxxxxxx"));
    assert!(result.output.ends_with("\n...[truncated]"));
}

#[test]
fn invocation_rejects_excessive_request_output_limit_without_running_tool() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["touch should-not-exist".to_string()];
    let session = session_with_policy(&host, policy);
    let mut request = invoke(
        &session.id,
        "Bash",
        json!({ "command": "touch should-not-exist" }),
    );
    request.max_output_bytes = Some(executioner_core::MAX_OUTPUT_BYTES + 1);

    let err = host.execute_invocation(request).unwrap_err();

    assert!(err.to_string().contains("maximum supported output size"));
    assert!(!std::path::Path::new(&session.workspace.root)
        .join("should-not-exist")
        .exists());
}

#[test]
fn invocation_rejects_excessive_request_timeout_without_running_tool() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["touch should-not-exist".to_string()];
    let session = session_with_policy(&host, policy);
    let mut request = invoke(
        &session.id,
        "Bash",
        json!({ "command": "touch should-not-exist" }),
    );
    request.timeout_ms = Some(executioner_core::MAX_TOOL_TIMEOUT_MS + 1);

    let err = host.execute_invocation(request).unwrap_err();

    assert!(err.to_string().contains("maximum supported tool timeout"));
    assert!(!std::path::Path::new(&session.workspace.root)
        .join("should-not-exist")
        .exists());
}

#[test]
fn invocation_rejects_zero_request_timeout_without_running_tool() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["touch should-not-exist".to_string()];
    let session = session_with_policy(&host, policy);
    let mut request = invoke(
        &session.id,
        "Bash",
        json!({ "command": "touch should-not-exist" }),
    );
    request.timeout_ms = Some(0);

    let err = host.execute_invocation(request).unwrap_err();

    assert!(err.to_string().contains("must be positive"));
    assert!(!std::path::Path::new(&session.workspace.root)
        .join("should-not-exist")
        .exists());
}

#[test]
fn invocation_rejects_oversized_direct_request_without_running_tool() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.process.allowed_commands = vec!["touch should-not-exist".to_string()];
    let session = session_with_policy(&host, policy);
    let mut request = invoke(
        &session.id,
        "Bash",
        json!({ "command": "touch should-not-exist" }),
    );
    request
        .metadata
        .insert("padding".to_string(), json!("x".repeat(1024 * 1024)));

    let err = host.execute_invocation(request).unwrap_err();

    assert!(err.to_string().contains("maximum JSON size"));
    assert!(!std::path::Path::new(&session.workspace.root)
        .join("should-not-exist")
        .exists());
}

#[test]
fn bash_rejects_excessive_tool_timeout_without_running() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let command = "touch should-not-exist";
    let session = session_with_allowed_command(&host, command);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({
                "command": command,
                "timeout": (executioner_core::MAX_TOOL_TIMEOUT_MS / 1000) + 1
            }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result
        .error
        .unwrap()
        .contains("maximum supported tool timeout"));
    assert!(!std::path::Path::new(&session.workspace.root)
        .join("should-not-exist")
        .exists());
}

#[test]
fn bash_rejects_zero_tool_timeout_without_running() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let command = "touch should-not-exist";
    let session = session_with_allowed_command(&host, command);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": command, "timeout": 0 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("must be positive"));
    assert!(!std::path::Path::new(&session.workspace.root)
        .join("should-not-exist")
        .exists());
}

#[test]
fn bash_respects_zero_session_output_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.max_output_bytes = Some(0);
    policy.process.allowed_commands = vec!["printf secret".to_string()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf secret" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "");
}

#[test]
fn bash_truncates_unicode_output_without_panic() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut policy = policy(true, false);
    policy.max_output_bytes = Some(1);
    policy.process.allowed_commands = vec!["printf '🙂🙂🙂'".to_string()];
    let session = session_with_policy(&host, policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Bash",
            json!({ "command": "printf '🙂🙂🙂'" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.ends_with("\n...[truncated]"));
}

#[test]
fn grep_truncates_unicode_match_without_panic() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::write(
        format!("{}/unicode.txt", session.workspace.root),
        format!("match {}", "🙂".repeat(80)),
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Grep",
            json!({ "pattern": "match", "path": "unicode.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("unicode.txt:1: match"));
}

#[test]
fn grep_respects_zero_session_output_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy(false, false);
    limited_policy.max_output_bytes = Some(0);
    let session = session_with_policy(&host, limited_policy);
    fs::write(
        format!("{}/secret.txt", session.workspace.root),
        "secret token",
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Grep",
            json!({ "pattern": "secret", "path": "secret.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "");
    assert_eq!(result.metadata["matchCount"], 1);
}

#[test]
fn glob_finds_matching_files() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::create_dir_all(format!("{}/src", session.workspace.root)).unwrap();
    fs::write(format!("{}/src/a.rs", session.workspace.root), "").unwrap();
    fs::write(format!("{}/src/b.ts", session.workspace.root), "").unwrap();

    let result = host
        .execute_invocation(invoke(&session.id, "Glob", json!({ "pattern": "**/*.rs" })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("src/a.rs"));
    assert!(!result.output.contains("src/b.ts"));
}

#[test]
fn list_returns_immediate_visible_entries_for_cwd() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::create_dir_all(format!("{}/dir/nested", session.workspace.root)).unwrap();
    fs::write(format!("{}/a.txt", session.workspace.root), "").unwrap();
    fs::write(format!("{}/.secret", session.workspace.root), "").unwrap();
    fs::write(format!("{}/dir/b.txt", session.workspace.root), "").unwrap();

    let root_result = host
        .execute_invocation(invoke(&session.id, "List", json!({})))
        .unwrap();
    let mut dir_request = invoke(&session.id, "List", json!({}));
    dir_request.cwd = Some("/workspace/dir".to_string());
    let dir_result = host.execute_invocation(dir_request).unwrap();

    assert_eq!(root_result.status, ToolResultStatus::Success);
    assert_eq!(root_result.output, "a.txt\ndir/");
    assert_eq!(root_result.metadata["entryCount"], 2);
    assert_eq!(dir_result.status, ToolResultStatus::Success);
    assert_eq!(dir_result.output, "b.txt\nnested/");
}

#[test]
fn list_caps_structured_entries_to_bound_result_size() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    for index in 0..1005 {
        fs::write(
            format!("{}/file_{index:04}.txt", session.workspace.root),
            "",
        )
        .unwrap();
    }

    let result = host
        .execute_invocation(invoke(&session.id, "List", json!({})))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.metadata["entryCount"], 1000);
    assert_eq!(result.metadata["totalEntries"], 1005);
    assert_eq!(result.metadata["truncated"], true);
    assert_eq!(result.metadata["entries"].as_array().unwrap().len(), 1000);
    assert!(result
        .output
        .contains("...[truncated at 1000 entries, 1005 total]"));
}

#[test]
fn list_stops_after_traversal_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    for index in 0..10_050 {
        fs::write(
            format!("{}/file_{index:05}.txt", session.workspace.root),
            "",
        )
        .unwrap();
    }

    let result = host
        .execute_invocation(invoke(&session.id, "List", json!({})))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.metadata["entryCount"], 1000);
    assert_eq!(result.metadata["totalEntries"], 10_000);
    assert_eq!(result.metadata["truncated"], true);
    assert_eq!(result.metadata["traversalLimitExceeded"], true);
}

#[test]
fn list_rejects_nul_cwd_as_tool_error() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    let mut request = invoke(&session.id, "List", json!({}));
    request.cwd = Some("/workspace/bad\0cwd".to_string());

    let result = host.execute_invocation(request).unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("path contains invalid"));
}

#[test]
fn list_glob_and_grep_enforce_read_roots_on_search_root() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy(false, false);
    limited_policy.read_roots = vec!["/workspace/public".to_string()];
    let session = session_with_policy(&host, limited_policy);
    fs::create_dir_all(format!("{}/public", session.workspace.root)).unwrap();
    fs::write(
        format!("{}/public/allowed.txt", session.workspace.root),
        "needle",
    )
    .unwrap();
    fs::write(format!("{}/secret.txt", session.workspace.root), "needle").unwrap();

    let list_root = host
        .execute_invocation(invoke(&session.id, "List", json!({})))
        .unwrap();
    let glob_root = host
        .execute_invocation(invoke(
            &session.id,
            "Glob",
            json!({ "pattern": "**/*.txt" }),
        ))
        .unwrap();
    let grep_root = host
        .execute_invocation(invoke(
            &session.id,
            "Grep",
            json!({ "pattern": "needle", "path": "." }),
        ))
        .unwrap();
    let mut list_public_request = invoke(&session.id, "List", json!({}));
    list_public_request.cwd = Some("/workspace/public".to_string());
    let list_public = host.execute_invocation(list_public_request).unwrap();

    assert_eq!(list_root.status, ToolResultStatus::PolicyDenied);
    assert_eq!(glob_root.status, ToolResultStatus::PolicyDenied);
    assert_eq!(grep_root.status, ToolResultStatus::PolicyDenied);
    assert_eq!(list_public.status, ToolResultStatus::Success);
    assert_eq!(list_public.output, "allowed.txt");
}

#[test]
fn list_rejects_unexpected_arguments_instead_of_ignoring_them() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::write(format!("{}/visible.txt", session.workspace.root), "").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "List",
            json!({ "includeHidden": true }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("unexpected argument"));
}

#[test]
fn process_and_search_tools_reject_unexpected_arguments() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session_with_allowed_command(&host, "echo");
    fs::write(format!("{}/visible.txt", session.workspace.root), "needle").unwrap();

    for (tool_name, args) in [
        ("Bash", json!({ "command": "echo hi", "environment": {} })),
        ("Glob", json!({ "pattern": "*", "include_hidden": true })),
        (
            "Grep",
            json!({ "pattern": "needle", "includeHidden": true }),
        ),
        (
            "Edit",
            json!({
                "path": "visible.txt",
                "oldString": "needle",
                "newString": "thread",
                "dryRun": true,
            }),
        ),
    ] {
        let result = host
            .execute_invocation(invoke(&session.id, tool_name, args))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::Error, "{tool_name}");
        assert!(
            result.error.unwrap().contains("unexpected argument"),
            "{tool_name}"
        );
    }
    assert_eq!(
        fs::read_to_string(format!("{}/visible.txt", session.workspace.root)).unwrap(),
        "needle"
    );
}

#[test]
fn glob_respects_zero_session_output_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy(false, false);
    limited_policy.max_output_bytes = Some(0);
    let session = session_with_policy(&host, limited_policy);
    fs::write(format!("{}/visible.txt", session.workspace.root), "").unwrap();

    let result = host
        .execute_invocation(invoke(&session.id, "Glob", json!({ "pattern": "*" })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "");
    assert_eq!(result.metadata["totalMatches"], 1);
}

#[test]
fn glob_double_star_slash_matches_root_and_nested_files() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::write(format!("{}/root.txt", session.workspace.root), "").unwrap();
    fs::create_dir_all(format!("{}/nested", session.workspace.root)).unwrap();
    fs::write(format!("{}/nested/child.txt", session.workspace.root), "").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Glob",
            json!({ "pattern": "**/*.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("root.txt"));
    assert!(result.output.contains("nested/child.txt"));
    assert_eq!(result.metadata["totalMatches"], 2);
}

#[test]
fn glob_caps_unbounded_max_results_request() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    for idx in 0..1100 {
        fs::write(format!("{}/file-{idx:04}.txt", session.workspace.root), "").unwrap();
    }

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Glob",
            json!({ "pattern": "*.txt", "maxResults": 1_000_000 }),
        ))
        .unwrap();
    let listed = result
        .output
        .lines()
        .filter(|line| !line.starts_with("...[truncated"))
        .count();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(listed, 1000);
    assert_eq!(result.metadata["totalMatches"], 1100);
    assert!(result.metadata["truncated"].as_bool().unwrap());
    assert!(result.output.contains("...[truncated at 1000 results"));
}

#[test]
fn glob_rejects_invalid_boolean_options_instead_of_defaulting() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Glob",
            json!({ "pattern": "*", "includeHidden": "false" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("includeHidden must be"));
}

#[test]
fn glob_rejects_oversized_patterns_before_compiling() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    let pattern = "a".repeat(65_536);

    let result = host
        .execute_invocation(invoke(&session.id, "Glob", json!({ "pattern": pattern })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("pattern exceeds maximum"));
}

#[test]
fn glob_stops_after_traversal_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    for index in 0..10_050 {
        fs::write(
            format!("{}/file_{index:05}.txt", session.workspace.root),
            "",
        )
        .unwrap();
    }

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Glob",
            json!({ "pattern": "*.txt", "maxResults": 1000 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.metadata["matchCount"], 1000);
    assert_eq!(result.metadata["totalMatches"], 10_000);
    assert_eq!(result.metadata["truncated"], true);
    assert_eq!(result.metadata["traversalLimitExceeded"], true);
}

#[test]
fn grep_rejects_invalid_string_filters_instead_of_defaulting() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::write(format!("{}/a.txt", session.workspace.root), "needle").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Grep",
            json!({ "pattern": "needle", "glob": ["*.txt"] }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("glob must be a string"));
}

#[test]
fn grep_rejects_oversized_patterns_before_compiling() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    let pattern = "a".repeat(65_536);

    let result = host
        .execute_invocation(invoke(&session.id, "Grep", json!({ "pattern": pattern })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("pattern exceeds maximum"));
}

#[test]
fn grep_rejects_oversized_glob_filters_before_compiling() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    let glob = "a".repeat(65_536);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Grep",
            json!({ "pattern": "needle", "glob": glob }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("glob exceeds maximum"));
}

#[test]
fn grep_rejects_oversized_candidate_files_before_reading() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    let large_path = format!("{}/large.log", session.workspace.root);
    fs::File::create(&large_path)
        .unwrap()
        .set_len(17 * 1024 * 1024)
        .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Grep",
            json!({ "pattern": "needle", "path": "large.log" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result
        .error
        .unwrap()
        .contains("exceeds maximum searchable size"));
}

#[test]
fn grep_errors_after_traversal_limit_without_matches() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    for index in 0..10_050 {
        fs::write(
            format!("{}/file_{index:05}.txt", session.workspace.root),
            "haystack",
        )
        .unwrap();
    }

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Grep",
            json!({ "pattern": "needle", "path": "." }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("maximum traversal count"));
}

#[test]
fn grep_finds_regex_matches() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::write(
        format!("{}/a.txt", session.workspace.root),
        "one\ntwo\nthree",
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Grep",
            json!({ "pattern": "tw.", "path": "." }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("a.txt:2: two"));
}

#[test]
fn grep_glob_double_star_slash_matches_root_and_nested_files() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    fs::write(format!("{}/root.txt", session.workspace.root), "needle").unwrap();
    fs::create_dir_all(format!("{}/nested", session.workspace.root)).unwrap();
    fs::write(
        format!("{}/nested/child.txt", session.workspace.root),
        "needle",
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Grep",
            json!({ "pattern": "needle", "path": ".", "glob": "**/*.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("root.txt:1: needle"));
    assert!(result.output.contains("nested/child.txt:1: needle"));
    assert_eq!(result.metadata["matchCount"], 2);
}

#[test]
fn grep_recursive_search_does_not_follow_symlink_escape() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = session(&host, false, false);
        let outside = temp.path().join("outside-secret.txt");
        fs::write(&outside, "needle outside workspace").unwrap();
        std::os::unix::fs::symlink(&outside, format!("{}/link.txt", session.workspace.root))
            .unwrap();

        let result = host
            .execute_invocation(invoke(
                &session.id,
                "Grep",
                json!({ "pattern": "needle", "path": "." }),
            ))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::Success);
        assert!(!result.output.contains("outside workspace"));
        assert_eq!(result.output, "No matches found for pattern: needle");
    }
}

#[test]
fn removed_and_non_substrate_tools_are_not_registered() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);

    for tool in [
        "BatchEdit",
        "apply_patch",
        "PromptUser",
        "WebFetch",
        "WebSearch",
        "ExpandConversation",
    ] {
        let err = host
            .execute_invocation(invoke(
                &session.id,
                tool,
                json!({ "url": "https://example.com", "query": "example" }),
            ))
            .unwrap_err();
        assert!(err.to_string().contains("tool not found"), "{tool}: {err}");
    }
}

#[test]
fn invocation_with_required_capabilities_fails_closed_without_running_tool() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    let mut request = invoke(
        &session.id,
        "Write",
        json!({ "path": "capability.txt", "content": "should not run" }),
    );
    request.required_capabilities = vec![ToolCapability {
        kind: "network".to_string(),
        scope: Map::new(),
    }];

    let result = host.execute_invocation(request).unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result
        .error
        .unwrap()
        .contains("required capabilities are not supported"));
    assert!(!std::path::Path::new(&format!("{}/capability.txt", session.workspace.root)).exists());
}

#[test]
fn invocation_with_idempotency_key_fails_closed_without_running_tool() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = session(&host, false, false);
    let mut request = invoke(
        &session.id,
        "Write",
        json!({ "path": "idempotent.txt", "content": "should not run" }),
    );
    request.idempotency_key = Some("idem-1".to_string());

    let result = host.execute_invocation(request).unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.error.unwrap().contains("idempotencyKey"));
    assert!(!std::path::Path::new(&format!("{}/idempotent.txt", session.workspace.root)).exists());
}
