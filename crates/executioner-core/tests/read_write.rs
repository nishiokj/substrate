use executioner_core::{
    CreateEnvironmentRequest, CreateSessionRequest, ExecutionPolicy, HostState, NetworkPolicy,
    ProcessPolicy, ResourceRef, ToolInvocationRequest, ToolResultStatus, WorkspaceArtifact,
    WorkspaceArtifactEntry, WorkspaceMode, WorkspaceSpec,
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read as _;
use std::time::Duration;
use tempfile::TempDir;

fn policy() -> ExecutionPolicy {
    ExecutionPolicy {
        read_roots: vec!["/workspace".to_string()],
        write_roots: vec!["/workspace".to_string()],
        process: ProcessPolicy {
            allow_exec: false,
            allowed_commands: vec![],
            denied_commands: vec![],
            max_processes: None,
        },
        network: NetworkPolicy {
            enabled: false,
            allow_hosts: vec![],
            deny_hosts: vec![],
        },
        ..ExecutionPolicy::default()
    }
}

fn create_session(host: &HostState) -> executioner_core::Session {
    create_session_with_policy(host, policy())
}

fn create_session_with_policy(
    host: &HostState,
    policy: ExecutionPolicy,
) -> executioner_core::Session {
    create_session_with_policy_and_id(host, policy, "env", "sess")
}

fn create_session_with_policy_and_id(
    host: &HostState,
    policy: ExecutionPolicy,
    environment_id: &str,
    session_id: &str,
) -> executioner_core::Session {
    host.create_environment(CreateEnvironmentRequest {
        environment_id: Some(environment_id.to_string()),
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
    .environment;
    host.create_session(
        environment_id,
        CreateSessionRequest {
            session_id: Some(session_id.to_string()),
            policy: None,
            metadata: Map::new(),
        },
    )
    .unwrap()
    .session
}

fn create_existing_session(host: &HostState, root: &std::path::Path) -> executioner_core::Session {
    host.create_environment(CreateEnvironmentRequest {
        environment_id: Some("env_existing".to_string()),
        workspace: WorkspaceSpec {
            mode: WorkspaceMode::Existing,
            root: Some(root.to_string_lossy().into_owned()),
            snapshot_ref: None,
            template_ref: None,
            mount_as_workspace: true,
        },
        policy: policy(),
        ttl_ms: None,
        metadata: Map::new(),
    })
    .unwrap()
    .environment;
    host.create_session(
        "env_existing",
        CreateSessionRequest {
            session_id: Some("sess_existing".to_string()),
            policy: None,
            metadata: Map::new(),
        },
    )
    .unwrap()
    .session
}

fn create_session_with_id(
    host: &HostState,
    session_id: &str,
) -> executioner_core::Result<executioner_core::CreateSessionResponse> {
    host.create_environment(CreateEnvironmentRequest {
        environment_id: Some(session_id.to_string()),
        workspace: WorkspaceSpec {
            mode: WorkspaceMode::New,
            root: None,
            snapshot_ref: None,
            template_ref: None,
            mount_as_workspace: true,
        },
        policy: policy(),
        ttl_ms: None,
        metadata: Map::new(),
    })?;
    host.create_session(
        session_id,
        CreateSessionRequest {
            session_id: Some(session_id.to_string()),
            policy: None,
            metadata: Map::new(),
        },
    )
}

fn invoke(session_id: &str, tool_name: &str, arguments: Value) -> ToolInvocationRequest {
    ToolInvocationRequest {
        invocation_id: Some(format!("inv_{tool_name}")),
        session_id: session_id.to_string(),
        tool_name: tool_name.to_string(),
        arguments: arguments.as_object().cloned().unwrap(),
        cwd: Some("/workspace".to_string()),
        timeout_ms: None,
        max_output_bytes: None,
        idempotency_key: None,
        required_capabilities: vec![],
        metadata: Map::new(),
    }
}

#[test]
fn multiple_sessions_can_share_one_environment_workspace() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let environment = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("env_shared".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: policy(),
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap()
        .environment;

    let session_a = host
        .create_session(
            &environment.id,
            CreateSessionRequest {
                session_id: Some("sess_a".to_string()),
                policy: None,
                metadata: Map::new(),
            },
        )
        .unwrap()
        .session;
    let session_b = host
        .create_session(
            &environment.id,
            CreateSessionRequest {
                session_id: Some("sess_b".to_string()),
                policy: None,
                metadata: Map::new(),
            },
        )
        .unwrap()
        .session;

    assert_eq!(session_a.workspace.root, environment.workspace.root);
    assert_eq!(session_b.workspace.root, environment.workspace.root);

    host.execute_invocation(invoke(
        &session_a.id,
        "Write",
        json!({ "path": "shared.txt", "content": "hello" }),
    ))
    .unwrap();

    let read = host
        .execute_invocation(invoke(
            &session_b.id,
            "Read",
            json!({ "path": "shared.txt" }),
        ))
        .unwrap();
    assert_eq!(read.output, "hello");
    assert_eq!(host.effects(&environment.id).unwrap().len(), 2);
    assert_eq!(host.get_environment(&environment.id).unwrap().revision, 1);
}

#[test]
fn many_sessions_can_share_one_environment_workspace() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let environment = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("env_many".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: policy(),
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap()
        .environment;

    let sessions = (0..30)
        .map(|index| {
            host.create_session(
                &environment.id,
                CreateSessionRequest {
                    session_id: Some(format!("sess_many_{index}")),
                    policy: None,
                    metadata: Map::new(),
                },
            )
            .unwrap()
            .session
        })
        .collect::<Vec<_>>();

    for (index, session) in sessions.iter().enumerate() {
        host.execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": format!("client-{index}.txt"), "content": format!("hello {index}") }),
        ))
        .unwrap();
    }

    let listing = host
        .execute_invocation(invoke(&sessions[29].id, "List", json!({})))
        .unwrap();
    assert_eq!(listing.status, ToolResultStatus::Success);
    assert!(listing.output.contains("client-0.txt"));
    assert!(listing.output.contains("client-29.txt"));
    assert_eq!(host.get_environment(&environment.id).unwrap().revision, 30);
}

#[test]
fn active_invocations_block_session_and_environment_teardown() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut exec_policy = policy();
    exec_policy.process.allow_exec = true;
    exec_policy.process.allowed_commands = vec!["sleep".to_string()];
    let session =
        create_session_with_policy_and_id(&host, exec_policy, "env_active", "sess_active");
    let worker_host = host.clone();
    let session_id = session.id.clone();
    let worker = std::thread::spawn(move || {
        worker_host
            .execute_invocation(invoke(&session_id, "Bash", json!({ "command": "sleep 1" })))
            .unwrap()
    });
    std::thread::sleep(Duration::from_millis(100));

    let mut blocked_session = None;
    let mut blocked_environment = None;
    for _ in 0..50 {
        blocked_session = host.close_session(&session.id).err();
        blocked_environment = host.destroy_environment("env_active").err();
        if blocked_session.is_some() && blocked_environment.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(blocked_session
        .unwrap()
        .to_string()
        .contains("session has active invocations"));
    assert!(blocked_environment
        .unwrap()
        .to_string()
        .contains("environment has active invocations"));

    let result = worker.join().unwrap();
    assert_eq!(result.status, ToolResultStatus::Success);
    host.destroy_environment("env_active").unwrap();
}

#[test]
fn environment_invocations_are_serialized_across_sessions() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut exec_policy = policy();
    exec_policy.process.allow_exec = true;
    exec_policy.process.allowed_commands = vec!["sleep".to_string()];
    let environment = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("env_serial".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: exec_policy,
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap()
        .environment;
    let sessions = ["sess_serial_a", "sess_serial_b"]
        .into_iter()
        .map(|session_id| {
            host.create_session(
                &environment.id,
                CreateSessionRequest {
                    session_id: Some(session_id.to_string()),
                    policy: None,
                    metadata: Map::new(),
                },
            )
            .unwrap()
            .session
        })
        .collect::<Vec<_>>();

    let started = std::time::Instant::now();
    let handles = sessions
        .into_iter()
        .map(|session| {
            let host = host.clone();
            std::thread::spawn(move || {
                host.execute_invocation(invoke(
                    &session.id,
                    "Bash",
                    json!({ "command": "sleep 0.3" }),
                ))
                .unwrap()
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        assert_eq!(handle.join().unwrap().status, ToolResultStatus::Success);
    }
    assert!(started.elapsed() >= Duration::from_millis(500));
}

#[test]
fn closing_one_session_preserves_shared_environment_for_other_sessions() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let environment = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("env_live".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: policy(),
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap()
        .environment;
    let session_a = host
        .create_session(
            &environment.id,
            CreateSessionRequest {
                session_id: Some("sess_live_a".to_string()),
                policy: None,
                metadata: Map::new(),
            },
        )
        .unwrap()
        .session;
    let session_b = host
        .create_session(
            &environment.id,
            CreateSessionRequest {
                session_id: Some("sess_live_b".to_string()),
                policy: None,
                metadata: Map::new(),
            },
        )
        .unwrap()
        .session;

    host.close_session(&session_a.id).unwrap();
    host.execute_invocation(invoke(
        &session_b.id,
        "Write",
        json!({ "path": "still-live.txt", "content": "ok" }),
    ))
    .unwrap();

    assert!(std::path::Path::new(&environment.workspace.root)
        .join("still-live.txt")
        .exists());
}

#[test]
fn new_workspace_rejects_environment_id_path_traversal() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();

    let err = create_session_with_id(&host, "../escaped").unwrap_err();

    assert!(err.to_string().contains("invalid environment id"));
    assert!(!temp.path().join("escaped").exists());
}

#[test]
fn new_workspace_rejects_environment_id_with_path_separator() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path().join("state")).unwrap();

    let err = create_session_with_id(&host, "nested/sess").unwrap_err();

    assert!(err.to_string().contains("invalid environment id"));
}

#[test]
fn host_state_rejects_symlink_state_directory() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let outside = temp.path().join("outside-state");
        let state_link = temp.path().join("state-link");
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, &state_link).unwrap();

        let err = HostState::new(&state_link).unwrap_err();

        assert!(err.to_string().contains("host state directory"));
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    }
}

#[test]
fn host_state_rejects_symlink_state_parent_directory() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let outside = temp.path().join("outside-parent");
        let link_parent = temp.path().join("linked-parent");
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, &link_parent).unwrap();

        let err = HostState::new(link_parent.join("state")).unwrap_err();

        assert!(err.to_string().contains("parent must not contain symlinks"));
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    }
}

#[test]
fn host_state_rejects_symlink_state_ancestor_directory() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let outside = temp.path().join("outside-parent");
        let link_parent = temp.path().join("linked-parent");
        fs::create_dir_all(outside.join("existing")).unwrap();
        std::os::unix::fs::symlink(&outside, &link_parent).unwrap();

        let err = HostState::new(link_parent.join("existing/state")).unwrap_err();

        assert!(err.to_string().contains("parent must not contain symlinks"));
        assert_eq!(fs::read_dir(outside.join("existing")).unwrap().count(), 0);
    }
}

#[test]
fn new_workspace_rejects_preexisting_environment_symlink_escape() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, state_dir.join("sess_link")).unwrap();

        let host = HostState::new(&state_dir).unwrap();
        let err = create_session_with_id(&host, "sess_link").unwrap_err();

        assert!(err.to_string().contains("managed workspace"));
        assert!(!outside.join("workspace").exists());
    }
}

#[test]
fn destroy_unlinks_swapped_managed_session_symlink_without_following_it() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("secret.txt"), "keep me").unwrap();
        let host = HostState::new(&state_dir).unwrap();
        let session = create_session_with_id(&host, "sess_swapped_cleanup")
            .unwrap()
            .session;
        let session_dir = std::path::Path::new(&session.workspace.root)
            .parent()
            .unwrap()
            .to_path_buf();

        fs::remove_dir_all(&session_dir).unwrap();
        std::os::unix::fs::symlink(&outside, &session_dir).unwrap();

        host.destroy_environment("sess_swapped_cleanup").unwrap();

        assert!(!session_dir.exists());
        assert_eq!(
            fs::read_to_string(outside.join("secret.txt")).unwrap(),
            "keep me"
        );
    }
}

#[test]
fn existing_workspace_rejects_symlinked_parent_component() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let outside = temp.path().join("outside");
        let link_parent = temp.path().join("link-parent");
        fs::create_dir_all(outside.join("workspace")).unwrap();
        std::os::unix::fs::symlink(&outside, &link_parent).unwrap();

        let err = host
            .create_environment(CreateEnvironmentRequest {
                environment_id: Some("sess_symlink_parent".to_string()),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::Existing,
                    root: Some(link_parent.join("workspace").display().to_string()),
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: policy(),
                ttl_ms: None,
                metadata: Map::new(),
            })
            .unwrap_err();

        assert!(err.to_string().contains("workspace.root parent"));
    }
}

#[test]
fn existing_workspace_rejects_root_swapped_to_symlink_before_tool_use() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("secret.txt"), "outside secret").unwrap();
        let session = create_existing_session(&host, &workspace);

        fs::remove_dir_all(&workspace).unwrap();
        std::os::unix::fs::symlink(&outside, &workspace).unwrap();

        let err = host
            .execute_invocation(invoke(&session.id, "Read", json!({ "path": "secret.txt" })))
            .unwrap_err();

        assert!(err.to_string().contains("workspace.root"));
    }
}

#[test]
fn export_workspace_rejects_root_swapped_to_symlink_before_archiving() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("secret.txt"), "outside secret").unwrap();
        let _session = create_existing_session(&host, &workspace);

        fs::remove_dir_all(&workspace).unwrap();
        std::os::unix::fs::symlink(&outside, &workspace).unwrap();

        let err = host.export_workspace("env_existing").unwrap_err();

        assert!(err.to_string().contains("workspace.root"));
    }
}

#[test]
fn direct_export_workspace_rejects_symlinked_workspace_root_before_creating_output_dir() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        let state = temp.path().join("state");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let host = HostState::new(&state).unwrap();
        let session = create_existing_session(&host, &workspace);

        fs::remove_dir_all(&workspace).unwrap();
        std::os::unix::fs::symlink(&outside, &workspace).unwrap();
        let output_dir = temp.path().join("artifacts");

        let err = executioner_core::artifact::export_workspace(&session, &output_dir).unwrap_err();

        assert!(err.to_string().contains("workspace.root"));
        assert!(!output_dir.exists());
    }
}

#[test]
fn host_rejects_invalid_session_id_on_lifecycle_operations() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path().join("state")).unwrap();

    for err in [
        host.get_session("../escaped").unwrap_err(),
        host.close_session("../escaped").unwrap_err(),
        host.destroy_session("../escaped").unwrap_err(),
        host.effects("../escaped").unwrap_err(),
        host.export_workspace("../escaped").unwrap_err(),
    ] {
        assert!(err.to_string().contains("invalid"));
    }
}

#[test]
fn host_rejects_invalid_invocation_id_without_running_tool() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);

    let err = host
        .execute_invocation(ToolInvocationRequest {
            invocation_id: Some("../escaped".to_string()),
            session_id: session.id.clone(),
            tool_name: "Write".to_string(),
            arguments: json!({ "path": "created.txt", "content": "bad" })
                .as_object()
                .cloned()
                .unwrap(),
            cwd: Some("/workspace".to_string()),
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err.to_string().contains("invalid invocation id"));
    assert!(!std::path::Path::new(&format!("{}/created.txt", session.workspace.root)).exists());
}

#[test]
fn create_environment_rejects_unenforceable_network_policy_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();
    let mut network_policy = policy();
    network_policy.network.enabled = true;

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("network_enabled".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: network_policy,
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("network policy is not enforceable"));
    assert!(!state_dir.join("network_enabled").exists());

    let mut host_list_policy = policy();
    host_list_policy.network.allow_hosts = vec!["example.com".to_string()];
    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("network_hosts".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: host_list_policy,
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("network policy is not enforceable"));
    assert!(!state_dir.join("network_hosts").exists());
}

#[test]
fn create_environment_rejects_ignored_workspace_fields_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();

    for (session_id, workspace, expected_error) in [
        (
            "new_with_root",
            WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: Some(temp.path().join("caller-root").display().to_string()),
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            "new workspaces must not include root",
        ),
        (
            "new_with_snapshot_ref",
            WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: Some("snap-1".to_string()),
                template_ref: None,
                mount_as_workspace: true,
            },
            "new workspaces must not include root",
        ),
        (
            "existing_with_template_ref",
            WorkspaceSpec {
                mode: WorkspaceMode::Existing,
                root: Some(temp.path().display().to_string()),
                snapshot_ref: None,
                template_ref: Some("template-1".to_string()),
                mount_as_workspace: true,
            },
            "existing workspaces must not include",
        ),
        (
            "unmounted",
            WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: false,
            },
            "mountAsWorkspace=false",
        ),
    ] {
        let err = host
            .create_environment(CreateEnvironmentRequest {
                environment_id: Some(session_id.to_string()),
                workspace,
                policy: policy(),
                ttl_ms: None,
                metadata: Map::new(),
            })
            .unwrap_err();

        assert!(
            err.to_string().contains(expected_error),
            "{session_id}: {err}"
        );
        assert!(!state_dir.join(session_id).exists(), "{session_id}");
    }
}

#[test]
fn create_environment_rejects_excessive_ttl_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("huge_ttl".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: policy(),
            ttl_ms: Some(u64::MAX),
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err.to_string().contains("ttlMs exceeds maximum"));
    assert!(!state_dir.join("huge_ttl").exists());
}

#[test]
fn create_environment_rejects_excessive_output_limit_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();
    let mut excessive_policy = policy();
    excessive_policy.max_output_bytes = Some(executioner_core::MAX_OUTPUT_BYTES + 1);

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("huge_output".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: excessive_policy,
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err.to_string().contains("maximum supported output size"));
    assert!(!state_dir.join("huge_output").exists());
}

#[test]
fn create_environment_rejects_oversized_direct_request_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();
    let mut metadata = Map::new();
    metadata.insert("padding".to_string(), json!("x".repeat(1024 * 1024)));

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("huge_request".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: policy(),
            ttl_ms: None,
            metadata,
        })
        .unwrap_err();

    assert!(err.to_string().contains("maximum JSON size"));
    assert!(!state_dir.join("huge_request").exists());
}

#[test]
fn create_environment_rejects_excessive_duration_limit_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();
    let mut excessive_policy = policy();
    excessive_policy.max_duration_ms = Some(executioner_core::MAX_TOOL_TIMEOUT_MS + 1);

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("huge_duration".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: excessive_policy,
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err.to_string().contains("maximum supported tool timeout"));
    assert!(!state_dir.join("huge_duration").exists());
}

#[test]
fn create_environment_rejects_zero_duration_limit_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();
    let mut zero_policy = policy();
    zero_policy.max_duration_ms = Some(0);

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("zero_duration".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: zero_policy,
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err.to_string().contains("must be positive"));
    assert!(!state_dir.join("zero_duration").exists());
}

#[test]
fn write_creates_file_with_metadata_and_effect() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "new.txt", "content": "Hello, World!" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("Created /workspace/new.txt"));
    assert_eq!(result.metadata["action"], "write");
    assert_eq!(result.metadata["atomic"], true);
    assert!(result.metadata.get("hostPath").is_none());
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, "file.write");
    assert_eq!(result.effects[0].resource.uri, "file:///workspace/new.txt");
    assert_eq!(
        fs::read_to_string(format!("{}/new.txt", session.workspace.root)).unwrap(),
        "Hello, World!"
    );
}

#[test]
fn write_creates_parent_directories_and_preserves_unicode() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    let content = "Japanese 日本語 and emoji 🎉";

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "a/b/c/unicode.txt", "content": content }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(
        fs::read_to_string(format!("{}/a/b/c/unicode.txt", session.workspace.root)).unwrap(),
        content
    );
}

#[test]
fn write_rejects_parent_outside_write_roots_before_creating_directories() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy();
    limited_policy.write_roots = vec!["/workspace/allowed/file.txt".to_string()];
    let session = create_session_with_policy(&host, limited_policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "allowed/file.txt", "content": "should not write" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.error.unwrap().contains("Write denied"));
    assert!(result.effects.is_empty());
    assert!(!std::path::Path::new(&format!("{}/allowed", session.workspace.root)).exists());
}

#[test]
fn write_fails_if_file_exists_without_mutating() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::write(format!("{}/existing.txt", session.workspace.root), "old").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "existing.txt", "content": "new" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("already exists"));
    assert_eq!(
        fs::read_to_string(format!("{}/existing.txt", session.workspace.root)).unwrap(),
        "old"
    );
    assert!(result.effects.is_empty());
}

#[test]
fn write_rejects_dangling_symlink_without_replacing_it() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = create_session(&host);
        let link = std::path::Path::new(&session.workspace.root).join("dangling");
        std::os::unix::fs::symlink("missing-target", &link).unwrap();

        let result = host
            .execute_invocation(invoke(
                &session.id,
                "Write",
                json!({ "path": "dangling", "content": "replacement" }),
            ))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::Error);
        assert!(result.error.unwrap().contains("already exists"));
        assert_eq!(
            fs::read_link(&link).unwrap(),
            std::path::PathBuf::from("missing-target")
        );
        assert!(result.effects.is_empty());
    }
}

#[test]
fn write_rejects_paths_outside_workspace() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "/tmp/escape.txt", "content": "bad" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::PolicyDenied);
    assert!(result.error.unwrap().contains("absolute host paths"));
}

#[test]
fn write_rejects_nul_path_as_tool_error_without_mutating() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "bad\0name.txt", "content": "bad" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("path contains invalid"));
    assert!(result.effects.is_empty());
    assert!(fs::read_dir(&session.workspace.root)
        .unwrap()
        .next()
        .is_none());
}

#[test]
fn empty_read_root_is_rejected_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();
    let mut limited_policy = policy();
    limited_policy.read_roots = vec!["".to_string()];

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("invalid_read_root".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: limited_policy,
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err.to_string().contains("policy.readRoots"));
    assert!(!state_dir.join("invalid_read_root").exists());
}

#[test]
fn empty_write_root_is_rejected_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();
    let mut limited_policy = policy();
    limited_policy.write_roots = vec!["".to_string()];

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("invalid_write_root".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: limited_policy,
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err.to_string().contains("policy.writeRoots"));
    assert!(!state_dir.join("invalid_write_root").exists());
}

#[test]
fn policy_root_traversal_is_rejected_without_workspace_side_effects() {
    let temp = TempDir::new().unwrap();
    let state_dir = temp.path().join("state");
    let host = HostState::new(&state_dir).unwrap();

    for (session_id, root) in [
        ("read_root_dot", "/workspace/."),
        ("read_root_parent", "/workspace/public/.."),
        ("read_root_outside", "/workspace/../outside"),
        ("read_root_typo", "/workspce"),
    ] {
        let mut limited_policy = policy();
        limited_policy.read_roots = vec![root.to_string()];

        let err = host
            .create_environment(CreateEnvironmentRequest {
                environment_id: Some(session_id.to_string()),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: limited_policy,
                ttl_ms: None,
                metadata: Map::new(),
            })
            .unwrap_err();

        assert!(
            err.to_string().contains("policy.readRoots"),
            "{root}: {err}"
        );
        assert!(!state_dir.join(session_id).exists(), "{root}");
    }
}

#[test]
fn write_rejects_missing_content_as_tool_error() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "missing.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("content"));
}

#[test]
fn write_preview_respects_session_output_limit_for_long_single_line_content() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy();
    limited_policy.max_output_bytes = Some(64);
    let session = create_session_with_policy(&host, limited_policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "huge.txt", "content": "x".repeat(10_000) }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("[truncated"));
    assert!(result.output.len() < 256);
    assert_eq!(
        fs::read_to_string(format!("{}/huge.txt", session.workspace.root)).unwrap(),
        "x".repeat(10_000)
    );
}

#[test]
fn write_preview_respects_zero_session_output_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy();
    limited_policy.max_output_bytes = Some(0);
    let session = create_session_with_policy(&host, limited_policy);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "silent.txt", "content": "created but no output" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "");
    assert_eq!(
        fs::read_to_string(format!("{}/silent.txt", session.workspace.root)).unwrap(),
        "created but no output"
    );
}

#[test]
fn read_returns_file_content_and_effect() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::write(
        format!("{}/test.txt", session.workspace.root),
        "Line 1\nLine 2",
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(&session.id, "Read", json!({ "path": "test.txt" })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "Line 1\nLine 2");
    assert_eq!(result.metadata["action"], "read");
    assert!(result.metadata.get("hostPath").is_none());
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, "file.read");
}

#[test]
fn existing_workspace_does_not_leak_absolute_host_paths_in_tool_metadata() {
    let temp = TempDir::new().unwrap();
    let workspace = temp.path().join("workspace");
    let state = temp.path().join("state");
    fs::create_dir_all(&workspace).unwrap();
    let host = HostState::new(&state).unwrap();
    let session = create_existing_session(&host, &workspace);
    fs::write(workspace.join("existing.txt"), "secret").unwrap();

    let read_result = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "existing.txt" }),
        ))
        .unwrap();
    let write_result = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "created.txt", "content": "new" }),
        ))
        .unwrap();

    assert_eq!(read_result.status, ToolResultStatus::Success);
    assert_eq!(write_result.status, ToolResultStatus::Success);
    assert!(read_result.metadata.get("hostPath").is_none());
    assert!(write_result.metadata.get("hostPath").is_none());
    assert!(!read_result
        .output
        .contains(&workspace.to_string_lossy().to_string()));
    assert!(!write_result
        .output
        .contains(&workspace.to_string_lossy().to_string()));
}

#[test]
fn read_respects_zero_session_output_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy();
    limited_policy.max_output_bytes = Some(0);
    let session = create_session_with_policy(&host, limited_policy);
    fs::write(format!("{}/large.txt", session.workspace.root), "secret").unwrap();

    let result = host
        .execute_invocation(invoke(&session.id, "Read", json!({ "path": "large.txt" })))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "");
    assert_eq!(result.effects.len(), 1);
}

#[test]
fn read_line_range_respects_zero_session_output_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy();
    limited_policy.max_output_bytes = Some(0);
    let session = create_session_with_policy(&host, limited_policy);
    fs::write(format!("{}/lines.txt", session.workspace.root), "a\nb\nc").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "lines.txt", "startLine": 2, "endLine": 2 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "");
    assert_eq!(result.effects.len(), 1);
}

#[test]
fn read_truncates_large_files() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::write(
        format!("{}/large.txt", session.workspace.root),
        "x".repeat(1000),
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "large.txt", "maxBytes": 100 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.contains("[truncated"));
    assert!(result.output.len() < 200);
}

#[test]
fn read_effect_skips_hashing_oversized_files() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    let path = format!("{}/huge.bin", session.workspace.root);
    fs::File::create(&path)
        .unwrap()
        .set_len(17 * 1024 * 1024)
        .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "huge.bin", "maxBytes": 16 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    let before = result.effects[0].before.as_ref().unwrap();
    assert_eq!(before.bytes, Some(17 * 1024 * 1024));
    assert_eq!(before.hash, None);
    assert_eq!(before.metadata["hashSkipped"], true);
}

#[test]
fn read_max_bytes_argument_cannot_exceed_session_output_limit() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let mut limited_policy = policy();
    limited_policy.max_output_bytes = Some(8);
    let session = create_session_with_policy(&host, limited_policy);
    fs::write(
        format!("{}/large.txt", session.workspace.root),
        "abcdefghijklmnopqrstuvwxyz",
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "large.txt", "maxBytes": 1000 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert!(result.output.starts_with("abcdefgh"));
    assert!(!result.output.contains("ijklmnop"));
    assert!(result.output.contains("[truncated"));
}

#[test]
fn read_rejects_invalid_numeric_options_instead_of_defaulting() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::write(
        format!("{}/large.txt", session.workspace.root),
        "x".repeat(1000),
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "large.txt", "maxBytes": "100" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("maxBytes must be"));
}

#[test]
fn read_rejects_excessive_max_bytes_without_reading() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::write(format!("{}/large.txt", session.workspace.root), "x").unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "large.txt", "maxBytes": executioner_core::MAX_OUTPUT_BYTES + 1 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result
        .error
        .unwrap()
        .contains("maximum supported output size"));
    assert!(host.effects("env").unwrap().is_empty());
}

#[test]
fn read_supports_line_ranges() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::write(
        format!("{}/lines.txt", session.workspace.root),
        "a\nb\nc\nd",
    )
    .unwrap();

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "lines.txt", "startLine": 2, "endLine": 3 }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Success);
    assert_eq!(result.output, "// Lines 2-3 of 4 total\nb\nc");
}

#[test]
fn read_rejects_zero_and_inverted_line_ranges() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::write(
        format!("{}/lines.txt", session.workspace.root),
        "a\nb\nc\nd",
    )
    .unwrap();

    let zero_start = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "lines.txt", "startLine": 0 }),
        ))
        .unwrap();
    let inverted = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "lines.txt", "startLine": 3, "endLine": 2 }),
        ))
        .unwrap();

    assert_eq!(zero_start.status, ToolResultStatus::Error);
    assert!(zero_start.error.unwrap().contains("startLine must be"));
    assert_eq!(inverted.status, ToolResultStatus::Error);
    assert!(inverted.error.unwrap().contains("endLine must be"));
}

#[test]
fn read_and_write_reject_unexpected_arguments() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::write(format!("{}/visible.txt", session.workspace.root), "visible").unwrap();

    let read = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "visible.txt", "maxByte": 1 }),
        ))
        .unwrap();
    let write = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "created.txt", "content": "created", "overwrite": true }),
        ))
        .unwrap();

    assert_eq!(read.status, ToolResultStatus::Error);
    assert!(read.error.unwrap().contains("unexpected argument"));
    assert_eq!(write.status, ToolResultStatus::Error);
    assert!(write.error.unwrap().contains("unexpected argument"));
    assert!(!std::path::Path::new(&format!("{}/created.txt", session.workspace.root)).exists());
}

#[test]
fn read_reports_missing_file_without_effects() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);

    let result = host
        .execute_invocation(invoke(
            &session.id,
            "Read",
            json!({ "path": "missing.txt" }),
        ))
        .unwrap();

    assert_eq!(result.status, ToolResultStatus::Error);
    assert!(result.error.unwrap().contains("File not found"));
    assert!(result.effects.is_empty());
}

#[test]
fn read_rejects_symlink_escape() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = create_session(&host);
        let outside = temp.path().join("outside.txt");
        fs::write(&outside, "secret").unwrap();
        std::os::unix::fs::symlink(&outside, format!("{}/link", session.workspace.root)).unwrap();

        let result = host
            .execute_invocation(invoke(&session.id, "Read", json!({ "path": "link" })))
            .unwrap();

        assert_eq!(result.status, ToolResultStatus::PolicyDenied);
        assert!(result.effects.is_empty());
    }
}

#[test]
fn closed_sessions_reject_invocations() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    host.close_session(&session.id).unwrap();

    let err = host
        .execute_invocation(invoke(
            &session.id,
            "Write",
            json!({ "path": "new.txt", "content": "nope" }),
        ))
        .unwrap_err();

    assert!(err.to_string().contains("not ready"));
}

#[test]
fn destroy_removes_managed_workspace() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    let root = session.workspace.root.clone();

    host.destroy_environment("env").unwrap();

    assert!(!std::path::Path::new(&root).exists());
}

#[test]
fn destroy_does_not_remove_existing_workspace() {
    let temp = TempDir::new().unwrap();
    let state = TempDir::new().unwrap();
    let host = HostState::new(state.path()).unwrap();
    fs::write(temp.path().join("kept.txt"), "kept").unwrap();
    let _session = create_existing_session(&host, temp.path());

    host.destroy_environment("env_existing").unwrap();

    assert!(temp.path().exists());
    assert_eq!(
        fs::read_to_string(temp.path().join("kept.txt")).unwrap(),
        "kept"
    );
}

#[test]
fn existing_workspace_rejects_relative_root() {
    let state = TempDir::new().unwrap();
    let host = HostState::new(state.path()).unwrap();

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("sess_relative_existing".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::Existing,
                root: Some("relative-workspace".to_string()),
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: policy(),
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err.to_string().contains("workspace.root must be absolute"));
    assert!(!state.path().join("sess_relative_existing").exists());
}

#[cfg(unix)]
#[test]
fn existing_workspace_rejects_symlink_root() {
    let temp = TempDir::new().unwrap();
    let state = TempDir::new().unwrap();
    let host = HostState::new(state.path()).unwrap();
    let real_workspace = temp.path().join("real");
    let linked_workspace = temp.path().join("linked");
    fs::create_dir_all(&real_workspace).unwrap();
    std::os::unix::fs::symlink(&real_workspace, &linked_workspace).unwrap();

    let err = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("sess_linked_existing".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::Existing,
                root: Some(linked_workspace.to_string_lossy().into_owned()),
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: policy(),
            ttl_ms: None,
            metadata: Map::new(),
        })
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("workspace.root must not be a symlink"));
    assert!(!state.path().join("sess_linked_existing").exists());
}

#[test]
fn ttl_expiry_removes_managed_workspace_and_rejects_late_access() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let environment = host
        .create_environment(CreateEnvironmentRequest {
            environment_id: Some("env_ttl".to_string()),
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: policy(),
            ttl_ms: Some(50),
            metadata: Map::new(),
        })
        .unwrap()
        .environment;
    let session = host
        .create_session(
            &environment.id,
            CreateSessionRequest {
                session_id: Some("sess_ttl".to_string()),
                policy: None,
                metadata: Map::new(),
            },
        )
        .unwrap()
        .session;
    let root = session.workspace.root.clone();

    std::thread::sleep(Duration::from_millis(75));

    let err = host.get_session(&session.id).unwrap_err();
    assert!(err.to_string().contains("session not found"));
    assert!(!std::path::Path::new(&root).exists());
}

#[test]
fn export_workspace_writes_tar_artifact_and_manifest() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::create_dir_all(format!("{}/src", session.workspace.root)).unwrap();
    fs::write(format!("{}/src/main.txt", session.workspace.root), "hello").unwrap();
    fs::write(format!("{}/README.md", session.workspace.root), "read me").unwrap();

    let artifact = host.export_workspace("env").unwrap();

    assert_eq!(artifact.environment_id, "env");
    assert_eq!(artifact.format, "tar");
    assert_eq!(artifact.file_count, 2);
    assert_eq!(artifact.directory_count, 1);
    assert_eq!(artifact.symlink_count, 0);
    assert_eq!(artifact.artifact.resource_type, "artifact");
    assert!(artifact.hash.starts_with("sha256:"));
    assert!(artifact.bytes > 0);
    assert!(artifact
        .entries
        .iter()
        .any(|entry| entry.logical_path == "/workspace/src/main.txt"
            && entry.archive_path == "src/main.txt"
            && entry.kind == "file"));

    let tar_path = artifact
        .artifact
        .uri
        .strip_prefix("file://")
        .expect("file uri");
    let manifest_path = artifact
        .manifest
        .uri
        .strip_prefix("file://")
        .expect("file uri");
    let manifest: executioner_core::WorkspaceArtifact =
        serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
    assert_eq!(manifest, artifact);

    let tar_file = fs::File::open(tar_path).unwrap();
    let mut archive = tar::Archive::new(tar_file);
    let mut files = Vec::<(String, String)>::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        if entry.header().entry_type().is_file() {
            let path = entry.path().unwrap().to_string_lossy().into_owned();
            let mut content = String::new();
            entry.read_to_string(&mut content).unwrap();
            files.push((path, content));
        }
    }
    files.sort();
    assert_eq!(
        files,
        vec![
            ("README.md".to_string(), "read me".to_string()),
            ("src/main.txt".to_string(), "hello".to_string()),
        ]
    );
}

#[test]
fn direct_export_workspace_excludes_output_directory_inside_workspace() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    let output_dir = std::path::Path::new(&session.workspace.root).join(".artifacts");
    fs::create_dir_all(&output_dir).unwrap();
    fs::write(output_dir.join("previous.tar"), "old artifact").unwrap();
    fs::write(format!("{}/app.txt", session.workspace.root), "app").unwrap();

    let artifact = executioner_core::artifact::export_workspace(&session, &output_dir).unwrap();

    assert!(artifact
        .entries
        .iter()
        .any(|entry| entry.archive_path == "app.txt"));
    assert!(!artifact
        .entries
        .iter()
        .any(|entry| entry.archive_path.starts_with(".artifacts")));
    let tar_path = artifact.artifact.uri.strip_prefix("file://").unwrap();
    let tar_file = fs::File::open(tar_path).unwrap();
    let mut archive = tar::Archive::new(tar_file);
    for entry in archive.entries().unwrap() {
        let entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        assert!(!path.starts_with(".artifacts"));
    }
}

#[test]
fn direct_export_workspace_rejects_workspace_root_as_output_directory() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    fs::write(format!("{}/app.txt", session.workspace.root), "app").unwrap();

    let err = executioner_core::artifact::export_workspace(
        &session,
        std::path::Path::new(&session.workspace.root),
    )
    .unwrap_err();

    assert!(err.to_string().contains("workspace root"));
    assert_eq!(fs::read_dir(&session.workspace.root).unwrap().count(), 1);
}

#[test]
fn export_workspace_rejects_excessive_entry_count_without_artifact_files() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path().join("state")).unwrap();
    let session = create_session(&host);
    for index in 0..10_001 {
        fs::write(format!("{}/file-{index}.txt", session.workspace.root), "").unwrap();
    }
    let output_dir = temp.path().join("artifacts");

    let err = executioner_core::artifact::export_workspace(&session, &output_dir).unwrap_err();

    assert!(err.to_string().contains("maximum entry count"));
    assert!(fs::read_dir(output_dir).unwrap().next().is_none());
}

#[test]
fn export_workspace_rejects_excessive_directory_depth_without_artifact_files() {
    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path().join("state")).unwrap();
    let session = create_session(&host);
    let mut current = std::path::PathBuf::from(&session.workspace.root);
    for _ in 0..258 {
        current = current.join("d");
        fs::create_dir(&current).unwrap();
    }
    let output_dir = temp.path().join("artifacts");

    let err = executioner_core::artifact::export_workspace(&session, &output_dir).unwrap_err();

    assert!(err.to_string().contains("maximum directory depth"));
    assert!(fs::read_dir(output_dir).unwrap().next().is_none());
}

#[test]
fn export_workspace_rejects_oversized_file_before_reading_it() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let session = create_session(&host);
        let oversized_path = std::path::Path::new(&session.workspace.root).join("huge.bin");
        let file = fs::File::create(&oversized_path).unwrap();
        file.set_len(100 * 1024 * 1024 + 1).unwrap();
        drop(file);
        fs::set_permissions(&oversized_path, fs::Permissions::from_mode(0o000)).unwrap();
        let output_dir = temp.path().join("artifacts");

        let err = executioner_core::artifact::export_workspace(&session, &output_dir).unwrap_err();

        fs::set_permissions(&oversized_path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(err.to_string().contains("maximum size"));
        assert!(fs::read_dir(output_dir).unwrap().next().is_none());
    }
}

#[test]
fn export_workspace_rejects_file_that_would_exceed_tar_cap_before_reading_it() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let session = create_session(&host);
        let capped_path = std::path::Path::new(&session.workspace.root).join("cap.bin");
        let file = fs::File::create(&capped_path).unwrap();
        file.set_len(100 * 1024 * 1024).unwrap();
        drop(file);
        fs::set_permissions(&capped_path, fs::Permissions::from_mode(0o000)).unwrap();
        let output_dir = temp.path().join("artifacts");

        let err = executioner_core::artifact::export_workspace(&session, &output_dir).unwrap_err();

        fs::set_permissions(&capped_path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(err.to_string().contains("maximum size"));
        assert!(fs::read_dir(output_dir).unwrap().next().is_none());
    }
}

#[test]
fn export_workspace_accounts_for_long_archive_path_headers_before_reading() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let session = create_session(&host);
        let long_name = "a".repeat(120);
        let capped_path = std::path::Path::new(&session.workspace.root).join(long_name);
        let file = fs::File::create(&capped_path).unwrap();
        file.set_len(100 * 1024 * 1024 - 1536).unwrap();
        drop(file);
        fs::set_permissions(&capped_path, fs::Permissions::from_mode(0o000)).unwrap();
        let output_dir = temp.path().join("artifacts");

        let err = executioner_core::artifact::export_workspace(&session, &output_dir).unwrap_err();

        fs::set_permissions(&capped_path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(err.to_string().contains("maximum size"));
        assert!(fs::read_dir(output_dir).unwrap().next().is_none());
    }
}

#[test]
fn direct_export_workspace_rejects_symlink_output_directory() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = create_session(&host);
        fs::write(format!("{}/app.txt", session.workspace.root), "app").unwrap();
        let outside = temp.path().join("outside-artifacts");
        let output_link = temp.path().join("artifact-link");
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, &output_link).unwrap();

        let err = executioner_core::artifact::export_workspace(&session, &output_link).unwrap_err();

        assert!(err.to_string().contains("must not be a symlink"));
        assert!(fs::read_dir(&outside).unwrap().next().is_none());
    }
}

#[test]
fn direct_export_workspace_rejects_symlink_output_parent_directory() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let session = create_session(&host);
        fs::write(format!("{}/app.txt", session.workspace.root), "app").unwrap();
        let outside = temp.path().join("outside-artifacts-parent");
        let linked_parent = temp.path().join("linked-parent");
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, &linked_parent).unwrap();

        let err = executioner_core::artifact::export_workspace(
            &session,
            &linked_parent.join("artifacts"),
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("output directory parent must not contain symlinks"));
        assert!(fs::read_dir(&outside).unwrap().next().is_none());
    }
}

#[test]
fn export_workspace_excludes_host_artifact_directory_inside_existing_workspace() {
    let temp = TempDir::new().unwrap();
    let workspace = temp.path().join("workspace");
    let state_dir = workspace.join(".substrate");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(workspace.join("app.txt"), "app").unwrap();
    fs::write(state_dir.join("private-state.json"), "internal").unwrap();
    let host = HostState::new(&state_dir).unwrap();
    let _session = create_existing_session(&host, &workspace);

    let artifact = host.export_workspace("env_existing").unwrap();

    assert!(artifact
        .entries
        .iter()
        .any(|entry| entry.archive_path == "app.txt"));
    assert!(!artifact
        .entries
        .iter()
        .any(|entry| entry.archive_path.starts_with(".substrate/")));

    let tar_path = artifact.artifact.uri.strip_prefix("file://").unwrap();
    let tar_file = fs::File::open(tar_path).unwrap();
    let mut archive = tar::Archive::new(tar_file);
    for entry in archive.entries().unwrap() {
        let entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        assert!(!path.starts_with(".substrate/"));
    }
}

#[test]
fn export_workspace_rejects_preexisting_state_session_symlink_for_existing_workspace() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path().join("workspace");
        let state_dir = temp.path().join("state");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(workspace.join("app.txt"), "app").unwrap();
        std::os::unix::fs::symlink(&outside, state_dir.join("env_existing")).unwrap();
        let host = HostState::new(&state_dir).unwrap();
        let _session = create_existing_session(&host, &workspace);

        let err = host.export_workspace("env_existing").unwrap_err();

        assert!(err.to_string().contains("host state environment directory"));
        assert!(!outside.join("artifacts").exists());
    }
}

#[test]
fn export_workspace_omits_unsafe_symlinks_without_following_them() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = create_session(&host);
        let outside = temp.path().join("outside.txt");
        fs::write(&outside, "secret").unwrap();
        std::os::unix::fs::symlink(&outside, format!("{}/link", session.workspace.root)).unwrap();

        let artifact = host.export_workspace("env").unwrap();

        assert_eq!(artifact.file_count, 0);
        assert_eq!(artifact.symlink_count, 0);
        assert!(!artifact
            .entries
            .iter()
            .any(|entry| entry.logical_path == "/workspace/link"));

        let tar_path = artifact.artifact.uri.strip_prefix("file://").unwrap();
        let tar_file = fs::File::open(tar_path).unwrap();
        let mut archive = tar::Archive::new(tar_file);
        assert_eq!(archive.entries().unwrap().count(), 0);
    }
}

#[test]
fn export_workspace_records_relative_symlink_target_without_archiving_contents() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = create_session(&host);
        fs::write(format!("{}/target.txt", session.workspace.root), "target").unwrap();
        std::os::unix::fs::symlink(
            "target.txt",
            format!("{}/relative-link", session.workspace.root),
        )
        .unwrap();

        let artifact = host.export_workspace("env").unwrap();
        let manifest_path = artifact.manifest.uri.strip_prefix("file://").unwrap();
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
        let entries = manifest["entries"].as_array().unwrap();
        let link = entries
            .iter()
            .find(|entry| entry["archivePath"] == "relative-link")
            .unwrap();

        assert_eq!(link["kind"], "symlink");
        assert_eq!(link["linkTarget"], "target.txt");

        let tar_path = artifact.artifact.uri.strip_prefix("file://").unwrap();
        let tar_file = fs::File::open(tar_path).unwrap();
        let mut archive = tar::Archive::new(tar_file);
        for entry in archive.entries().unwrap() {
            let entry = entry.unwrap();
            assert_ne!(
                entry.path().unwrap().to_string_lossy(),
                "relative-link",
                "symlink content must not be archived"
            );
        }
    }
}

#[test]
fn export_workspace_drops_backslash_symlink_target_instead_of_rewriting_it() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = create_session(&host);
        std::os::unix::fs::symlink(
            "dir\\target.txt",
            format!("{}/backslash-link", session.workspace.root),
        )
        .unwrap();

        let artifact = host.export_workspace("env").unwrap();

        assert_eq!(artifact.symlink_count, 0);
        assert!(!artifact
            .entries
            .iter()
            .any(|entry| entry.archive_path == "backslash-link"));
    }
}

#[test]
fn export_workspace_rejects_backslash_file_paths_instead_of_rewriting_them() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = create_session(&host);
        fs::write(
            format!("{}/dir\\target.txt", session.workspace.root),
            "target",
        )
        .unwrap();

        let err = host.export_workspace("env").unwrap_err();

        assert!(err.to_string().contains("unsupported workspace path"));
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn export_workspace_rejects_non_utf8_file_paths_instead_of_rewriting_them() {
    use std::os::unix::ffi::OsStrExt;

    let temp = TempDir::new().unwrap();
    let host = HostState::new(temp.path()).unwrap();
    let session = create_session(&host);
    let mut path = std::path::PathBuf::from(&session.workspace.root);
    path.push(std::ffi::OsStr::from_bytes(b"bad-\xff.txt"));
    fs::write(path, "secret").unwrap();

    let err = host.export_workspace("env").unwrap_err();

    assert!(err.to_string().contains("not valid UTF-8"));
}

#[test]
fn export_workspace_drops_relative_symlink_target_that_escapes_workspace() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path()).unwrap();
        let session = create_session(&host);
        fs::write(temp.path().join("outside.txt"), "outside").unwrap();
        fs::create_dir_all(format!("{}/dir", session.workspace.root)).unwrap();
        std::os::unix::fs::symlink(
            "../../outside.txt",
            format!("{}/dir/escaping-link", session.workspace.root),
        )
        .unwrap();

        let artifact = host.export_workspace("env").unwrap();

        assert_eq!(artifact.symlink_count, 0);
        assert!(!artifact
            .entries
            .iter()
            .any(|entry| entry.archive_path == "dir/escaping-link"));
    }
}

#[test]
fn materialize_workspace_artifact_round_trips_export_with_unsafe_symlink() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let session = create_session(&host);
        fs::write(format!("{}/kept.txt", session.workspace.root), "kept").unwrap();
        std::os::unix::fs::symlink(
            "/etc/passwd",
            format!("{}/unsafe-link", session.workspace.root),
        )
        .unwrap();
        let artifact = host.export_workspace("env").unwrap();
        let destination = temp.path().join("restored-unsafe-symlink");

        executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
            .unwrap();

        assert_eq!(
            fs::read_to_string(destination.join("kept.txt")).unwrap(),
            "kept"
        );
        assert!(!destination.join("unsafe-link").exists());
    }
}

#[test]
fn materialize_workspace_artifact_restores_files_and_safe_symlinks() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let session = create_session(&host);
        fs::create_dir_all(format!("{}/src", session.workspace.root)).unwrap();
        fs::write(format!("{}/src/main.txt", session.workspace.root), "hello").unwrap();
        std::os::unix::fs::symlink(
            "src/main.txt",
            format!("{}/main-link", session.workspace.root),
        )
        .unwrap();
        let artifact = host.export_workspace("env").unwrap();
        let destination = temp.path().join("restored");

        executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
            .unwrap();

        assert_eq!(
            fs::read_to_string(destination.join("src/main.txt")).unwrap(),
            "hello"
        );
        assert_eq!(
            fs::read_link(destination.join("main-link")).unwrap(),
            std::path::PathBuf::from("src/main.txt")
        );
    }
}

#[test]
fn materialize_workspace_artifact_rejects_symlink_artifact_resource() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let tar_path = temp.path().join("workspace.tar");
        let link_path = temp.path().join("workspace-link.tar");
        write_tar_with_file(&tar_path, "file.txt", "payload");
        let (hash, bytes) = test_hash_file(&tar_path);
        std::os::unix::fs::symlink(&tar_path, &link_path).unwrap();
        let destination = temp.path().join("restore");
        let artifact = WorkspaceArtifact {
            environment_id: "sess".to_string(),
            artifact: ResourceRef {
                resource_type: "artifact".to_string(),
                uri: format!("file://{}", link_path.to_string_lossy()),
            },
            manifest: ResourceRef {
                resource_type: "artifact_manifest".to_string(),
                uri: "file:///unused".to_string(),
            },
            format: "tar".to_string(),
            bytes,
            hash,
            file_count: 1,
            directory_count: 0,
            symlink_count: 0,
            entries: vec![WorkspaceArtifactEntry {
                logical_path: "/workspace/file.txt".to_string(),
                archive_path: "file.txt".to_string(),
                kind: "file".to_string(),
                link_target: None,
                bytes: Some("payload".len() as u64),
                hash: Some(test_hash_bytes(b"payload")),
            }],
            created_at: "now".to_string(),
        };

        let err =
            executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
                .unwrap_err();

        assert!(err.to_string().contains("must be a regular file"));
        assert!(!destination.exists());
    }
}

#[test]
fn materialize_workspace_artifact_rejects_symlink_manifest_resource() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let tar_path = temp.path().join("workspace.tar");
        let manifest_path = temp.path().join("workspace.manifest.json");
        let manifest_link = temp.path().join("workspace.manifest-link.json");
        write_tar_with_file(&tar_path, "file.txt", "payload");
        let (hash, bytes) = test_hash_file(&tar_path);
        let destination = temp.path().join("restore");
        let artifact = WorkspaceArtifact {
            environment_id: "sess".to_string(),
            artifact: ResourceRef {
                resource_type: "artifact".to_string(),
                uri: format!("file://{}", tar_path.to_string_lossy()),
            },
            manifest: ResourceRef {
                resource_type: "artifact_manifest".to_string(),
                uri: format!("file://{}", manifest_link.to_string_lossy()),
            },
            format: "tar".to_string(),
            bytes,
            hash,
            file_count: 1,
            directory_count: 0,
            symlink_count: 0,
            entries: vec![WorkspaceArtifactEntry {
                logical_path: "/workspace/file.txt".to_string(),
                archive_path: "file.txt".to_string(),
                kind: "file".to_string(),
                link_target: None,
                bytes: Some("payload".len() as u64),
                hash: Some(test_hash_bytes(b"payload")),
            }],
            created_at: "now".to_string(),
        };
        fs::write(&manifest_path, serde_json::to_vec(&artifact).unwrap()).unwrap();
        std::os::unix::fs::symlink(&manifest_path, &manifest_link).unwrap();

        let err =
            executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
                .unwrap_err();

        assert!(err.to_string().contains("must be a regular file"));
        assert!(!destination.exists());
    }
}

#[test]
fn materialize_workspace_artifact_rejects_symlinked_destination_parent() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let session = create_session(&host);
        fs::write(format!("{}/file.txt", session.workspace.root), "payload").unwrap();
        let artifact = host.export_workspace("env").unwrap();
        let outside = temp.path().join("outside");
        let link_parent = temp.path().join("link-parent");
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, &link_parent).unwrap();
        let destination = link_parent.join("restored");

        let err =
            executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
                .unwrap_err();

        assert!(err.to_string().contains("parent must not contain symlinks"));
        assert!(!outside.join("restored").exists());
    }
}

#[test]
fn materialize_workspace_artifact_rejects_symlinked_destination_ancestor() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let host = HostState::new(temp.path().join("state")).unwrap();
        let session = create_session(&host);
        fs::write(format!("{}/file.txt", session.workspace.root), "payload").unwrap();
        let artifact = host.export_workspace("env").unwrap();
        let outside = temp.path().join("outside");
        let link_parent = temp.path().join("link-parent");
        fs::create_dir_all(outside.join("existing")).unwrap();
        std::os::unix::fs::symlink(&outside, &link_parent).unwrap();
        let destination = link_parent.join("existing").join("restored");

        let err =
            executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
                .unwrap_err();

        assert!(err.to_string().contains("parent must not contain symlinks"));
        assert!(!outside.join("existing").join("restored").exists());
    }
}

#[test]
fn materialize_workspace_artifact_rejects_manifest_path_traversal() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("malicious.tar");
    write_tar_with_file(&tar_path, "escape.txt", "payload");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes,
        hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/../escape.txt".to_string(),
            archive_path: "../escape.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(
                "sha256:239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
                    .to_string(),
            ),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("unsafe artifact path"));
    assert!(!temp.path().join("escape.txt").exists());
}

#[test]
fn materialize_workspace_artifact_does_not_leave_partial_files_on_failure() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("partial.tar");
    write_tar_with_file(&tar_path, "first.txt", "first");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes,
        hash,
        file_count: 2,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![
            WorkspaceArtifactEntry {
                logical_path: "/workspace/first.txt".to_string(),
                archive_path: "first.txt".to_string(),
                kind: "file".to_string(),
                link_target: None,
                bytes: Some("first".len() as u64),
                hash: Some(
                    "sha256:a7937b64b8caa58f03721bb6bacf5c78cb235febe0e70b1b84cd99541461a08e"
                        .to_string(),
                ),
            },
            WorkspaceArtifactEntry {
                logical_path: "/workspace/missing.txt".to_string(),
                archive_path: "missing.txt".to_string(),
                kind: "file".to_string(),
                link_target: None,
                bytes: Some("missing".len() as u64),
                hash: Some(
                    "sha256:ffa63583dfa6706b87d284b86b0d693a161e4840aad2c5cf6b5d27c3b9621f7d"
                        .to_string(),
                ),
            },
        ],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("manifest file missing"));
    assert!(!destination.join("first.txt").exists());
}

#[test]
fn materialize_workspace_artifact_rejects_manifest_file_with_missing_parent_directory() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("implicit-parent.tar");
    write_tar_with_file(&tar_path, "dir/file.txt", "payload");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes,
        hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/dir/file.txt".to_string(),
            archive_path: "dir/file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(test_hash_bytes(b"payload")),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("manifest parent directory missing"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_inconsistent_manifest_counts() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("counts.tar");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes,
        hash,
        file_count: 2,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(
                "sha256:239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
                    .to_string(),
            ),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("manifest counts"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_wrong_format_before_extracting() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("wrong-format.tar");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "zip".to_string(),
        bytes,
        hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(
                "sha256:239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
                    .to_string(),
            ),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("unsupported workspace artifact format"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_invalid_artifact_without_leaving_created_parents() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("wrong-format-nested.tar");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination_parent = temp.path().join("new-parent").join("nested");
    let destination = destination_parent.join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "zip".to_string(),
        bytes,
        hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(test_hash_bytes(b"payload")),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("unsupported workspace artifact format"));
    assert!(!destination.exists());
    assert!(!destination_parent.exists());
    assert!(!temp.path().join("new-parent").exists());
}

#[test]
fn materialize_workspace_artifact_rejects_logical_archive_path_mismatch() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("mismatch.tar");
    write_tar_with_file(&tar_path, "actual.txt", "payload");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes,
        hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/reported.txt".to_string(),
            archive_path: "actual.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(
                "sha256:239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
                    .to_string(),
            ),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("logical path"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_backslash_archive_paths() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("backslash.tar");
    write_tar_with_file(&tar_path, "dir\\file.txt", "payload");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes,
        hash,
        file_count: 1,
        directory_count: 1,
        symlink_count: 0,
        entries: vec![
            WorkspaceArtifactEntry {
                logical_path: "/workspace/dir".to_string(),
                archive_path: "dir".to_string(),
                kind: "directory".to_string(),
                link_target: None,
                bytes: None,
                hash: None,
            },
            WorkspaceArtifactEntry {
                logical_path: "/workspace/dir/file.txt".to_string(),
                archive_path: "dir/file.txt".to_string(),
                kind: "file".to_string(),
                link_target: None,
                bytes: Some("payload".len() as u64),
                hash: Some(
                    "sha256:239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
                        .to_string(),
                ),
            },
        ],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("unsafe artifact path"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_stale_manifest_resource() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    let manifest_path = temp.path().join("workspace.manifest.json");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: format!("file://{}", manifest_path.to_string_lossy()),
        },
        format: "tar".to_string(),
        bytes,
        hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(
                "sha256:239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
                    .to_string(),
            ),
        }],
        created_at: "now".to_string(),
    };
    let stale = {
        let mut stale = artifact.clone();
        stale.entries[0].logical_path = "/workspace/stale.txt".to_string();
        stale
    };
    fs::write(&manifest_path, serde_json::to_vec_pretty(&stale).unwrap()).unwrap();

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("manifest resource"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_oversized_manifest_resource() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    let manifest_path = temp.path().join("workspace.manifest.json");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let (hash, bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: format!("file://{}", manifest_path.to_string_lossy()),
        },
        format: "tar".to_string(),
        bytes,
        hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(test_hash_bytes(b"payload")),
        }],
        created_at: "now".to_string(),
    };
    let mut manifest = serde_json::to_value(&artifact).unwrap();
    manifest.as_object_mut().unwrap().insert(
        "padding".to_string(),
        Value::String("x".repeat(11 * 1024 * 1024)),
    );
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("manifest resource exceeds"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_oversized_declared_artifact_before_reading() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let (_hash, _bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: 100 * 1024 * 1024 + 1,
        hash: "sha256:declared-too-large".to_string(),
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(
                "sha256:239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
                    .to_string(),
            ),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("maximum size"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_oversized_manifest_file_entry_before_extracting() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_empty_tar(&tar_path);
    let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes,
        hash: tar_hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/huge.bin".to_string(),
            archive_path: "huge.bin".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some(100 * 1024 * 1024 + 1),
            hash: Some(test_hash_bytes(b"")),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("maximum size"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_excessive_manifest_path_depth() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_empty_tar(&tar_path);
    let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
    let archive_path = std::iter::repeat_n("d", 257).collect::<Vec<_>>().join("/");
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes,
        hash: tar_hash,
        file_count: 0,
        directory_count: 0,
        symlink_count: 1,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: format!("/workspace/{archive_path}"),
            archive_path,
            kind: "symlink".to_string(),
            link_target: Some("target.txt".to_string()),
            bytes: None,
            hash: None,
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("maximum path depth"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_oversized_actual_artifact_before_hashing() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    fs::File::create(&tar_path)
        .unwrap()
        .set_len(100 * 1024 * 1024 + 1)
        .unwrap();
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: 0,
        hash: test_hash_bytes(b""),
        file_count: 0,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("maximum size"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_relative_file_manifest_uri() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file://relative.manifest.json".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes,
        hash: tar_hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(test_hash_bytes(b"payload")),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("artifact file uri must be absolute"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_unsupported_manifest_uri_scheme() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "https://example.invalid/workspace.manifest.json".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes,
        hash: tar_hash,
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(test_hash_bytes(b"payload")),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("manifest uri must be file://"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_symlink_entry_without_target() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_empty_tar(&tar_path);
    let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes,
        hash: tar_hash,
        file_count: 0,
        directory_count: 0,
        symlink_count: 1,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/missing-link".to_string(),
            archive_path: "missing-link".to_string(),
            kind: "symlink".to_string(),
            link_target: None,
            bytes: None,
            hash: None,
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("manifest symlink entry is incomplete"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_nul_manifest_archive_path() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_empty_tar(&tar_path);
    let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes,
        hash: tar_hash,
        file_count: 0,
        directory_count: 0,
        symlink_count: 1,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/bad\0link".to_string(),
            archive_path: "bad\0link".to_string(),
            kind: "symlink".to_string(),
            link_target: Some("target.txt".to_string()),
            bytes: None,
            hash: None,
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("unsafe artifact path"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_non_utf8_archive_paths_instead_of_rewriting_them() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let tar_path = temp.path().join("workspace.tar");
        write_tar_with_raw_path(&tar_path, b"bad-\xff.txt", b"payload");
        let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
        let destination = temp.path().join("restore");
        let artifact = WorkspaceArtifact {
            environment_id: "sess_test".to_string(),
            artifact: ResourceRef {
                resource_type: "artifact".to_string(),
                uri: format!("file://{}", tar_path.to_string_lossy()),
            },
            manifest: ResourceRef {
                resource_type: "artifact_manifest".to_string(),
                uri: "file:///unused".to_string(),
            },
            format: "tar".to_string(),
            bytes: tar_bytes,
            hash: tar_hash,
            file_count: 1,
            directory_count: 0,
            symlink_count: 0,
            entries: vec![WorkspaceArtifactEntry {
                logical_path: "/workspace/bad-\u{fffd}.txt".to_string(),
                archive_path: "bad-\u{fffd}.txt".to_string(),
                kind: "file".to_string(),
                link_target: None,
                bytes: Some("payload".len() as u64),
                hash: Some(test_hash_bytes(b"payload")),
            }],
            created_at: "now".to_string(),
        };

        let err =
            executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
                .unwrap_err();

        assert!(err.to_string().contains("not valid UTF-8"));
        assert!(!destination.exists());
    }
}

#[test]
fn materialize_workspace_artifact_rejects_tar_missing_end_of_archive_marker() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let mut tar_bytes = fs::read(&tar_path).unwrap();
    let new_len = tar_bytes.len().saturating_sub(1024);
    tar_bytes.truncate(new_len);
    fs::write(&tar_path, &tar_bytes).unwrap();
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes.len() as u64,
        hash: test_hash_bytes(&tar_bytes),
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(test_hash_bytes(b"payload")),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("end-of-archive"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_tar_trailing_data_after_end_of_archive() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_tar_with_file(&tar_path, "file.txt", "payload");
    let mut tar_bytes = fs::read(&tar_path).unwrap();
    let mut trailing_block = vec![0_u8; 512];
    trailing_block[..b"trailing-data".len()].copy_from_slice(b"trailing-data");
    tar_bytes.extend_from_slice(&trailing_block);
    fs::write(&tar_path, &tar_bytes).unwrap();
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes.len() as u64,
        hash: test_hash_bytes(&tar_bytes),
        file_count: 1,
        directory_count: 0,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/file.txt".to_string(),
            archive_path: "file.txt".to_string(),
            kind: "file".to_string(),
            link_target: None,
            bytes: Some("payload".len() as u64),
            hash: Some(test_hash_bytes(b"payload")),
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("trailing data"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_nul_manifest_symlink_target() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_empty_tar(&tar_path);
    let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes,
        hash: tar_hash,
        file_count: 0,
        directory_count: 0,
        symlink_count: 1,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/link".to_string(),
            archive_path: "link".to_string(),
            kind: "symlink".to_string(),
            link_target: Some("target\0.txt".to_string()),
            bytes: None,
            hash: None,
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("unsafe symlink target"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_backslash_manifest_symlink_target() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_empty_tar(&tar_path);
    let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes,
        hash: tar_hash,
        file_count: 0,
        directory_count: 0,
        symlink_count: 1,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/link".to_string(),
            archive_path: "link".to_string(),
            kind: "symlink".to_string(),
            link_target: Some("dir\\target.txt".to_string()),
            bytes: None,
            hash: None,
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err.to_string().contains("unsafe symlink target"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_manifest_directory_missing_from_artifact() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("workspace.tar");
    write_empty_tar(&tar_path);
    let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
    let destination = temp.path().join("restore");
    let artifact = WorkspaceArtifact {
        environment_id: "sess_test".to_string(),
        artifact: ResourceRef {
            resource_type: "artifact".to_string(),
            uri: format!("file://{}", tar_path.to_string_lossy()),
        },
        manifest: ResourceRef {
            resource_type: "artifact_manifest".to_string(),
            uri: "file:///unused".to_string(),
        },
        format: "tar".to_string(),
        bytes: tar_bytes,
        hash: tar_hash,
        file_count: 0,
        directory_count: 1,
        symlink_count: 0,
        entries: vec![WorkspaceArtifactEntry {
            logical_path: "/workspace/empty-dir".to_string(),
            archive_path: "empty-dir".to_string(),
            kind: "directory".to_string(),
            link_target: None,
            bytes: None,
            hash: None,
        }],
        created_at: "now".to_string(),
    };

    let err = executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("manifest directory missing from artifact"));
    assert!(!destination.exists());
}

#[test]
fn materialize_workspace_artifact_rejects_excessive_manifest_entries() {
    #[cfg(unix)]
    {
        let temp = TempDir::new().unwrap();
        let tar_path = temp.path().join("workspace.tar");
        write_empty_tar(&tar_path);
        let (tar_hash, tar_bytes) = test_hash_file(&tar_path);
        let entries = (0..10_001)
            .map(|index| WorkspaceArtifactEntry {
                logical_path: format!("/workspace/link-{index}.txt"),
                archive_path: format!("link-{index}.txt"),
                kind: "symlink".to_string(),
                link_target: Some("target.txt".to_string()),
                bytes: None,
                hash: None,
            })
            .collect::<Vec<_>>();
        let artifact = WorkspaceArtifact {
            environment_id: "sess_test".to_string(),
            artifact: ResourceRef {
                resource_type: "artifact".to_string(),
                uri: format!("file://{}", tar_path.to_string_lossy()),
            },
            manifest: ResourceRef {
                resource_type: "artifact_manifest".to_string(),
                uri: "file:///unused".to_string(),
            },
            format: "tar".to_string(),
            bytes: tar_bytes,
            hash: tar_hash,
            file_count: 0,
            directory_count: 0,
            symlink_count: entries.len(),
            entries,
            created_at: "now".to_string(),
        };
        let destination = temp.path().join("restored");

        let err =
            executioner_core::artifact::materialize_workspace_artifact(&artifact, &destination)
                .unwrap_err();

        assert!(err.to_string().contains("maximum entry count"));
        assert!(!destination.exists());
    }
}

fn write_empty_tar(path: &std::path::Path) {
    let file = fs::File::create(path).unwrap();
    let mut builder = tar::Builder::new(file);
    builder.finish().unwrap();
}

fn write_tar_with_file(path: &std::path::Path, archive_path: &str, content: &str) {
    let file = fs::File::create(path).unwrap();
    let mut builder = tar::Builder::new(file);
    let mut header = tar::Header::new_gnu();
    header.set_size(content.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append_data(&mut header, archive_path, content.as_bytes())
        .unwrap();
    builder.finish().unwrap();
}

#[cfg(unix)]
fn write_tar_with_raw_path(path: &std::path::Path, archive_path: &[u8], content: &[u8]) {
    use std::os::unix::ffi::OsStrExt;

    let file = fs::File::create(path).unwrap();
    let mut builder = tar::Builder::new(file);
    let mut header = tar::Header::new_gnu();
    header.set_size(content.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    let archive_path = std::path::Path::new(std::ffi::OsStr::from_bytes(archive_path));
    builder
        .append_data(&mut header, archive_path, content)
        .unwrap();
    builder.finish().unwrap();
}

fn test_hash_file(path: &std::path::Path) -> (String, u64) {
    let bytes = fs::read(path).unwrap();
    (test_hash_bytes(&bytes), bytes.len() as u64)
}

fn test_hash_bytes(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    format!("sha256:{hash:x}")
}
