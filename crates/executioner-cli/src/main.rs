use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use executioner_core::{CreateSessionRequest, ToolInvocationRequest};
use executioner_host::serve;
use executioner_sdk::{Environment, ToolCall};
use executioner_worker::{FileBroker, HttpHostClient, Worker};
use reqwest::Url;
use serde::Serialize;
use serde_json::{Map, Value};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

const MAX_HTTP_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_HTTP_JSON_BODY_BYTES: usize = 10 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "substrate-runtime")]
#[command(about = "Standalone agent tool execution substrate")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Host {
        #[arg(long, default_value = "127.0.0.1:8765")]
        addr: SocketAddr,
        #[arg(long, default_value = "/tmp/executioner")]
        state_dir: PathBuf,
    },
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },
    Invoke {
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        tool: String,
        #[arg(long)]
        args_json: String,
        #[arg(long)]
        cwd: Option<String>,
    },
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
    },
    Env {
        #[command(subcommand)]
        command: EnvCommand,
    },
}

#[derive(Debug, Subcommand)]
enum SessionCommand {
    List {
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
        #[arg(long)]
        environment_id: Option<String>,
    },
    Get {
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
        #[arg(long)]
        session_id: String,
    },
    Create {
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
        #[arg(long)]
        environment_id: String,
    },
    Export {
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
        #[arg(long)]
        environment_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum WorkerCommand {
    Run {
        #[arg(long, default_value = "worker")]
        id: String,
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
        #[arg(long)]
        queue_dir: PathBuf,
        #[arg(long, default_value_t = 250)]
        idle_sleep_ms: u64,
    },
    RunOnce {
        #[arg(long, default_value = "worker")]
        id: String,
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
        #[arg(long)]
        queue_dir: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum EnvCommand {
    List {
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
    },
    Get {
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
        #[arg(long)]
        environment_id: String,
    },
    Effects {
        #[arg(long, default_value = "http://127.0.0.1:8765/")]
        host_url: String,
        #[arg(long)]
        environment_id: String,
    },
    Smoke {
        #[arg(long, default_value = "/tmp/executioner-env-queue")]
        queue_dir: PathBuf,
        #[arg(long, default_value = "/tmp/executioner-env-state")]
        state_dir: PathBuf,
        #[arg(long)]
        workspace: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Host { addr, state_dir } => {
            let state = executioner_core::HostState::new(state_dir)?;
            serve(state, addr).await?;
        }
        Command::Session { command } => match command {
            SessionCommand::List {
                host_url,
                environment_id,
            } => {
                let base_url = normalize_url(&host_url)?;
                let response: Value = if let Some(environment_id) = environment_id {
                    validate_session_id(&environment_id)?;
                    get_json(
                        &base_url,
                        &format!("environments/{environment_id}/sessions"),
                    )
                    .await?
                } else {
                    get_json(&base_url, "sessions").await?
                };
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            SessionCommand::Get {
                host_url,
                session_id,
            } => {
                validate_session_id(&session_id)?;
                let base_url = normalize_url(&host_url)?;
                let response: Value =
                    get_json(&base_url, &format!("sessions/{session_id}")).await?;
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            SessionCommand::Create {
                host_url,
                environment_id,
            } => {
                let request = CreateSessionRequest {
                    session_id: None,
                    policy: None,
                    metadata: Map::new(),
                };
                let base_url = normalize_url(&host_url)?;
                let response: Value = post_json(
                    &base_url,
                    &format!("environments/{environment_id}/sessions"),
                    &request,
                )
                .await?;
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            SessionCommand::Export {
                host_url,
                environment_id,
            } => {
                validate_session_id(&environment_id)?;
                let base_url = normalize_url(&host_url)?;
                let response: Value = post_empty(
                    &base_url,
                    &format!("environments/{environment_id}/artifacts/workspace"),
                )
                .await?;
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
        },
        Command::Invoke {
            host_url,
            session_id,
            tool,
            args_json,
            cwd,
        } => {
            validate_session_id(&session_id)?;
            let arguments: Map<String, Value> =
                serde_json::from_str(&args_json).context("--args-json must be a JSON object")?;
            let request = ToolInvocationRequest {
                invocation_id: None,
                session_id: session_id.clone(),
                tool_name: tool,
                arguments,
                cwd,
                timeout_ms: None,
                max_output_bytes: None,
                idempotency_key: None,
                required_capabilities: vec![],
                metadata: Map::new(),
            };
            let base_url = normalize_url(&host_url)?;
            let response: Value = post_json(
                &base_url,
                &format!("sessions/{session_id}/invocations"),
                &request,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        Command::Worker { command } => match command {
            WorkerCommand::Run {
                id,
                host_url,
                queue_dir,
                idle_sleep_ms,
            } => {
                validate_identifier("worker id", &id)?;
                let worker = Worker::new(id).with_idle_sleep(Duration::from_millis(idle_sleep_ms));
                let broker = FileBroker::new(queue_dir)?;
                let host = HttpHostClient::new(host_url)?;
                worker.run(&broker, &host).await?;
            }
            WorkerCommand::RunOnce {
                id,
                host_url,
                queue_dir,
            } => {
                validate_identifier("worker id", &id)?;
                let worker = Worker::new(id);
                let broker = FileBroker::new(queue_dir)?;
                let host = HttpHostClient::new(host_url)?;
                let result = worker.run_once(&broker, &host).await?;
                println!("{result:?}");
            }
        },
        Command::Env { command } => match command {
            EnvCommand::List { host_url } => {
                let base_url = normalize_url(&host_url)?;
                let response: Value = get_json(&base_url, "environments").await?;
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            EnvCommand::Get {
                host_url,
                environment_id,
            } => {
                validate_session_id(&environment_id)?;
                let base_url = normalize_url(&host_url)?;
                let response: Value =
                    get_json(&base_url, &format!("environments/{environment_id}")).await?;
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            EnvCommand::Effects {
                host_url,
                environment_id,
            } => {
                validate_session_id(&environment_id)?;
                let base_url = normalize_url(&host_url)?;
                let response: Value =
                    get_json(&base_url, &format!("environments/{environment_id}/effects")).await?;
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            EnvCommand::Smoke {
                queue_dir,
                state_dir,
                workspace,
            } => {
                let mut builder = Environment::builder()
                    .file_backend(queue_dir)
                    .in_process_host(state_dir)
                    .managed_worker_with_sleep("env-smoke-worker", Duration::from_millis(1));
                builder = if let Some(root) = workspace {
                    builder.existing_workspace(root)
                } else {
                    builder.new_workspace()
                };

                let env = Environment::create(builder.build()?).await?;
                let session = env.create_session().await?;
                session
                    .submit(ToolCall::new(
                        "Write",
                        object(serde_json::json!({
                            "path": "executioner-smoke.txt",
                            "content": "hello from executioner sdk"
                        }))?,
                    ))
                    .await?;
                let result = session
                    .submit(ToolCall::new(
                        "Read",
                        object(serde_json::json!({ "path": "executioner-smoke.txt" }))?,
                    ))
                    .await?;
                let environment = env.close().await?;

                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "environment": environment,
                        "result": result,
                    }))?
                );
            }
        },
    }
    Ok(())
}

fn normalize_url(url: &str) -> anyhow::Result<Url> {
    if url.starts_with("http:///") || url.starts_with("https:///") {
        anyhow::bail!("invalid host url: host is required");
    }
    let mut url = Url::parse(url).context("invalid host url")?;
    match url.scheme() {
        "http" | "https" => {}
        scheme => anyhow::bail!("invalid host url: unsupported scheme: {scheme}"),
    }
    if url.host_str().is_none() {
        anyhow::bail!("invalid host url: host is required");
    }
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("invalid host url: credentials are not allowed");
    }
    if url.query().is_some() || url.fragment().is_some() {
        anyhow::bail!("invalid host url: query strings and fragments are not allowed");
    }
    if !url.path().ends_with('/') {
        let mut path = url.path().to_string();
        path.push('/');
        url.set_path(&path);
    }
    Ok(url)
}

fn object(value: Value) -> anyhow::Result<Map<String, Value>> {
    value.as_object().cloned().context("expected a JSON object")
}

async fn post_json<T: Serialize>(base_url: &Url, path: &str, body: &T) -> anyhow::Result<Value> {
    let response = http_client()?
        .post(base_url.join(path)?)
        .json(body)
        .send()
        .await?;
    read_capped_json_response(response, MAX_HTTP_JSON_BODY_BYTES).await
}

async fn post_empty(base_url: &Url, path: &str) -> anyhow::Result<Value> {
    let response = http_client()?.post(base_url.join(path)?).send().await?;
    read_capped_json_response(response, MAX_HTTP_JSON_BODY_BYTES).await
}

async fn get_json(base_url: &Url, path: &str) -> anyhow::Result<Value> {
    let response = http_client()?.get(base_url.join(path)?).send().await?;
    read_capped_json_response(response, MAX_HTTP_JSON_BODY_BYTES).await
}

fn http_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?)
}

async fn read_capped_json_response(
    response: reqwest::Response,
    max_bytes: usize,
) -> anyhow::Result<Value> {
    let status = response.status();
    if !status.is_success() {
        let text = capped_response_text(response, MAX_HTTP_ERROR_BODY_BYTES).await;
        bail!("host returned {status}: {text}");
    }
    let bytes = capped_response_bytes(response, max_bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn capped_response_text(mut response: reqwest::Response, max_bytes: usize) -> String {
    let mut bytes = Vec::new();
    let mut truncated = false;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = max_bytes.saturating_sub(bytes.len());
                if chunk.len() > remaining {
                    bytes.extend_from_slice(&chunk[..remaining]);
                    truncated = true;
                    break;
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(err) => return format!("failed to read error body: {err}"),
        }
    }
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if truncated {
        text.push_str("\n...[truncated]");
    }
    text
}

async fn capped_response_bytes(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = max_bytes.saturating_sub(bytes.len());
                if chunk.len() > remaining {
                    bail!("response body exceeds maximum size of {max_bytes} bytes");
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(None) => return Ok(bytes),
            Err(err) => return Err(err.into()),
        }
    }
}

fn validate_session_id(session_id: &str) -> anyhow::Result<()> {
    validate_identifier("session id", session_id)
}

fn validate_identifier(label: &str, value: &str) -> anyhow::Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        anyhow::bail!("invalid {label}: {value}")
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::{timeout, Duration};

    #[test]
    fn rejects_invalid_session_ids_before_url_construction() {
        assert!(super::validate_session_id("sess_ok-1").is_ok());
        assert!(super::validate_session_id("../escaped").is_err());
        assert!(super::validate_session_id("nested/session").is_err());
        assert!(super::validate_session_id("").is_err());
    }

    #[test]
    fn rejects_invalid_worker_ids_before_queue_claiming() {
        assert!(super::validate_identifier("worker id", "worker_1").is_ok());
        assert!(super::validate_identifier("worker id", "../escaped").is_err());
        assert!(super::validate_identifier("worker id", "nested/worker").is_err());
        assert!(super::validate_identifier("worker id", "").is_err());
    }

    #[test]
    fn host_url_normalization_preserves_path_prefix() {
        let url = super::normalize_url("http://127.0.0.1:8765/api").unwrap();

        assert_eq!(
            url.join("sessions").unwrap().as_str(),
            "http://127.0.0.1:8765/api/sessions"
        );
    }

    #[test]
    fn host_url_normalization_rejects_unsafe_urls() {
        for url in [
            "file:///tmp/executioner",
            "http:///tmp/executioner",
            "http://user:pass@127.0.0.1:8765/",
            "http://127.0.0.1:8765/?token=secret",
            "http://127.0.0.1:8765/#fragment",
        ] {
            assert!(super::normalize_url(url).is_err(), "accepted {url}");
        }
    }

    #[tokio::test]
    async fn cli_http_json_response_body_is_capped() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 1024];
            let _ = socket.read(&mut buffer).await.unwrap();
            let body = format!(
                "{{\"padding\":\"{}\"}}",
                "x".repeat(super::MAX_HTTP_JSON_BODY_BYTES)
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let base_url = super::normalize_url(&format!("http://{addr}/")).unwrap();

        let err = super::post_empty(&base_url, "oversized").await.unwrap_err();

        assert!(err.to_string().contains("maximum size"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cli_http_error_response_body_is_capped() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 1024];
            let _ = socket.read(&mut buffer).await.unwrap();
            let body = "x".repeat(super::MAX_HTTP_ERROR_BODY_BYTES + 1);
            let response = format!(
                "HTTP/1.1 500 Internal Server Error\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let base_url = super::normalize_url(&format!("http://{addr}/")).unwrap();

        let err = super::post_empty(&base_url, "error").await.unwrap_err();

        assert!(err.to_string().contains("host returned"));
        assert!(err.to_string().contains("...[truncated]"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cli_http_client_does_not_follow_redirects_with_request_body() {
        let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_addr = redirect_listener.local_addr().unwrap();
        let capture_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let capture_addr = capture_listener.local_addr().unwrap();

        let redirect_server = tokio::spawn(async move {
            let (mut socket, _) = redirect_listener.accept().await.unwrap();
            let mut buffer = [0_u8; 2048];
            let _ = socket.read(&mut buffer).await.unwrap();
            let response = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nlocation: http://{capture_addr}/capture\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let capture_server = tokio::spawn(async move {
            match timeout(Duration::from_millis(500), capture_listener.accept()).await {
                Ok(Ok((mut socket, _))) => {
                    let mut buffer = [0_u8; 2048];
                    let _ = socket.read(&mut buffer).await.unwrap();
                    let response = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\nconnection: close\r\n\r\n{\"ok\":true}";
                    socket.write_all(response.as_bytes()).await.unwrap();
                    true
                }
                Ok(Err(err)) => panic!("capture listener failed: {err}"),
                Err(_) => false,
            }
        });
        let base_url = super::normalize_url(&format!("http://{redirect_addr}/")).unwrap();

        let result = super::post_json(
            &base_url,
            "sessions",
            &serde_json::json!({ "secret": "do not forward" }),
        )
        .await;

        redirect_server.await.unwrap();
        let captured = capture_server.await.unwrap();
        assert!(result.is_err());
        assert!(!captured, "redirect target received the POST body");
    }
}
