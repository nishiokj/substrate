use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use executioner_core::{
    CreateSessionRequest, ErrorEnvelope, ExecutionerError, HostState, Session,
    ToolInvocationRequest, ToolInvocationResult,
};
use serde::Serialize;
use std::net::SocketAddr;

const MAX_HTTP_REQUEST_BODY_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct HostServer {
    state: HostState,
}

impl HostServer {
    pub fn new(state: HostState) -> Self {
        Self { state }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/health", get(health))
            .route("/sessions", post(create_session))
            .route(
                "/sessions/{session_id}",
                get(get_session).delete(delete_session),
            )
            .route("/sessions/{session_id}/close", post(close_session))
            .route(
                "/sessions/{session_id}/invocations",
                post(execute_invocation),
            )
            .route("/sessions/{session_id}/effects", get(get_effects))
            .route(
                "/sessions/{session_id}/artifacts/workspace",
                post(export_workspace),
            )
            .layer(DefaultBodyLimit::max(MAX_HTTP_REQUEST_BODY_BYTES))
            .with_state(self.state)
    }
}

pub async fn serve(state: HostState, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, HostServer::new(state).router()).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

async fn create_session(
    State(state): State<HostState>,
    payload: Result<Json<CreateSessionRequest>, JsonRejection>,
) -> Result<Json<executioner_core::CreateSessionResponse>, ApiError> {
    let Json(request) = payload.map_err(json_rejection)?;
    Ok(Json(state.create_session(request)?))
}

async fn get_session(
    State(state): State<HostState>,
    Path(session_id): Path<String>,
) -> Result<Json<Session>, ApiError> {
    Ok(Json(state.get_session(&session_id)?))
}

async fn close_session(
    State(state): State<HostState>,
    Path(session_id): Path<String>,
) -> Result<Json<Session>, ApiError> {
    Ok(Json(state.close_session(&session_id)?))
}

async fn delete_session(
    State(state): State<HostState>,
    Path(session_id): Path<String>,
) -> Result<Json<Session>, ApiError> {
    Ok(Json(state.destroy_session(&session_id)?))
}

async fn execute_invocation(
    State(state): State<HostState>,
    Path(session_id): Path<String>,
    payload: Result<Json<ToolInvocationRequest>, JsonRejection>,
) -> Result<Json<ToolInvocationResult>, ApiError> {
    let Json(mut request) = payload.map_err(json_rejection)?;
    if request.session_id != session_id {
        return Err(ExecutionerError::InvalidRequest(format!(
            "request sessionId does not match route session id: {session_id}"
        ))
        .into());
    }
    request.session_id = session_id;
    Ok(Json(state.execute_invocation(request)?))
}

async fn get_effects(
    State(state): State<HostState>,
    Path(session_id): Path<String>,
) -> Result<Json<Vec<executioner_core::Effect>>, ApiError> {
    Ok(Json(state.effects(&session_id)?))
}

async fn export_workspace(
    State(state): State<HostState>,
    Path(session_id): Path<String>,
) -> Result<Json<executioner_core::WorkspaceArtifact>, ApiError> {
    Ok(Json(state.export_workspace(&session_id)?))
}

fn json_rejection(rejection: JsonRejection) -> ApiError {
    ApiError::with_status(
        ExecutionerError::InvalidRequest(format!("invalid JSON request body: {rejection}")),
        rejection.status(),
    )
}

#[derive(Debug)]
struct ApiError {
    error: ExecutionerError,
    status: Option<StatusCode>,
}

impl ApiError {
    fn with_status(error: ExecutionerError, status: StatusCode) -> Self {
        Self {
            error,
            status: Some(status),
        }
    }
}

impl From<ExecutionerError> for ApiError {
    fn from(value: ExecutionerError) -> Self {
        Self {
            error: value,
            status: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let default_status = match &self.error {
            ExecutionerError::SessionNotFound(_) => StatusCode::NOT_FOUND,
            ExecutionerError::PolicyDenied(_) => StatusCode::FORBIDDEN,
            ExecutionerError::InvalidRequest(_) | ExecutionerError::SessionNotReady(_) => {
                StatusCode::BAD_REQUEST
            }
            ExecutionerError::ToolNotFound(_) => StatusCode::NOT_FOUND,
            ExecutionerError::Io(_) | ExecutionerError::Json(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        let status = self.status.unwrap_or(default_status);

        let body = ErrorBody {
            error: ErrorEnvelope {
                code: self.error.code().to_string(),
                message: self.error.to_string(),
                retryable: false,
            },
        };
        (status, Json(body)).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: ErrorEnvelope,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use executioner_core::{
        ExecutionPolicy, NetworkPolicy, ProcessPolicy, WorkspaceMode, WorkspaceSpec,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use tower::ServiceExt;

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

    async fn error_code(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        body["error"]["code"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn creates_session_over_http_router() {
        let temp = TempDir::new().unwrap();
        let app = HostServer::new(HostState::new(temp.path()).unwrap()).router();
        let body = serde_json::to_vec(&executioner_core::CreateSessionRequest {
            session_id: None,
            workspace: WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            },
            policy: policy(),
            ttl_ms: None,
            metadata: serde_json::Map::new(),
        })
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn runs_write_then_read_over_router() {
        let temp = TempDir::new().unwrap();
        let state = HostState::new(temp.path()).unwrap();
        let session = state
            .create_session(executioner_core::CreateSessionRequest {
                session_id: Some("sess".to_string()),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: policy(),
                ttl_ms: None,
                metadata: serde_json::Map::new(),
            })
            .unwrap()
            .session;

        let app = HostServer::new(state).router();
        let write_body = json!({
            "sessionId": session.id,
            "toolName": "Write",
            "arguments": { "path": "hello.txt", "content": "hello" },
            "cwd": "/workspace"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions/sess/invocations")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&write_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rejects_invocation_body_session_mismatch() {
        let temp = TempDir::new().unwrap();
        let state = HostState::new(temp.path()).unwrap();
        state
            .create_session(executioner_core::CreateSessionRequest {
                session_id: Some("sess".to_string()),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: policy(),
                ttl_ms: None,
                metadata: serde_json::Map::new(),
            })
            .unwrap();

        let app = HostServer::new(state).router();
        let body = json!({
            "sessionId": "other_session",
            "toolName": "Write",
            "arguments": { "path": "hello.txt", "content": "hello" },
            "cwd": "/workspace"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions/sess/invocations")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_unknown_create_session_fields() {
        let temp = TempDir::new().unwrap();
        let app = HostServer::new(HostState::new(temp.path()).unwrap()).router();
        let body = json!({
            "workspace": {
                "mode": "new",
                "mountAsWorkspace": true
            },
            "policy": policy(),
            "unsupportedControl": true
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status().is_client_error());
        assert_eq!(error_code(response).await, "invalid_request");
    }

    #[tokio::test]
    async fn rejects_malformed_json_with_error_envelope() {
        let temp = TempDir::new().unwrap();
        let app = HostServer::new(HostState::new(temp.path()).unwrap()).router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from("{ definitely not json"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(response).await, "invalid_request");
    }

    #[tokio::test]
    async fn rejects_oversized_json_body_before_creating_session() {
        let temp = TempDir::new().unwrap();
        let state = HostState::new(temp.path()).unwrap();
        let app = HostServer::new(state.clone()).router();
        let body = format!(
            r#"{{
                "sessionId": "sess_huge_body",
                "workspace": {{ "mode": "new", "mountAsWorkspace": true }},
                "policy": {},
                "metadata": {{ "padding": "{}" }}
            }}"#,
            serde_json::to_string(&policy()).unwrap(),
            "x".repeat(MAX_HTTP_REQUEST_BODY_BYTES)
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(error_code(response).await, "invalid_request");
        assert!(state.get_session("sess_huge_body").is_err());
    }

    #[tokio::test]
    async fn rejects_unknown_invocation_fields_without_running_tool() {
        let temp = TempDir::new().unwrap();
        let state = HostState::new(temp.path()).unwrap();
        let session = state
            .create_session(executioner_core::CreateSessionRequest {
                session_id: Some("sess".to_string()),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: policy(),
                ttl_ms: None,
                metadata: serde_json::Map::new(),
            })
            .unwrap()
            .session;

        let app = HostServer::new(state).router();
        let body = json!({
            "sessionId": "sess",
            "toolName": "Write",
            "arguments": { "path": "should-not-exist.txt", "content": "unsafe" },
            "cwd": "/workspace",
            "unsupportedControl": true
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions/sess/invocations")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status().is_client_error());
        assert_eq!(error_code(response).await, "invalid_request");
        assert!(
            !std::path::Path::new(&format!("{}/should-not-exist.txt", session.workspace.root))
                .exists()
        );
    }

    #[tokio::test]
    async fn exports_workspace_artifact_over_router() {
        let temp = TempDir::new().unwrap();
        let state = HostState::new(temp.path()).unwrap();
        let session = state
            .create_session(executioner_core::CreateSessionRequest {
                session_id: Some("sess".to_string()),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: policy(),
                ttl_ms: None,
                metadata: serde_json::Map::new(),
            })
            .unwrap()
            .session;
        std::fs::write(format!("{}/hello.txt", session.workspace.root), "hello").unwrap();

        let app = HostServer::new(state).router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions/sess/artifacts/workspace")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
