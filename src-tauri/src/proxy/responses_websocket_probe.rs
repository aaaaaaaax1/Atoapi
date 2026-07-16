use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::{header, StatusCode, Url};
use reqwest_websocket::{CloseCode, Message, RequestBuilderExt, WebSocket};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::time::timeout;
use uuid::Uuid;

const OPENAI_BETA_HEADER: &str = "responses_websockets=2026-02-06";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const EVENT_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_EVENTS_PER_RESPONSE: usize = 4_096;

pub(crate) struct ResponsesWebSocketProbeTarget {
    pub provider_id: String,
    pub model_id: String,
    pub responses_url: String,
    pub api_key: String,
    pub use_system_proxy: bool,
    pub custom_user_agent: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ResponsesWebSocketProbeStatus {
    Verified,
    Unsupported,
    Unavailable,
    Error,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ResponsesWebSocketProbeResult {
    pub provider_id: String,
    pub model_id: String,
    pub status: ResponsesWebSocketProbeStatus,
    pub message: String,
    pub checked_at: DateTime<Utc>,
    pub connection_attempts: u8,
    pub messages_sent: u8,
    pub handshake_status: Option<u16>,
    pub first_response_id_present: bool,
    pub continuation_response_id_present: bool,
    pub close_code: Option<u16>,
    pub elapsed_ms: u64,
}

struct TurnOutcome {
    response_id: Option<String>,
    text: String,
}

struct ProbeFailure {
    status: ResponsesWebSocketProbeStatus,
    message: String,
    close_code: Option<u16>,
}

pub(crate) async fn probe_responses_websocket(
    target: ResponsesWebSocketProbeTarget,
) -> ResponsesWebSocketProbeResult {
    let started = Instant::now();
    let mut result = ResponsesWebSocketProbeResult {
        provider_id: target.provider_id.clone(),
        model_id: target.model_id.clone(),
        status: ResponsesWebSocketProbeStatus::Error,
        message: String::new(),
        checked_at: Utc::now(),
        connection_attempts: 0,
        messages_sent: 0,
        handshake_status: None,
        first_response_id_present: false,
        continuation_response_id_present: false,
        close_code: None,
        elapsed_ms: 0,
    };

    let websocket_url = match responses_websocket_url(&target.responses_url) {
        Ok(url) => url,
        Err(message) => {
            return finish(
                result,
                started,
                ResponsesWebSocketProbeStatus::Error,
                message,
            )
        }
    };
    let client =
        match websocket_client(target.use_system_proxy, target.custom_user_agent.as_deref()) {
            Ok(client) => client,
            Err(err) => {
                return finish(
                    result,
                    started,
                    ResponsesWebSocketProbeStatus::Error,
                    format!("failed to create WebSocket client: {err}"),
                )
            }
        };

    result.connection_attempts = 1;
    let handshake = client
        .get(websocket_url)
        .header(header::AUTHORIZATION, format!("Bearer {}", target.api_key))
        .header("x-api-key", &target.api_key)
        .header("OpenAI-Beta", OPENAI_BETA_HEADER)
        .upgrade()
        .send();
    let upgrade = match timeout(HANDSHAKE_TIMEOUT, handshake).await {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            return finish(
                result,
                started,
                classify_error_text(&err.to_string()),
                format!("WebSocket handshake failed: {err}"),
            )
        }
        Err(_) => {
            return finish(
                result,
                started,
                ResponsesWebSocketProbeStatus::Unavailable,
                "WebSocket handshake timed out".to_string(),
            )
        }
    };
    let handshake_status = upgrade.status();
    result.handshake_status = Some(handshake_status.as_u16());
    if handshake_status != StatusCode::SWITCHING_PROTOCOLS {
        let body = upgrade
            .into_inner()
            .bytes()
            .await
            .ok()
            .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
            .unwrap_or_default();
        let summary = compact_error(&body);
        let status = classify_handshake(handshake_status, &summary);
        return finish(
            result,
            started,
            status,
            if summary.is_empty() {
                format!("WebSocket handshake returned HTTP {handshake_status}")
            } else {
                format!("WebSocket handshake returned HTTP {handshake_status}: {summary}")
            },
        );
    }

    let mut websocket = match upgrade.into_websocket().await {
        Ok(websocket) => websocket,
        Err(err) => {
            return finish(
                result,
                started,
                ResponsesWebSocketProbeStatus::Error,
                format!("WebSocket upgrade validation failed: {err}"),
            )
        }
    };

    let nonce = format!("ato-ws-{}", Uuid::new_v4().simple());
    let first_request = json!({
        "type": "response.create",
        "model": target.model_id,
        "input": format!(
            "Remember the exact token {nonce}. Reply only ACK."
        ),
        "store": true,
        "stream": true,
        "max_output_tokens": 64,
    });
    if let Err(failure) = send_json(&mut websocket, &first_request).await {
        return finish_failure(result, started, failure);
    }
    result.messages_sent = 1;
    let first = match read_turn(&mut websocket).await {
        Ok(outcome) => outcome,
        Err(failure) => return finish_failure(result, started, failure),
    };
    result.first_response_id_present = first.response_id.is_some();
    let Some(first_response_id) = first.response_id else {
        return finish(
            result,
            started,
            ResponsesWebSocketProbeStatus::Unsupported,
            "seed response completed without a reusable response id".to_string(),
        );
    };

    let continuation_request = json!({
        "type": "response.create",
        "model": target.model_id,
        "previous_response_id": first_response_id,
        "input": "Return only the exact verification token from the immediately previous turn.",
        "store": true,
        "stream": true,
        "max_output_tokens": 64,
    });
    if let Err(failure) = send_json(&mut websocket, &continuation_request).await {
        return finish_failure(result, started, failure);
    }
    result.messages_sent = 2;
    let continuation = match read_turn(&mut websocket).await {
        Ok(outcome) => outcome,
        Err(failure) => return finish_failure(result, started, failure),
    };
    result.continuation_response_id_present = continuation.response_id.is_some();
    let _ = websocket.close(CloseCode::Normal, None).await;

    if continuation
        .text
        .to_ascii_lowercase()
        .contains(&nonce.to_ascii_lowercase())
    {
        finish(
            result,
            started,
            ResponsesWebSocketProbeStatus::Verified,
            "verification passed; one WebSocket preserved semantic continuation across two response.create messages"
                .to_string(),
        )
    } else {
        finish(
            result,
            started,
            ResponsesWebSocketProbeStatus::Unsupported,
            "continuation completed but did not preserve the verification token".to_string(),
        )
    }
}

fn websocket_client(
    use_system_proxy: bool,
    custom_user_agent: Option<&str>,
) -> reqwest::Result<reqwest::Client> {
    let user_agent = custom_user_agent
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(crate::ATOAPI_USER_AGENT);
    let mut builder = reqwest::Client::builder()
        .user_agent(user_agent)
        .redirect(reqwest::redirect::Policy::none())
        .http1_only()
        .connect_timeout(Duration::from_secs(20))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true);
    if !use_system_proxy {
        builder = builder.no_proxy();
    }
    builder.build()
}

fn responses_websocket_url(responses_url: &str) -> Result<Url, String> {
    let mut url =
        Url::parse(responses_url.trim()).map_err(|err| format!("invalid Responses URL: {err}"))?;
    if !url.path().trim_end_matches('/').ends_with("/responses") {
        return Err("WebSocket verification requires an explicit Responses endpoint".to_string());
    }
    let scheme = match url.scheme() {
        "https" | "wss" => "wss",
        "http" | "ws" => "ws",
        other => return Err(format!("unsupported Responses URL scheme: {other}")),
    };
    url.set_scheme(scheme)
        .map_err(|_| "failed to convert Responses URL to WebSocket scheme".to_string())?;
    Ok(url)
}

async fn send_json(websocket: &mut WebSocket, value: &Value) -> Result<(), ProbeFailure> {
    timeout(
        SEND_TIMEOUT,
        websocket.send(Message::Text(value.to_string())),
    )
    .await
    .map_err(|_| ProbeFailure {
        status: ResponsesWebSocketProbeStatus::Unavailable,
        message: "timed out sending response.create".to_string(),
        close_code: None,
    })?
    .map_err(|err| {
        let error = err.to_string();
        ProbeFailure {
            status: classify_error_text(&error),
            message: format!("failed to send response.create: {error}"),
            close_code: close_code_from_text(&error),
        }
    })
}

async fn read_turn(websocket: &mut WebSocket) -> Result<TurnOutcome, ProbeFailure> {
    let mut response_id = None;
    let mut text = String::new();
    for _ in 0..MAX_EVENTS_PER_RESPONSE {
        let next = timeout(EVENT_TIMEOUT, websocket.next())
            .await
            .map_err(|_| ProbeFailure {
                status: ResponsesWebSocketProbeStatus::Unavailable,
                message: "timed out waiting for a WebSocket response event".to_string(),
                close_code: None,
            })?;
        let Some(message) = next else {
            return Err(ProbeFailure {
                status: ResponsesWebSocketProbeStatus::Unavailable,
                message: "WebSocket closed before a terminal response event".to_string(),
                close_code: None,
            });
        };
        let message = message.map_err(|err| {
            let error = err.to_string();
            ProbeFailure {
                status: classify_error_text(&error),
                message: format!("WebSocket receive failed: {error}"),
                close_code: close_code_from_text(&error),
            }
        })?;
        let raw = match message {
            Message::Text(text) => text,
            Message::Binary(bytes) => {
                String::from_utf8(bytes.to_vec()).map_err(|_| ProbeFailure {
                    status: ResponsesWebSocketProbeStatus::Error,
                    message: "WebSocket returned non-UTF-8 binary data".to_string(),
                    close_code: None,
                })?
            }
            Message::Close { code, reason } => {
                let code = u16::from(code);
                return Err(ProbeFailure {
                    status: classify_close(code, &reason),
                    message: if reason.trim().is_empty() {
                        format!("WebSocket closed with code {code}")
                    } else {
                        format!(
                            "WebSocket closed with code {code}: {}",
                            compact_error(&reason)
                        )
                    },
                    close_code: Some(code),
                });
            }
            Message::Ping(_) | Message::Pong(_) => continue,
        };
        let event: Value = serde_json::from_str(&raw).map_err(|err| ProbeFailure {
            status: ResponsesWebSocketProbeStatus::Error,
            message: format!("WebSocket returned invalid JSON: {err}"),
            close_code: None,
        })?;
        if let Some(id) = event
            .get("response")
            .and_then(Value::as_object)
            .and_then(|response| response.get("id"))
            .and_then(Value::as_str)
        {
            response_id = Some(id.to_string());
        }
        match event.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => {
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    text.push_str(delta);
                }
            }
            Some("response.completed") => {
                if text.is_empty() {
                    text = event
                        .get("response")
                        .and_then(extract_response_text)
                        .unwrap_or_default();
                }
                return Ok(TurnOutcome { response_id, text });
            }
            Some("error" | "response.failed" | "response.incomplete") => {
                let message = event_error_message(&event);
                return Err(ProbeFailure {
                    status: classify_error_text(&message),
                    message,
                    close_code: None,
                });
            }
            _ => {}
        }
    }
    Err(ProbeFailure {
        status: ResponsesWebSocketProbeStatus::Error,
        message: "WebSocket response exceeded the event safety limit".to_string(),
        close_code: None,
    })
}

fn extract_response_text(response: &Value) -> Option<String> {
    let mut parts = Vec::new();
    for item in response.get("output")?.as_array()? {
        if let Some(text) = item.get("text").and_then(Value::as_str) {
            parts.push(text);
        }
        if let Some(content) = item.get("content").and_then(Value::as_array) {
            for part in content {
                if let Some(text) = part
                    .get("text")
                    .or_else(|| part.get("output_text"))
                    .and_then(Value::as_str)
                {
                    parts.push(text);
                }
            }
        }
    }
    (!parts.is_empty()).then(|| parts.join(""))
}

fn event_error_message(event: &Value) -> String {
    event
        .pointer("/error/message")
        .or_else(|| event.pointer("/response/error/message"))
        .and_then(Value::as_str)
        .map(compact_error)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            format!(
                "WebSocket returned terminal event {}",
                event
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            )
        })
}

fn classify_handshake(status: StatusCode, summary: &str) -> ResponsesWebSocketProbeStatus {
    if matches!(status.as_u16(), 404 | 405 | 426 | 501) || indicates_unsupported(summary) {
        ResponsesWebSocketProbeStatus::Unsupported
    } else if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        ResponsesWebSocketProbeStatus::Unavailable
    } else {
        ResponsesWebSocketProbeStatus::Error
    }
}

fn classify_close(code: u16, reason: &str) -> ResponsesWebSocketProbeStatus {
    if matches!(code, 1012 | 1013) || indicates_unavailable(reason) {
        ResponsesWebSocketProbeStatus::Unavailable
    } else if matches!(code, 1002 | 1003) || indicates_unsupported(reason) {
        ResponsesWebSocketProbeStatus::Unsupported
    } else {
        ResponsesWebSocketProbeStatus::Error
    }
}

fn classify_error_text(text: &str) -> ResponsesWebSocketProbeStatus {
    if indicates_unavailable(text) {
        ResponsesWebSocketProbeStatus::Unavailable
    } else if indicates_unsupported(text) {
        ResponsesWebSocketProbeStatus::Unsupported
    } else {
        ResponsesWebSocketProbeStatus::Error
    }
}

fn indicates_unavailable(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    text.contains("no available account")
        || text.contains("try again later")
        || text.contains("temporarily unavailable")
        || text.contains("overloaded")
        || text.contains(" 1012")
        || text.contains(" 1013")
}

fn indicates_unsupported(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    text.contains("not support")
        || text.contains("unsupported")
        || text.contains("unknown event")
        || text.contains("unknown parameter")
        || text.contains("websocket is not enabled")
}

fn close_code_from_text(text: &str) -> Option<u16> {
    [1013, 1012, 1011, 1008, 1003, 1002]
        .into_iter()
        .find(|code| text.contains(&code.to_string()))
}

fn compact_error(text: &str) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    compact.chars().take(320).collect()
}

fn finish_failure(
    mut result: ResponsesWebSocketProbeResult,
    started: Instant,
    failure: ProbeFailure,
) -> ResponsesWebSocketProbeResult {
    result.close_code = failure.close_code;
    finish(result, started, failure.status, failure.message)
}

fn finish(
    mut result: ResponsesWebSocketProbeResult,
    started: Instant,
    status: ResponsesWebSocketProbeStatus,
    message: String,
) -> ResponsesWebSocketProbeResult {
    result.status = status;
    result.message = message;
    result.elapsed_ms = started.elapsed().as_millis() as u64;
    result
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use axum::{
        extract::ws::{CloseFrame, Message as AxumMessage, WebSocketUpgrade},
        response::IntoResponse,
        routing::get,
        Router,
    };
    use tokio::{net::TcpListener, sync::Mutex};

    use super::*;

    async fn start_semantic_server() -> (
        String,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<Value>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let connections = Arc::new(AtomicUsize::new(0));
        let messages = Arc::new(Mutex::new(Vec::new()));
        let connections_for_route = connections.clone();
        let messages_for_route = messages.clone();
        let app = Router::new().route(
            "/v1/responses",
            get(move |upgrade: WebSocketUpgrade| {
                let connections = connections_for_route.clone();
                let messages = messages_for_route.clone();
                async move {
                    upgrade
                        .on_upgrade(move |mut socket| async move {
                            connections.fetch_add(1, Ordering::SeqCst);
                            let first = socket.recv().await.unwrap().unwrap();
                            let AxumMessage::Text(first) = first else {
                                panic!("expected text response.create");
                            };
                            let first: Value = serde_json::from_str(&first).unwrap();
                            let token = first["input"]
                                .as_str()
                                .unwrap()
                                .split("token ")
                                .nth(1)
                                .and_then(|value| value.split('.').next())
                                .unwrap()
                                .to_string();
                            messages.lock().await.push(first);
                            socket
                                .send(AxumMessage::Text(
                                    json!({"type":"response.created","response":{"id":"resp-seed"}})
                                        .to_string(),
                                ))
                                .await
                                .unwrap();
                            socket
                                .send(AxumMessage::Text(
                                    json!({"type":"response.completed","response":{"id":"resp-seed"}})
                                        .to_string(),
                                ))
                                .await
                                .unwrap();

                            let second = socket.recv().await.unwrap().unwrap();
                            let AxumMessage::Text(second) = second else {
                                panic!("expected second text response.create");
                            };
                            let second: Value = serde_json::from_str(&second).unwrap();
                            messages.lock().await.push(second);
                            socket
                                .send(AxumMessage::Text(
                                    json!({"type":"response.created","response":{"id":"resp-next"}})
                                        .to_string(),
                                ))
                                .await
                                .unwrap();
                            socket
                                .send(AxumMessage::Text(
                                    json!({
                                        "type":"response.completed",
                                        "response":{
                                            "id":"resp-next",
                                            "output":[{
                                                "type":"message",
                                                "content":[{"type":"output_text","text":token}]
                                            }]
                                        }
                                    }).to_string(),
                                ))
                                .await
                                .unwrap();
                        })
                        .into_response()
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (
            format!("http://{address}/v1/responses"),
            connections,
            messages,
            task,
        )
    }

    fn target(responses_url: String) -> ResponsesWebSocketProbeTarget {
        ResponsesWebSocketProbeTarget {
            provider_id: "provider-a".to_string(),
            model_id: "gpt-5.6-luna".to_string(),
            responses_url,
            api_key: "test-secret".to_string(),
            use_system_proxy: false,
            custom_user_agent: None,
        }
    }

    #[tokio::test]
    async fn semantic_probe_uses_one_connection_and_exactly_two_messages() {
        let (url, connections, messages, server) = start_semantic_server().await;

        let result = probe_responses_websocket(target(url)).await;

        assert_eq!(result.status, ResponsesWebSocketProbeStatus::Verified);
        assert_eq!(result.connection_attempts, 1);
        assert_eq!(result.messages_sent, 2);
        assert_eq!(result.handshake_status, Some(101));
        assert!(result.first_response_id_present);
        assert!(result.continuation_response_id_present);
        assert_eq!(connections.load(Ordering::SeqCst), 1);
        let messages = messages.lock().await;
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["type"], "response.create");
        assert_eq!(messages[0]["store"], true);
        assert_eq!(messages[1]["previous_response_id"], "resp-seed");
        assert_eq!(messages[1]["store"], true);
        drop(messages);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn close_1013_is_unavailable_without_a_second_message() {
        let connections = Arc::new(AtomicUsize::new(0));
        let connections_for_route = connections.clone();
        let app = Router::new().route(
            "/v1/responses",
            get(move |upgrade: WebSocketUpgrade| {
                let connections = connections_for_route.clone();
                async move {
                    upgrade
                        .on_upgrade(move |mut socket| async move {
                            connections.fetch_add(1, Ordering::SeqCst);
                            let _ = socket.recv().await;
                            socket
                                .send(AxumMessage::Close(Some(CloseFrame {
                                    code: 1013,
                                    reason: "no available account".into(),
                                })))
                                .await
                                .unwrap();
                        })
                        .into_response()
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let result =
            probe_responses_websocket(target(format!("http://{address}/v1/responses"))).await;

        assert_eq!(result.status, ResponsesWebSocketProbeStatus::Unavailable);
        assert_eq!(result.messages_sent, 1);
        assert_eq!(result.close_code, Some(1013));
        assert_eq!(connections.load(Ordering::SeqCst), 1);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn http_404_handshake_is_unsupported_without_sending_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new()).await.unwrap();
        });

        let result =
            probe_responses_websocket(target(format!("http://{address}/v1/responses"))).await;

        assert_eq!(result.status, ResponsesWebSocketProbeStatus::Unsupported);
        assert_eq!(result.connection_attempts, 1);
        assert_eq!(result.messages_sent, 0);
        assert_eq!(result.handshake_status, Some(404));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn non_responses_endpoint_is_rejected_before_a_connection_attempt() {
        let result =
            probe_responses_websocket(target("http://127.0.0.1:9/v1/chat/completions".to_string()))
                .await;

        assert_eq!(result.status, ResponsesWebSocketProbeStatus::Error);
        assert_eq!(result.connection_attempts, 0);
        assert_eq!(result.messages_sent, 0);
        assert!(result.message.contains("explicit Responses endpoint"));
    }

    #[tokio::test]
    async fn handshake_redirect_is_reported_without_following_it() {
        let redirected_hits = Arc::new(AtomicUsize::new(0));
        let redirected_hits_for_route = redirected_hits.clone();
        let app = Router::new()
            .route(
                "/v1/responses",
                get(|| async {
                    (
                        StatusCode::TEMPORARY_REDIRECT,
                        [(header::LOCATION, "/redirected")],
                    )
                }),
            )
            .route(
                "/redirected",
                get(move || {
                    let redirected_hits = redirected_hits_for_route.clone();
                    async move {
                        redirected_hits.fetch_add(1, Ordering::SeqCst);
                        StatusCode::NOT_FOUND
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let result =
            probe_responses_websocket(target(format!("http://{address}/v1/responses"))).await;

        assert_eq!(result.status, ResponsesWebSocketProbeStatus::Error);
        assert_eq!(result.connection_attempts, 1);
        assert_eq!(result.messages_sent, 0);
        assert_eq!(result.handshake_status, Some(307));
        assert_eq!(redirected_hits.load(Ordering::SeqCst), 0);
        server.abort();
        let _ = server.await;
    }
}
