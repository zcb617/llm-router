//! Minimal Rust HTTP outbound used by llm_router for Codex CLI OAuth.
//! Uses reqwest + rustls-tls-native-roots (same family as Codex CLI HTTP client).
//!
//! Protocol (stdin JSON → stdout JSON):
//! {
//!   "method": "POST",
//!   "url": "https://...",
//!   "headers": [["Name", "value"], ...],
//!   "body_b64": "...",
//!   "timeout_ms": 600000
//! }
//!
//! Response:
//! {
//!   "ok": true,
//!   "status": 200,
//!   "headers": [["name", "value"], ...],
//!   "body_b64": "..."
//! }

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct OutboundRequest {
    method: String,
    url: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
    #[serde(default)]
    body_b64: Option<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    600_000
}

#[derive(Debug, Serialize)]
struct OutboundResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    headers: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_b64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn emit(resp: &OutboundResponse) {
    let encoded = serde_json::to_vec(resp).unwrap_or_else(|e| {
        format!(r#"{{"ok":false,"error":"serialize failed: {e}"}}"#).into_bytes()
    });
    let mut stdout = io::stdout().lock();
    let _ = stdout.write_all(&encoded);
    let _ = stdout.write_all(b"\n");
    let _ = stdout.flush();
}

#[tokio::main]
async fn main() {
    let mut stdin_buf = Vec::new();
    if let Err(err) = io::stdin().read_to_end(&mut stdin_buf) {
        emit(&OutboundResponse {
            ok: false,
            status: None,
            headers: vec![],
            body_b64: None,
            error: Some(format!("failed to read stdin: {err}")),
        });
        std::process::exit(2);
    }

    let req: OutboundRequest = match serde_json::from_slice(&stdin_buf) {
        Ok(v) => v,
        Err(err) => {
            emit(&OutboundResponse {
                ok: false,
                status: None,
                headers: vec![],
                body_b64: None,
                error: Some(format!("invalid request json: {err}")),
            });
            std::process::exit(2);
        }
    };

    match run(req).await {
        Ok(resp) => {
            emit(&resp);
            if !resp.ok {
                std::process::exit(1);
            }
        }
        Err(err) => {
            emit(&OutboundResponse {
                ok: false,
                status: None,
                headers: vec![],
                body_b64: None,
                error: Some(err),
            });
            std::process::exit(1);
        }
    }
}

async fn run(req: OutboundRequest) -> Result<OutboundResponse, String> {
    let method = Method::from_bytes(req.method.as_bytes())
        .map_err(|e| format!("invalid method {}: {e}", req.method))?;

    let mut header_map = HeaderMap::new();
    for (name, value) in &req.headers {
        // Host is set by the client from the URL; skip explicit Host to avoid mismatches.
        if name.eq_ignore_ascii_case("host") {
            continue;
        }
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| format!("invalid header name {name}: {e}"))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|e| format!("invalid header value for {name}: {e}"))?;
        header_map.append(header_name, header_value);
    }

    let body_bytes = match req.body_b64.as_deref() {
        Some(raw) if !raw.is_empty() => B64
            .decode(raw.as_bytes())
            .map_err(|e| format!("invalid body_b64: {e}"))?,
        _ => Vec::new(),
    };

    let timeout = Duration::from_millis(req.timeout_ms.max(1));
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(4)
        .build()
        .map_err(|e| format!("client build failed: {e}"))?;

    let mut builder = client.request(method, &req.url).headers(header_map);
    if !body_bytes.is_empty() {
        builder = builder.body(body_bytes);
    }

    let response = builder
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;

    let status = response.status().as_u16();
    let mut out_headers = Vec::new();
    for (name, value) in response.headers().iter() {
        if let Ok(v) = value.to_str() {
            out_headers.push((name.as_str().to_string(), v.to_string()));
        }
    }

    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("read body failed: {e}"))?;
        body.extend_from_slice(&chunk);
    }

    Ok(OutboundResponse {
        ok: true,
        status: Some(status),
        headers: out_headers,
        body_b64: Some(B64.encode(body)),
        error: None,
    })
}
