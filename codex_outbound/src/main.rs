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
//!
//! Optional request correlation fields (`request_id`, `source`) are echoed in
//! responses. Successful responses also include `first_body_at_ms`, the Unix
//! millisecond timestamp of the first non-empty response body chunk and
//! `first_output_at_ms`, the first Responses content delta timestamp.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Deserialize)]
struct OutboundRequest {
    /// HTTP 出站请求方法；CONNECT 隧道模式不使用。
    #[serde(default)]
    method: Option<String>,
    /// HTTP 出站请求 URL；CONNECT 隧道模式不使用。
    #[serde(default)]
    url: Option<String>,
    /// 出站模式；缺省时保持既有 HTTP 请求行为。
    #[serde(default)]
    mode: Option<String>,
    /// CONNECT 隧道唯一允许访问的上游主机。
    #[serde(default)]
    target_host: Option<String>,
    /// CONNECT 隧道唯一允许访问的上游端口。
    #[serde(default)]
    target_port: Option<u16>,
    #[serde(default)]
    headers: Vec<(String, String)>,
    #[serde(default)]
    body_b64: Option<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    source: Option<String>,
}

fn default_timeout_ms() -> u64 {
    600_000
}

#[derive(Debug, Serialize)]
struct OutboundResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    headers: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_b64: Option<String>,
    /// 首个非空响应体 chunk 的 Unix 毫秒时间戳。
    first_body_at_ms: Option<u64>,
    /// 首个 Responses 内容增量所在接收 chunk 的 Unix 毫秒时间戳。
    first_output_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn unix_time_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as u64)
}

fn record_first_body_at_ms(
    first_body_at_ms: &mut Option<u64>,
    chunk: &[u8],
    observed_at_ms: Option<u64>,
) {
    if !chunk.is_empty() && first_body_at_ms.is_none() {
        *first_body_at_ms = observed_at_ms;
    }
}

/// 判断 SSE 事件是否包含可计作首个输出的 Responses 内容增量。
fn is_content_delta(event: &serde_json::Value) -> bool {
    let event_type = event.get("type").and_then(serde_json::Value::as_str);
    let delta = event.get("delta").and_then(serde_json::Value::as_str);
    matches!(
        event_type,
        Some(
            "response.output_text.delta"
                | "response.function_call_arguments.delta"
                | "response.custom_tool_call_input.delta"
                | "response.refusal.delta"
        )
    ) && delta.is_some_and(|value| !value.is_empty())
}

/// 解析完整 SSE data 行，并在首次内容增量时记录接收时间。
fn record_sse_data_line(
    line: &[u8],
    first_output_at_ms: &mut Option<u64>,
    observed_at_ms: Option<u64>,
) {
    if first_output_at_ms.is_some() {
        return;
    }
    let Some(data) = line.strip_prefix(b"data:") else {
        return;
    };
    let Ok(data) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(event) = serde_json::from_str::<serde_json::Value>(data.trim()) else {
        return;
    };
    if is_content_delta(&event) {
        *first_output_at_ms = observed_at_ms;
    }
}

/// 增量处理跨 chunk 的 SSE 行，并在 EOF 时解析最后一条未换行事件。
fn record_first_output_at_ms(
    first_output_at_ms: &mut Option<u64>,
    timing_buffer: &mut Vec<u8>,
    chunk: &[u8],
    observed_at_ms: Option<u64>,
    eof: bool,
) {
    if first_output_at_ms.is_some() {
        timing_buffer.clear();
        return;
    }
    timing_buffer.extend_from_slice(chunk);
    while let Some(line_end) = timing_buffer.iter().position(|byte| *byte == b'\n') {
        let mut line: Vec<u8> = timing_buffer.drain(..=line_end).collect();
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        record_sse_data_line(&line, first_output_at_ms, observed_at_ms);
        if first_output_at_ms.is_some() {
            timing_buffer.clear();
            return;
        }
    }
    if eof && !timing_buffer.is_empty() {
        let mut line = std::mem::take(timing_buffer);
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        record_sse_data_line(&line, first_output_at_ms, observed_at_ms);
    }
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
            request_id: None,
            source: None,
            status: None,
            headers: vec![],
            body_b64: None,
            first_body_at_ms: None,
            first_output_at_ms: None,
            error: Some(format!("failed to read stdin: {err}")),
        });
        std::process::exit(2);
    }

    let req: OutboundRequest = match serde_json::from_slice(&stdin_buf) {
        Ok(v) => v,
        Err(err) => {
            emit(&OutboundResponse {
                ok: false,
                request_id: None,
                source: None,
                status: None,
                headers: vec![],
                body_b64: None,
                first_body_at_ms: None,
                first_output_at_ms: None,
                error: Some(format!("invalid request json: {err}")),
            });
            std::process::exit(2);
        }
    };

    if req.mode.as_deref() == Some("connect_tunnel") {
        let target_host = match req.target_host.as_deref() {
            Some(host) if !host.trim().is_empty() => host,
            _ => {
                emit_connect_tunnel_startup_error("missing target_host");
                std::process::exit(1);
            }
        };
        let target_port = match req.target_port {
            Some(port) if port > 0 => port,
            _ => {
                emit_connect_tunnel_startup_error("invalid target_port");
                std::process::exit(1);
            }
        };
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(_) => {
                emit_connect_tunnel_startup_error("failed to bind loopback listener");
                std::process::exit(1);
            }
        };
        let local_addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(_) => {
                emit_connect_tunnel_startup_error("failed to read loopback listener address");
                std::process::exit(1);
            }
        };
        if emit_connect_tunnel_ready(local_addr).is_err() {
            std::process::exit(1);
        }
        if run_connect_tunnel(listener, target_host, target_port)
            .await
            .is_err()
        {
            std::process::exit(1);
        }
        return;
    }

    if req.mode.as_deref() == Some("http_stream") {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(_) => {
                emit_connect_tunnel_startup_error("failed to bind loopback listener");
                std::process::exit(1);
            }
        };
        let local_addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(_) => {
                emit_connect_tunnel_startup_error("failed to read loopback listener address");
                std::process::exit(1);
            }
        };
        if emit_connect_tunnel_ready(local_addr).is_err() {
            std::process::exit(1);
        }
        if let Err(error) = run_http_stream(listener, &req).await {
            eprintln!("HTTP stream forwarding failed: {error}");
            std::process::exit(1);
        }
        return;
    }

    match run(&req).await {
        Ok(resp) => {
            emit(&resp);
            if !resp.ok {
                std::process::exit(1);
            }
        }
        Err(err) => {
            emit(&OutboundResponse {
                ok: false,
                request_id: req.request_id.clone(),
                source: req.source.clone(),
                status: None,
                headers: vec![],
                body_b64: None,
                first_body_at_ms: None,
                first_output_at_ms: None,
                error: Some(err),
            });
            std::process::exit(1);
        }
    }
}

/// 将 CONNECT 隧道监听地址作为单行 JSON 发布给本地 Python 调用方。
fn emit_connect_tunnel_ready(local_addr: SocketAddr) -> io::Result<()> {
    let payload = serde_json::json!({
        "ok": true,
        "host": local_addr.ip().to_string(),
        "port": local_addr.port(),
    });
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &payload)?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}

/// 在隧道启动失败时向 Python 调用方发送不含认证信息的错误 JSON。
fn emit_connect_tunnel_startup_error(error: &str) {
    let payload = serde_json::json!({"ok": false, "error": error});
    let mut stdout = io::stdout().lock();
    let _ = serde_json::to_writer(&mut stdout, &payload);
    let _ = stdout.write_all(b"\n");
    let _ = stdout.flush();
}

/// 接受一条受目标限制的本地 CONNECT 请求并原样转发后续 TCP 字节。
async fn run_connect_tunnel(
    listener: TcpListener,
    target_host: &str,
    target_port: u16,
) -> Result<(), String> {
    const MAX_CONNECT_HEADER_BYTES: usize = 16 * 1024;

    let (client, _) = listener
        .accept()
        .await
        .map_err(|_| "failed to accept local CONNECT client".to_string())?;
    drop(listener);
    let mut client = BufReader::new(client);
    let mut header = Vec::new();
    let mut ending = [0_u8; 4];
    loop {
        if header.len() >= MAX_CONNECT_HEADER_BYTES {
            send_connect_response(client.get_mut(), "400 Bad Request").await;
            return Err("CONNECT request header too large".to_string());
        }
        let mut byte = [0_u8; 1];
        if client.read_exact(&mut byte).await.is_err() {
            send_connect_response(client.get_mut(), "400 Bad Request").await;
            return Err("incomplete CONNECT request header".to_string());
        }
        header.push(byte[0]);
        ending.rotate_left(1);
        ending[3] = byte[0];
        if ending == *b"\r\n\r\n" {
            break;
        }
    }

    let request_header = match std::str::from_utf8(&header) {
        Ok(value) => value,
        Err(_) => {
            send_connect_response(client.get_mut(), "400 Bad Request").await;
            return Err("invalid CONNECT request encoding".to_string());
        }
    };
    let request_line = match request_header.lines().next() {
        Some(line) => line,
        None => {
            send_connect_response(client.get_mut(), "400 Bad Request").await;
            return Err("missing CONNECT request line".to_string());
        }
    };
    let request_parts: Vec<&str> = request_line.split_whitespace().collect();
    if request_parts.len() != 3 || request_parts[0] != "CONNECT" {
        send_connect_response(client.get_mut(), "400 Bad Request").await;
        return Err("invalid CONNECT request line".to_string());
    }
    let (requested_host, requested_port) = match parse_connect_authority(request_parts[1]) {
        Some(authority) => authority,
        None => {
            send_connect_response(client.get_mut(), "400 Bad Request").await;
            return Err("invalid CONNECT authority".to_string());
        }
    };
    if !requested_host.eq_ignore_ascii_case(target_host) || requested_port != target_port {
        send_connect_response(client.get_mut(), "403 Forbidden").await;
        return Err("CONNECT target does not match configured target".to_string());
    }

    let mut upstream = match tokio::time::timeout(
        Duration::from_secs(30),
        TcpStream::connect((target_host, target_port)),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        _ => {
            send_connect_response(client.get_mut(), "502 Bad Gateway").await;
            return Err("failed to connect to configured target".to_string());
        }
    };
    client
        .get_mut()
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await
        .map_err(|_| "failed to acknowledge CONNECT request".to_string())?;
    tokio::io::copy_bidirectional(&mut client, &mut upstream)
        .await
        .map_err(|_| "CONNECT tunnel byte forwarding failed".to_string())?;
    Ok(())
}

/// 解析 CONNECT authority，并要求它显式携带有效端口。
fn parse_connect_authority(authority: &str) -> Option<(String, u16)> {
    let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        let closing_bracket = bracketed.find(']')?;
        let host = &bracketed[..closing_bracket];
        let port = bracketed[closing_bracket + 1..].strip_prefix(':')?;
        (host, port)
    } else {
        let (host, port) = authority.rsplit_once(':')?;
        if host.is_empty() || host.contains(':') {
            return None;
        }
        (host, port)
    };
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), port.parse::<u16>().ok()?))
}

/// 向本地 mitmproxy 客户端返回 CONNECT 状态行并结束响应头。
async fn send_connect_response(client: &mut TcpStream, status: &str) {
    let response = format!("HTTP/1.1 {status}\r\nConnection: close\r\n\r\n");
    let _ = client.write_all(response.as_bytes()).await;
}

/// 处理一次本机 HTTP 中继请求，并在客户端断开时取消对应 Codex 上游请求。
async fn run_http_stream(listener: TcpListener, req: &OutboundRequest) -> Result<(), String> {
    const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;

    let (mut client, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
        .await
        .map_err(|_| "local stream client accept timed out".to_string())?
        .map_err(|_| "failed to accept local stream client".to_string())?;
    drop(listener);

    let header_result = tokio::time::timeout(Duration::from_secs(30), async {
        let mut header = Vec::new();
        let mut ending = [0_u8; 4];
        loop {
            if header.len() >= MAX_HTTP_HEADER_BYTES {
                return Err("local HTTP request header too large".to_string());
            }
            let mut byte = [0_u8; 1];
            client
                .read_exact(&mut byte)
                .await
                .map_err(|error| format!("incomplete local HTTP request header: {error}"))?;
            header.push(byte[0]);
            ending.rotate_left(1);
            ending[3] = byte[0];
            if ending == *b"\r\n\r\n" {
                return Ok(header);
            }
        }
    })
    .await
    .map_err(|_| "local HTTP request header timed out".to_string())?;

    let header = match header_result {
        Ok(header) => header,
        Err(error) => {
            send_http_stream_error(&mut client, 400, "Bad Request", "invalid local stream request").await;
            return Err(error);
        }
    };
    let request_header = match std::str::from_utf8(&header) {
        Ok(value) => value,
        Err(_) => {
            send_http_stream_error(&mut client, 400, "Bad Request", "invalid local stream request").await;
            return Err("invalid local HTTP request encoding".to_string());
        }
    };
    let mut lines = request_header.split("\r\n");
    if lines.next() != Some("POST /stream HTTP/1.1") {
        send_http_stream_error(&mut client, 400, "Bad Request", "invalid local stream request").await;
        return Err("invalid local HTTP request line".to_string());
    }
    let mut has_body = false;
    for line in lines.filter(|line| !line.is_empty()) {
        let Some((name, value)) = line.split_once(':') else {
            send_http_stream_error(&mut client, 400, "Bad Request", "invalid local stream request").await;
            return Err("invalid local HTTP request header".to_string());
        };
        if name.eq_ignore_ascii_case("transfer-encoding")
            || (name.eq_ignore_ascii_case("content-length") && value.trim() != "0")
        {
            has_body = true;
        }
    }
    if has_body {
        send_http_stream_error(&mut client, 400, "Bad Request", "local stream request body is not allowed").await;
        return Err("local HTTP request body is not allowed".to_string());
    }

    let (mut reader, mut writer) = client.into_split();
    let relay = async {
        let response = match send_request(req).await {
            Ok(response) => response,
            Err(error) => {
                let mut safe_error = req
                    .url
                    .as_deref()
                    .filter(|url| !url.is_empty())
                    .map(|url| error.replace(url, "[upstream URL]"))
                    .unwrap_or(error);
                for (name, value) in &req.headers {
                    if name.eq_ignore_ascii_case("authorization")
                        || name.eq_ignore_ascii_case("proxy-authorization")
                        || name.eq_ignore_ascii_case("cookie")
                        || name.eq_ignore_ascii_case("set-cookie")
                    {
                        if !value.is_empty() {
                            safe_error = safe_error.replace(value, "[redacted]");
                        }
                    }
                }
                eprintln!("Codex upstream request failed: {safe_error}");
                send_http_stream_error(
                    &mut writer,
                    502,
                    "Bad Gateway",
                    "upstream request failed",
                )
                .await;
                return Ok(());
            }
        };

        let status = response.status();
        let reason = status.canonical_reason().unwrap_or("Unknown");
        let mut response_head = format!("HTTP/1.1 {} {}\r\n", status.as_u16(), reason);
        for (name, value) in response.headers().iter() {
            let name = name.as_str();
            if matches!(
                name,
                "connection"
                    | "keep-alive"
                    | "proxy-authenticate"
                    | "proxy-authorization"
                    | "te"
                    | "trailer"
                    | "transfer-encoding"
                    | "upgrade"
                    | "content-length"
            ) {
                continue;
            }
            if let Ok(value) = value.to_str() {
                response_head.push_str(name);
                response_head.push_str(": ");
                response_head.push_str(value);
                response_head.push_str("\r\n");
            }
        }
        response_head.push_str("Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
        writer
            .write_all(response_head.as_bytes())
            .await
            .map_err(|error| format!("failed to write local HTTP response headers: {error}"))?;

        let mut upstream = response.bytes_stream();
        while let Some(chunk) = upstream.next().await {
            let chunk = chunk.map_err(|error| {
                format!("failed to read upstream response stream: {}", error.without_url())
            })?;
            if chunk.is_empty() {
                continue;
            }
            let chunk_prefix = format!("{:X}\r\n", chunk.len());
            writer
                .write_all(chunk_prefix.as_bytes())
                .await
                .map_err(|error| format!("failed to write local HTTP response chunk: {error}"))?;
            writer
                .write_all(&chunk)
                .await
                .map_err(|error| format!("failed to write local HTTP response chunk: {error}"))?;
            writer
                .write_all(b"\r\n")
                .await
                .map_err(|error| format!("failed to write local HTTP response chunk: {error}"))?;
        }
        writer
            .write_all(b"0\r\n\r\n")
            .await
            .map_err(|error| format!("failed to finish local HTTP response: {error}"))?;
        writer
            .shutdown()
            .await
            .map_err(|error| format!("failed to close local HTTP response: {error}"))?;
        Ok(())
    };

    tokio::select! {
        result = relay => result,
        read_result = async {
            let mut extra = [0_u8; 1];
            loop {
                match reader.read(&mut extra).await {
                    Ok(0) => return Ok(()),
                    Ok(_) => return Err("unexpected data after local HTTP request headers".to_string()),
                    Err(error) => return Err(format!("failed to monitor local HTTP client: {error}")),
                }
            }
        } => read_result,
    }
}

/// 在上游尚未返回响应头时向本机 mitmproxy 返回一个固定错误响应。
async fn send_http_stream_error<W>(writer: &mut W, status: u16, reason: &str, message: &str)
where
    W: AsyncWrite + Unpin,
{
    let body = format!("{{\"error\":\"{message}\"}}");
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = writer.write_all(response.as_bytes()).await;
    let _ = writer.shutdown().await;
}

async fn run(req: &OutboundRequest) -> Result<OutboundResponse, String> {
    let response = send_request(req).await?;

    let status = response.status().as_u16();
    let mut out_headers = Vec::new();
    for (name, value) in response.headers().iter() {
        if let Ok(v) = value.to_str() {
            out_headers.push((name.as_str().to_string(), v.to_string()));
        }
    }

    let mut body = Vec::new();
    let mut first_body_at_ms = None;
    let mut first_output_at_ms = None;
    let mut timing_buffer = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("read body failed: {e}"))?;
        let observed_at_ms = unix_time_ms();
        record_first_body_at_ms(&mut first_body_at_ms, &chunk, observed_at_ms);
        record_first_output_at_ms(
            &mut first_output_at_ms,
            &mut timing_buffer,
            &chunk,
            observed_at_ms,
            false,
        );
        body.extend_from_slice(&chunk);
    }
    record_first_output_at_ms(
        &mut first_output_at_ms,
        &mut timing_buffer,
        &[],
        unix_time_ms(),
        true,
    );

    Ok(OutboundResponse {
        ok: true,
        request_id: req.request_id.clone(),
        source: req.source.clone(),
        status: Some(status),
        headers: out_headers,
        body_b64: Some(B64.encode(body)),
        first_body_at_ms,
        first_output_at_ms,
        error: None,
    })
}

/// 按既有 OAuth HTTP 参数构造 reqwest 请求并取得真实上游响应头。
async fn send_request(req: &OutboundRequest) -> Result<reqwest::Response, String> {
    let method_value = req
        .method
        .as_deref()
        .ok_or_else(|| "missing method".to_string())?;
    let url = req
        .url
        .as_deref()
        .ok_or_else(|| "missing url".to_string())?;
    let method = Method::from_bytes(method_value.as_bytes())
        .map_err(|e| format!("invalid method {}: {e}", method_value))?;

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

    let mut builder = client.request(method, url).headers(header_map);
    if !body_bytes.is_empty() {
        builder = builder.body(body_bytes);
    }

    let response = builder
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;

    Ok(response)
}
