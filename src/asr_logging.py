"""ASR 请求日志：提取表单和文件信息，不保存音频字节。"""

import json
import time
import uuid
from datetime import datetime
from email.message import Message
from email.parser import BytesHeaderParser
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_SENSITIVE_HEADERS = {
    "authorization", "proxy-authorization", "x-api-key", "cookie", "set-cookie",
}
_SENSITIVE_FIELDS = {
    "api_key", "apikey", "access_token", "refresh_token", "token", "password",
    "secret", "client_secret", "authorization", "proxy_authorization", "x_api_key",
}


def _sensitive_field(name):
    return str(name).lower().replace("-", "_") in _SENSITIVE_FIELDS


def _safe_headers(headers):
    """保留头名称和普通头的值，隐藏认证信息。"""
    return {
        str(name): "[REDACTED]" if str(name).lower() in _SENSITIVE_HEADERS else str(value)
        for name, value in headers.items()
    }


def _safe_url(url):
    """路径中的网关密钥已由调用方剥离，这里隐藏 URL 用户信息和敏感参数。"""
    parts = urlsplit(str(url))
    netloc = parts.netloc.rsplit("@", 1)[-1]
    query = urlencode([
        (name, "[REDACTED]" if _sensitive_field(name) else value)
        for name, value in parse_qsl(parts.query, keep_blank_values=True)
    ])
    return urlunsplit((parts.scheme, netloc, parts.path, query, ""))


def _audio_format(prefix):
    """依据常见文件头识别容器；仅用于诊断，不能代替解码验证。"""
    if prefix.startswith(b"RIFF") and prefix[8:12] == b"WAVE":
        return "wav"
    if prefix.startswith(b"fLaC"):
        return "flac"
    if prefix.startswith(b"OggS"):
        return "ogg"
    if prefix.startswith(b"\x1a\x45\xdf\xa3"):
        return "webm/matroska"
    if prefix[4:8] == b"ftyp":
        return "mp4/m4a"
    if len(prefix) >= 2 and prefix[0] == 0xff and prefix[1] & 0xf6 == 0xf0:
        return "aac/adts"
    if prefix.startswith(b"ID3") or (len(prefix) >= 2 and prefix[0] == 0xff and prefix[1] & 0xe0 == 0xe0):
        return "mp3"
    return None


def _multipart_metadata(content, content_type):
    """按边界读取表单，文件只取长度和少量文件头，不复制或保存文件内容。"""
    result = {"size_bytes": len(content), "fields": [], "files": []}
    message = Message()
    message["Content-Type"] = content_type or ""
    boundary = message.get_param("boundary", header="content-type")
    if message.get_content_type().lower() != "multipart/form-data" or not boundary:
        if content:
            result["files"].append({
                "field": None, "filename": None, "content_type": content_type or None,
                "size_bytes": len(content), "detected_format": _audio_format(content[:16]),
            })
        result["parse_error"] = "request is not multipart/form-data with a boundary"
        return result

    try:
        marker = b"--" + boundary.encode("ascii")
    except UnicodeEncodeError:
        result["parse_error"] = "multipart boundary is not ASCII"
        return result
    first = content.find(marker)
    if first < 0 or (first and content[first - 2:first] != b"\r\n"):
        result["parse_error"] = "multipart boundary not found"
        return result
    cursor = first + len(marker)
    delimiter = b"\r\n" + marker
    while True:
        if content[cursor:cursor + 2] == b"--":
            return result
        if content[cursor:cursor + 2] != b"\r\n":
            result["parse_error"] = "invalid multipart boundary"
            return result
        headers_start = cursor + 2
        headers_end = content.find(b"\r\n\r\n", headers_start)
        if headers_end < 0:
            result["parse_error"] = "multipart part headers incomplete"
            return result
        part_headers = BytesHeaderParser().parsebytes(content[headers_start:headers_end] + b"\r\n\r\n")
        start = headers_end + 4
        end = content.find(delimiter, start)
        while end >= 0 and content[end + len(delimiter):end + len(delimiter) + 2] not in (b"\r\n", b"--"):
            end = content.find(delimiter, end + len(delimiter))
        if end < 0:
            result["parse_error"] = "multipart part is incomplete"
            return result

        name = part_headers.get_param("name", header="content-disposition")
        filename = part_headers.get_filename()
        declared_type = part_headers.get("Content-Type")
        media_type = part_headers.get_content_type().lower() if declared_type else ""
        prefix = content[start:min(end, start + 512)]
        detected_format = _audio_format(prefix[:16])
        binary = any(byte < 32 and byte not in (9, 10, 13) for byte in prefix)
        is_file = (
            filename is not None or str(name).lower() in {"file", "audio", "input_audio"}
            or media_type.startswith(("audio/", "video/")) or media_type == "application/octet-stream"
            or detected_format is not None or binary
        )
        if is_file:
            result["files"].append({
                "field": name, "filename": filename, "content_type": declared_type,
                "size_bytes": end - start, "detected_format": detected_format,
            })
        else:
            value = content[start:end].decode(part_headers.get_content_charset() or "utf-8", errors="replace")
            if _sensitive_field(name):
                value = "[REDACTED]"
            result["fields"].append({"name": name, "value": value})
        cursor = end + len(delimiter)


def capture_asr_request(request):
    """在转发和鉴权前捕获入站信息，返回不含音频的可持久化数据。"""
    content = request.content or b""
    content_type = request.headers.get("Content-Type", "")
    try:
        body = _multipart_metadata(content, content_type)
    except Exception:
        body = {"size_bytes": len(content), "fields": [], "files": [], "parse_error": "multipart parsing failed"}
    model = next((part["value"] for part in body["fields"] if part["name"] == "model"), None)
    return {
        "call_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "started_at": time.monotonic(),
        "url": _safe_url(request.url),
        "method": str(request.method),
        "request_headers": _safe_headers(request.headers),
        "request_body": json.dumps(body, ensure_ascii=False),
        "model": model,
    }


def asr_response_details(response):
    """记录响应状态、脱敏头和文本正文；异常的音频响应也不保存字节。"""
    if response is None:
        return None, {}, ""
    if isinstance(response, dict):
        status, headers, content = response.get("status"), response.get("headers", {}), response.get("content", b"")
    else:
        status, headers = response.status_code, response.headers
        try:
            content = response.content or b""
        except Exception as exc:
            return status, _safe_headers(headers), f"[response body unavailable: {type(exc).__name__}]"
    safe_headers = _safe_headers(headers)
    if isinstance(content, str):
        body = content
    elif str(headers.get("Content-Type", "")).lower().startswith("audio/") or _audio_format(content[:16]):
        body = f"[binary audio response omitted: {len(content)} bytes]"
    else:
        body = content.decode("utf-8", errors="replace")
    return status, safe_headers, body.replace("\x00", "\\u0000")
