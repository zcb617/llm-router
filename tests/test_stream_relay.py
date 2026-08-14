import asyncio

import httpx

from src import stream_relay


class _Request:
    method = "POST"

    def __init__(self, token):
        self.headers = {stream_relay.RELAY_TOKEN_HEADER: token}

    async def read(self):
        return b"{}"


class _UpstreamResponse:
    def __init__(self, status_code, body_chunks=(), body=b"", headers=None):
        self.status_code = status_code
        self.headers = headers or {"Content-Type": "text/event-stream"}
        self._body_chunks = list(body_chunks)
        self._body = body

    async def aread(self):
        return self._body

    async def aiter_raw(self):
        for chunk in self._body_chunks:
            yield chunk


class _MidstreamFailureResponse(_UpstreamResponse):
    async def aiter_raw(self):
        yield b"data: partial\n\n"
        raise httpx.ReadError("upstream disconnected")


class _StreamContext:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


class _Client:
    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []
        self.requests = []

    def stream(self, _method, url, **_kwargs):
        self.urls.append(url)
        self.requests.append((_method, url, _kwargs))
        return _StreamContext(self.responses.pop(0))


class _DownstreamResponse:
    def __init__(self, status, headers):
        self.status = status
        self.headers = headers
        self.chunks = []

    async def prepare(self, _request):
        return self

    async def write(self, chunk):
        self.chunks.append(chunk)

    async def write_eof(self):
        return None


def test_stream_relay_switches_after_quota_before_first_downstream_chunk(monkeypatch):
    downstream_responses = []

    def _stream_response(status, headers):
        response = _DownstreamResponse(status, headers)
        downstream_responses.append(response)
        return response

    monkeypatch.setattr(stream_relay.web, "StreamResponse", _stream_response)

    failures = []
    relay = stream_relay.StreamRelayServer(failures.append)
    relay._client = _Client(
        [
            _UpstreamResponse(429, body=b'{"error":"quota"}'),
            _UpstreamResponse(200, body_chunks=[b"data: from-second\n\n"]),
        ]
    )
    relay.register(
        "token",
        [
            {"upstream_id": 1, "url": "http://first.test/stream"},
            {"upstream_id": 2, "url": "http://second.test/stream"},
        ],
    )

    response = asyncio.run(relay._handle_stream(_Request("token")))

    assert response is downstream_responses[0]
    assert response.status == 200
    assert response.chunks == [b"data: from-second\n\n"]
    assert relay._client.urls == [
        "http://first.test/stream",
        "http://second.test/stream",
    ]
    assert failures == [1]


def test_stream_relay_switches_when_2xx_closes_before_first_chunk(monkeypatch):
    downstream_responses = []

    def _stream_response(status, headers):
        response = _DownstreamResponse(status, headers)
        downstream_responses.append(response)
        return response

    monkeypatch.setattr(stream_relay.web, "StreamResponse", _stream_response)

    failures = []
    relay = stream_relay.StreamRelayServer(failures.append)
    client = _Client(
        [
            _UpstreamResponse(200),
            _UpstreamResponse(200, body_chunks=[b"data: from-second\n\n"]),
        ]
    )
    relay._client = client
    relay.register(
        "token",
        [
            {"upstream_id": 1, "url": "http://first.test/stream"},
            {"upstream_id": 2, "url": "http://second.test/stream"},
        ],
    )

    response = asyncio.run(relay._handle_stream(_Request("token")))

    assert response.status == 200
    assert response.chunks == [b"data: from-second\n\n"]
    assert client.urls == [
        "http://first.test/stream",
        "http://second.test/stream",
    ]
    assert failures == [1]


def test_stream_relay_forwards_prepared_headers_and_body_to_selected_upstream(monkeypatch):
    downstream_responses = []

    def _stream_response(status, headers):
        response = _DownstreamResponse(status, headers)
        downstream_responses.append(response)
        return response

    monkeypatch.setattr(stream_relay.web, "StreamResponse", _stream_response)
    relay = stream_relay.StreamRelayServer()
    client = _Client([_UpstreamResponse(200, [b"data: ok\n\n"])])
    relay._client = client
    relay.register("token", [{
        "upstream_id": 2,
        "url": "https://api.example.com/v1/messages",
        "body": b'{"model":"target-model"}',
        "headers": {
            "Authorization": "Bearer sk-upstream",
            "anthropic-version": "2023-06-01",
            "X-Claude-Code-Session-Id": "client-session",
        },
    }])

    asyncio.run(relay._handle_stream(_Request("token")))

    method, url, kwargs = client.requests[0]
    assert method == "POST"
    assert url == "https://api.example.com/v1/messages"
    assert kwargs["content"] == b'{"model":"target-model"}'
    assert kwargs["headers"] == {
        "Authorization": "Bearer sk-upstream",
        "anthropic-version": "2023-06-01",
        "X-Claude-Code-Session-Id": "client-session",
    }
    assert downstream_responses[0].chunks == [b"data: ok\n\n"]


def test_stream_relay_does_not_switch_after_downstream_has_received_data(monkeypatch):
    downstream_responses = []

    def _stream_response(status, headers):
        response = _DownstreamResponse(status, headers)
        downstream_responses.append(response)
        return response

    monkeypatch.setattr(stream_relay.web, "StreamResponse", _stream_response)

    failures = []
    relay = stream_relay.StreamRelayServer(failures.append)
    client = _Client(
        [
            _MidstreamFailureResponse(200),
            _UpstreamResponse(200, body_chunks=[b"data: second\n\n"]),
        ]
    )
    relay._client = client
    relay.register(
        "token",
        [
            {"upstream_id": 1, "url": "http://first.test/stream"},
            {"upstream_id": 2, "url": "http://second.test/stream"},
        ],
    )

    try:
        asyncio.run(relay._handle_stream(_Request("token")))
    except httpx.ReadError:
        pass
    else:
        raise AssertionError("midstream upstream failure should end the current request")

    assert client.urls == ["http://first.test/stream"]
    assert downstream_responses[0].chunks == [b"data: partial\n\n"]
    assert failures == [1]
