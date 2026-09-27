from unittest.mock import MagicMock, patch

import pytest

from eventstream.ingestion.base_rpc import (
    AdaptiveChunker,
    RpcClient,
    RpcError,
    is_range_too_large,
)

POST = "requests.Session.post"


def _response(json_body, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = str(json_body)
    if json_body is None:
        resp.json.side_effect = ValueError("no json")
    else:
        resp.json.return_value = json_body
    return resp


def test_get_block_number_parses_hex():
    client = RpcClient(rpc_url="http://fake")
    with patch(POST, return_value=_response({"jsonrpc": "2.0", "id": 1, "result": "0x2a"})):
        assert client.get_block_number() == 42


def test_rpc_error_raised_for_non_rate_limit_error():
    client = RpcClient(rpc_url="http://fake")
    body = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "bad params"}}
    with patch(POST, return_value=_response(body)):
        with pytest.raises(RpcError) as excinfo:
            client.get_block_number()
        assert excinfo.value.code == -32602


def test_rate_limit_error_is_retried_then_succeeds():
    client = RpcClient(rpc_url="http://fake", max_retries=3)
    rate_limited = _response(
        {"jsonrpc": "2.0", "id": 1, "error": {"code": -32016, "message": "over rate limit"}}
    )
    success = _response({"jsonrpc": "2.0", "id": 1, "result": "0x1"})
    with patch(POST, side_effect=[rate_limited, success]), patch("time.sleep"):
        assert client.get_block_number() == 1


@pytest.mark.parametrize(
    "status, code, message",
    [
        # Verbatim errors observed from https://mainnet.base.org during real ingestion.
        (413, -32614, "eth_getLogs is limited to a 2,000 range"),
        (500, -32020, "backend response too large"),
    ],
)
def test_range_limit_errors_surface_immediately_despite_http_error_status(status, code, message):
    """The JSON-RPC error body must be read even on HTTP 4xx/5xx, and never retried blindly."""
    client = RpcClient(rpc_url="http://fake", max_retries=5)
    body = {"jsonrpc": "2.0", "id": 1, "error": {"code": code, "message": message}}
    with (
        patch(POST, return_value=_response(body, status_code=status)) as post,
        patch("time.sleep"),
        pytest.raises(RpcError) as excinfo,
    ):
        client.get_logs("0xaddr", [None], 0, 5000)
    assert excinfo.value.code == code
    assert post.call_count == 1
    assert is_range_too_large(excinfo.value.code, excinfo.value.message)


def test_http_5xx_without_json_body_is_retried():
    client = RpcClient(rpc_url="http://fake", max_retries=3)
    bad_gateway = _response(None, status_code=502)
    success = _response({"jsonrpc": "2.0", "id": 1, "result": "0x9"})
    with patch(POST, side_effect=[bad_gateway, success]), patch("time.sleep"):
        assert client.get_block_number() == 9


def test_exhausted_retries_raise_rather_than_return_nothing():
    client = RpcClient(rpc_url="http://fake", max_retries=2)
    with (
        patch(POST, return_value=_response(None, status_code=503)),
        patch("time.sleep"),
        pytest.raises(RpcError, match="exhausted retries"),
    ):
        client.get_block_number()


@pytest.mark.parametrize(
    "code, message, expected",
    [
        (-32614, "eth_getLogs is limited to a 2,000 range", True),
        (-32020, "backend response too large", True),
        (-32000, "query returned more than 10000 results, block range too large", True),
        (-32602, "bad params", False),
        (-32016, "over rate limit", False),
        (None, "exhausted retries: timeout", False),
    ],
)
def test_is_range_too_large(code, message, expected):
    assert is_range_too_large(code, message) is expected


def test_fetch_logs_chunked_shrinks_range_on_error():
    client = RpcClient(rpc_url="http://fake")
    calls = []

    def fake_get_logs(address, topics, from_block, to_block):
        calls.append((from_block, to_block))
        if to_block - from_block + 1 > 250 and len(calls) == 1:
            raise RpcError(-32000, "query returned more than 10000 results, block range too large")
        return []

    client.get_logs = fake_get_logs  # type: ignore[method-assign]
    chunks = list(client.fetch_logs_chunked("0xaddr", [None], 0, 999, chunk_size=1000))
    assert len(calls) > 1
    assert calls[1][1] - calls[1][0] + 1 <= 500
    assert all(c == [] for c in chunks)


def test_fetch_logs_chunked_shrinks_on_base_public_rpc_error_codes():
    """Reproduces mainnet.base.org: a 2,000-block span cap and a response-size cap."""
    client = RpcClient(rpc_url="http://fake")
    rejected: list[int] = []
    accepted: list[tuple[int, int]] = []

    def fake_get_logs(address, topics, from_block, to_block):
        span = to_block - from_block + 1
        if span > 2000:
            rejected.append(span)
            raise RpcError(-32614, "eth_getLogs is limited to a 2,000 range")
        if span > 300:
            rejected.append(span)
            raise RpcError(-32020, "backend response too large")
        accepted.append((from_block, to_block))
        return []

    client.get_logs = fake_get_logs  # type: ignore[method-assign]
    list(client.fetch_logs_chunked("0xaddr", [None], 0, 9999, chunk_size=10_000))
    assert rejected[0] == 10_000  # first attempt used the requested size and was refused
    assert all(hi - lo + 1 <= 300 for lo, hi in accepted)
    covered = {b for lo, hi in accepted for b in range(lo, hi + 1)}
    assert covered == set(range(10_000))  # rejected spans left no gaps


def test_fetch_logs_chunked_does_not_swallow_unrelated_errors():
    client = RpcClient(rpc_url="http://fake")

    def fake_get_logs(address, topics, from_block, to_block):
        raise RpcError(-32602, "bad params")

    client.get_logs = fake_get_logs  # type: ignore[method-assign]
    with pytest.raises(RpcError):
        list(client.fetch_logs_chunked("0xaddr", [None], 0, 99, chunk_size=100))


def test_fetch_logs_chunked_covers_full_range():
    client = RpcClient(rpc_url="http://fake")
    seen_ranges = []

    def fake_get_logs(address, topics, from_block, to_block):
        seen_ranges.append((from_block, to_block))
        return []

    client.get_logs = fake_get_logs  # type: ignore[method-assign]
    list(client.fetch_logs_chunked("0xaddr", [None], 0, 249, chunk_size=100))
    covered = set()
    for lo, hi in seen_ranges:
        covered.update(range(lo, hi + 1))
    assert covered == set(range(0, 250))


def test_get_block_timestamps_raises_on_missing_block():
    client = RpcClient(rpc_url="http://fake")
    with (
        patch(POST, return_value=_response({"jsonrpc": "2.0", "id": 1, "result": None})),
        pytest.raises(RpcError, match="no block"),
    ):
        client.get_block_timestamps([123])


def test_chunker_shrinks_on_rejection_and_lowers_target():
    chunker = AdaptiveChunker(initial=1000, maximum=2000, target_logs=16_000)
    chunker.on_too_large(1000)
    assert chunker.size == 500
    assert chunker.target_logs < 16_000
    for _ in range(20):
        chunker.on_too_large(1)
    assert chunker.size == 1  # never below one block


def test_chunker_steers_toward_target_density_and_respects_maximum():
    chunker = AdaptiveChunker(initial=100, maximum=400, target_logs=16_000)
    for _ in range(50):  # sparse blocks: 10 logs/block -> ideal span far above the cap
        chunker.on_success(chunker.size, chunker.size * 10)
    assert chunker.size == 400
    chunker.on_success(400, 400 * 200)  # dense blocks: 200 logs/block -> ideal 80
    assert chunker.size == 80


def test_chunker_target_recovers_after_rejections_instead_of_ratcheting_down():
    chunker = AdaptiveChunker(initial=100, maximum=2000, target_logs=16_000)
    for _ in range(10):
        chunker.on_too_large(chunker.size * 2)
    assert chunker.target_logs < 16_000 * 0.25  # a burst of rejections backs the target off...
    for _ in range(400):
        chunker.on_success(chunker.size, chunker.size * 100)
    assert chunker.target_logs == 16_000  # ...and sustained success restores it in full
    assert chunker.size == 160  # 16,000 logs / 100 per block
