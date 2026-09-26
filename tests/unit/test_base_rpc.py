from unittest.mock import MagicMock, patch

from eventstream.ingestion.base_rpc import RpcClient, RpcError


def _response(json_body):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = json_body
    return resp


def test_get_block_number_parses_hex():
    client = RpcClient(rpc_url="http://fake")
    with patch(
        "requests.post", return_value=_response({"jsonrpc": "2.0", "id": 1, "result": "0x2a"})
    ):
        assert client.get_block_number() == 42


def test_rpc_error_raised_for_non_rate_limit_error():
    client = RpcClient(rpc_url="http://fake")
    body = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "bad params"}}
    with patch("requests.post", return_value=_response(body)):
        try:
            client.get_block_number()
            raise AssertionError("expected RpcError")
        except RpcError as exc:
            assert exc.code == -32602


def test_rate_limit_error_is_retried_then_succeeds():
    client = RpcClient(rpc_url="http://fake", max_retries=3)
    rate_limited = _response(
        {"jsonrpc": "2.0", "id": 1, "error": {"code": -32016, "message": "over rate limit"}}
    )
    success = _response({"jsonrpc": "2.0", "id": 1, "result": "0x1"})
    with patch("requests.post", side_effect=[rate_limited, success]), patch("time.sleep"):
        assert client.get_block_number() == 1


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
