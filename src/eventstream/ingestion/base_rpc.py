"""Minimal read-only JSON-RPC client for Base.

This client only ever calls read-only JSON-RPC methods (eth_blockNumber,
eth_getLogs, eth_getBlockByNumber). It never constructs, signs, or
broadcasts a transaction, and never requires a private key.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import requests


class RpcError(RuntimeError):
    def __init__(self, code: int | None, message: str):
        self.code = code
        self.message = message
        super().__init__(f"RPC error {code}: {message}")


# JSON-RPC error codes / substrings observed on public Base endpoints that
# indicate the caller should back off and/or shrink the requested range,
# rather than a hard failure. See docs/DATA_SOURCE.md for sourcing notes.
#
# Rate limits: back off and retry the same request.
_RATE_LIMIT_CODES = {-32016, -32005}
# Range / response-size limits: retrying the same request can never succeed,
# the caller must shrink the block range. Observed on mainnet.base.org:
#   -32614 "eth_getLogs is limited to a 2,000 range"  (HTTP 413)
#   -32020 "backend response too large"               (HTTP 500)
_RANGE_TOO_LARGE_CODES = {-32020, -32614}
_RANGE_TOO_LARGE_HINTS = (
    "block range",
    "too many",
    "limit exceeded",
    "query returned more than",
    "too large",
    "limited to a",
    "response size",
)


def is_range_too_large(code: int | None, message: str) -> bool:
    """True when an RPC error means "ask for fewer blocks", not "try again"."""
    if code in _RANGE_TOO_LARGE_CODES:
        return True
    lowered = message.lower()
    return any(hint in lowered for hint in _RANGE_TOO_LARGE_HINTS)


class AdaptiveChunker:
    """Thread-safe block-span controller shared by every worker of one run.

    Shrinks immediately when the provider rejects a span (halving it and
    backing the response-size target off, so the limit is not hit again
    straight away), and otherwise steers the span toward `target_logs` per
    request using the observed log density, growing gently. The target
    recovers a little on every success, up to its original value: without
    that, a handful of rejections during a long run would ratchet requests
    down to a fraction of what the provider accepts. Sharing one instance
    means a limit discovered by one worker benefits all of them.
    """

    def __init__(self, initial: int, maximum: int, target_logs: int = 16_000):
        self.maximum = max(1, maximum)
        self._base_target = target_logs
        self.target_logs = target_logs
        self._size = max(1, min(initial, self.maximum))
        self._lock = threading.Lock()

    @property
    def size(self) -> int:
        with self._lock:
            return self._size

    def on_success(self, span: int, n_logs: int) -> None:
        with self._lock:
            self.target_logs = min(self._base_target, int(self.target_logs * 1.02) + 1)
            ideal = self.maximum if n_logs == 0 else int(self.target_logs * span / n_logs)
            ideal = max(1, min(ideal, self.maximum))
            if ideal > self._size:
                self._size = min(ideal, max(self._size + 1, int(self._size * 1.25)))
            else:
                self._size = ideal

    def on_too_large(self, span: int) -> None:
        with self._lock:
            self._size = max(1, min(self._size, span // 2))
            self.target_logs = max(1_000, int(self.target_logs * 0.85))


@dataclass
class RpcClient:
    rpc_url: str
    timeout_seconds: int = 20
    max_retries: int = 5
    _session: requests.Session = field(default_factory=requests.Session, init=False, repr=False)

    @staticmethod
    def _backoff(attempt: int) -> None:
        time.sleep(min(2**attempt, 30) + random.uniform(0, 0.5))

    def _post(self, payload: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = self._session.post(self.rpc_url, json=payload, timeout=self.timeout_seconds)
            except requests.RequestException as exc:
                last_error = exc
                self._backoff(attempt)
                continue

            # Parse the body regardless of HTTP status: public endpoints report
            # range/size limits as HTTP 413/500 *with* a JSON-RPC error body, and
            # that body is what tells us to shrink rather than blindly retry.
            try:
                body = resp.json()
            except ValueError:
                body = None

            if isinstance(body, dict) and "error" in body:
                err = body["error"]
                code = err.get("code")
                message = str(err.get("message", ""))
                if is_range_too_large(code, message):
                    raise RpcError(code, message)
                if code in _RATE_LIMIT_CODES:
                    last_error = RpcError(code, message)
                    self._backoff(attempt)
                    continue
                raise RpcError(code, message)

            if resp.status_code == 429 or resp.status_code >= 500 or body is None:
                last_error = RuntimeError(f"HTTP {resp.status_code} without a JSON-RPC result")
                self._backoff(attempt)
                continue
            if resp.status_code >= 400:
                raise RpcError(None, f"HTTP {resp.status_code}: {resp.text[:200]}")
            return body["result"]
        raise RpcError(None, f"exhausted retries: {last_error}")

    def _call(self, method: str, params: list[Any]) -> Any:
        return self._post({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})

    def _batch_call(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        """Sequential JSON-RPC calls.

        JSON-RPC batch-array support is inconsistent across public/free RPC
        endpoints (some reject or silently mishandle it), so this
        deliberately issues one call per method rather than a single batched
        HTTP request — simpler and more portable than optimizing for a batch
        endpoint we can't rely on. See docs/DATA_SOURCE.md.
        """
        return [self._call(m, p) for m, p in calls]

    def get_block_number(self) -> int:
        return int(self._call("eth_blockNumber", []), 16)

    def get_chain_id(self) -> int:
        return int(self._call("eth_chainId", []), 16)

    def get_logs(
        self, address: str, topics: list[str | None], from_block: int, to_block: int
    ) -> list[dict[str, Any]]:
        params = [
            {
                "address": address,
                "topics": topics,
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
            }
        ]
        return self._call("eth_getLogs", params)

    def get_block_timestamps(self, block_numbers: list[int]) -> dict[int, int]:
        """Fetch UNIX timestamps for a set of block numbers (one call per block).

        Only a fallback: providers that return `blockTimestamp` on each log
        (mainnet.base.org does) make this unnecessary. A block the provider
        cannot return is an error, never silently dropped.
        """
        unique = sorted(set(block_numbers))
        calls = [("eth_getBlockByNumber", [hex(n), False]) for n in unique]
        results = self._batch_call(calls)
        out: dict[int, int] = {}
        for n, block in zip(unique, results, strict=True):
            if block is None:
                raise RpcError(None, f"provider returned no block for {n}")
            out[n] = int(block["timestamp"], 16)
        return out

    def fetch_logs_chunked(
        self,
        address: str,
        topics: list[str | None],
        start_block: int,
        end_block: int,
        chunk_size: int,
        chunker: AdaptiveChunker | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield logs chunk-by-chunk, adapting the span to provider limits.

        Every block in [start_block, end_block] is covered exactly once: a
        rejected span is retried smaller from the same starting block, and a
        failure that is not a range/size limit propagates rather than being
        skipped.
        """
        chunker = chunker or AdaptiveChunker(initial=chunk_size, maximum=chunk_size)
        current = start_block
        while current <= end_block:
            span = min(chunker.size, end_block - current + 1)
            upper = current + span - 1
            try:
                logs = self.get_logs(address, topics, current, upper)
            except RpcError as exc:
                if is_range_too_large(exc.code, exc.message) and span > 1:
                    chunker.on_too_large(span)
                    continue
                raise
            chunker.on_success(span, len(logs))
            yield logs
            current = upper + 1
