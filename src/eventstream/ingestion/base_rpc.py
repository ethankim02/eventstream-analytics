"""Minimal read-only JSON-RPC client for Base.

This client only ever calls read-only JSON-RPC methods (eth_blockNumber,
eth_getLogs, eth_getBlockByNumber). It never constructs, signs, or
broadcasts a transaction, and never requires a private key.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import requests


class RpcError(RuntimeError):
    def __init__(self, code: int | None, message: str):
        self.code = code
        self.message = message
        super().__init__(f"RPC error {code}: {message}")


# JSON-RPC error codes / substrings observed on public Base endpoints that
# indicate the caller should back off and/or shrink the requested range,
# rather than a hard failure. See docs/DATA_SOURCE.md for sourcing notes;
# exact limits are not officially pinned down, so we treat them defensively.
_RATE_LIMIT_CODES = {-32016, -32005}
_RANGE_TOO_LARGE_HINTS = ("block range", "too many", "limit exceeded", "query returned more than")


@dataclass
class RpcClient:
    rpc_url: str
    timeout_seconds: int = 20
    max_retries: int = 5

    def _post(self, payload: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = requests.post(self.rpc_url, json=payload, timeout=self.timeout_seconds)
                resp.raise_for_status()
                body = resp.json()
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(min(2**attempt, 30))
                continue

            if "error" in body:
                err = body["error"]
                code = err.get("code")
                message = str(err.get("message", ""))
                if code in _RATE_LIMIT_CODES:
                    time.sleep(min(2**attempt, 30))
                    last_error = RpcError(code, message)
                    continue
                raise RpcError(code, message)
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
        """Batch-fetch UNIX timestamps for a set of block numbers."""
        unique = sorted(set(block_numbers))
        calls = [("eth_getBlockByNumber", [hex(n), False]) for n in unique]
        results = self._batch_call(calls)
        out: dict[int, int] = {}
        for n, block in zip(unique, results, strict=True):
            if block is None:
                continue
            out[n] = int(block["timestamp"], 16)
        return out

    def fetch_logs_chunked(
        self,
        address: str,
        topics: list[str | None],
        start_block: int,
        end_block: int,
        chunk_size: int,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield logs chunk-by-chunk, shrinking the chunk on range errors."""
        current = start_block
        size = max(1, chunk_size)
        while current <= end_block:
            upper = min(current + size - 1, end_block)
            try:
                logs = self.get_logs(address, topics, current, upper)
            except RpcError as exc:
                message = exc.message.lower()
                if any(hint in message for hint in _RANGE_TOO_LARGE_HINTS) and size > 1:
                    size = max(1, size // 2)
                    continue
                raise
            yield logs
            current = upper + 1
