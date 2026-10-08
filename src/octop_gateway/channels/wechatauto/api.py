"""HTTP client for the wechatauto-channels local bridge.

The bridge (``wechatauto-bridge``, binds 127.0.0.1 only) owns all WeChat
client internals — SQLCipher DB reads, UIA/OCR sends — and exposes a small
Bearer-token HTTP API:

- ``GET  /health``                          → ``{ok, wxid, nickname, media}``
- ``GET  /events?cursor=N&timeout_ms=T``    → ``{cursor, events: [{seq, event}]}``
- ``GET  /media?chat=X&local_id=N``         → ``{ok, path}`` (on-demand decrypt)
- ``POST /send       {to, text, verify?}``  → ``{ok, results}``
- ``POST /send_file  {to, path, image?}``   → ``{ok, ...}``
"""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class BridgeAPIError(Exception):
    """Raised when the bridge returns a non-2xx response or ``ok: false``."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        self.status = status
        super().__init__(message)


class BridgeClient:
    """Async client bound to the channel's shared aiohttp session."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        session: aiohttp.ClientSession,
        poll_timeout_ms: int = 30000,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._session = session
        self._poll_timeout_ms = poll_timeout_ms

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"} if self._token else {}

    async def health(self) -> dict[str, Any]:
        """Return bridge health payload (``wxid`` / ``nickname`` / ``media``)."""
        async with self._session.get(f"{self._base_url}/health", headers=self._headers()) as resp:
            data = await resp.json(content_type=None)
            if resp.status != 200 or not data.get("ok"):
                raise BridgeAPIError(f"bridge health failed: {data}", status=resp.status)
            return data  # type: ignore[no-any-return]

    async def get_events(self, cursor: int, timeout_ms: int | None = None) -> dict[str, Any]:
        """Long-poll for inbound events after ``cursor``.

        The bridge idles up to ``timeout_ms`` and returns
        ``{"cursor": N, "events": [{"seq": N, "event": {...}}]}``.
        """
        wait_ms = timeout_ms if timeout_ms is not None else self._poll_timeout_ms
        url = f"{self._base_url}/events"
        params = {"cursor": str(cursor), "timeout_ms": str(wait_ms)}
        # Client timeout must exceed the long-poll window or the request aborts early.
        timeout = aiohttp.ClientTimeout(total=(wait_ms + 10000) / 1000.0)
        async with self._session.get(url, params=params, headers=self._headers(), timeout=timeout) as resp:
            data = await resp.json(content_type=None)
            if resp.status != 200:
                raise BridgeAPIError(f"events poll failed: {data}", status=resp.status)
            return data  # type: ignore[no-any-return]

    async def download_media(self, *, chat: str, local_id: int) -> str | None:
        """Ask the bridge to decrypt one media message; returns a local path.

        ``None`` means the media is no longer decryptable (WeChat image keys
        are transient — old attachments may be gone for good).
        """
        url = f"{self._base_url}/media"
        params = {"chat": chat, "local_id": str(local_id)}
        async with self._session.get(url, params=params, headers=self._headers()) as resp:
            data = await resp.json(content_type=None)
            if resp.status != 200:
                raise BridgeAPIError(f"media download failed: {data}", status=resp.status)
            path = data.get("path")
            return str(path) if path else None

    async def send_text(self, *, to: str, text: str, verify: bool = False) -> dict[str, Any]:
        """Send a text message through the bridge's UIA path."""
        payload = {"to": to, "text": text, "verify": verify}
        async with self._session.post(f"{self._base_url}/send", json=payload, headers=self._headers()) as resp:
            data = await resp.json(content_type=None)
            if resp.status != 200 or not data.get("ok"):
                raise BridgeAPIError(f"send_text failed: {data}", status=resp.status)
            return data  # type: ignore[no-any-return]

    async def send_file(self, *, to: str, path: str, image: bool = False, verify: bool = False) -> dict[str, Any]:
        """Send a file/image from a local path through the bridge."""
        payload = {"to": to, "path": path, "image": image, "verify": verify}
        async with self._session.post(f"{self._base_url}/send_file", json=payload, headers=self._headers()) as resp:
            data = await resp.json(content_type=None)
            if resp.status != 200 or not data.get("ok"):
                raise BridgeAPIError(f"send_file failed: {data}", status=resp.status)
            return data  # type: ignore[no-any-return]
