"""WeChat (wechatauto bridge) channel: long-poll over a local sidecar.

Uses the wechatauto-channels HTTP bridge (``wechatauto-bridge``) which owns
the native WeChat 4.x client on the host — SQLCipher DB reads for inbound,
UIA/OCR window driving for outbound. This adapter only speaks HTTP:

- GET ``/events`` long-poll for inbound messages (DM + group)
- POST ``/send`` / ``/send_file`` for outbound

The bridge already normalizes messages into a stable event dict
(``chat_id`` / ``chat_type`` / ``sender_*`` / ``at_usernames`` / ``quoted``),
including authoritative group-mention data, so ``parse_inbound`` is a thin
field mapping onto :class:`InboundMessage`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import mimetypes
import os
import re
import shlex
import subprocess
import tempfile
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

from octop_gateway.channel import BaseChannel, ChannelConfig, ChannelCredentialsError, MessageProcessor
from octop_gateway.channels.wechatauto.api import BridgeAPIError, BridgeClient
from octop_gateway.constraints import ChannelConstraints
from octop_gateway.models import (
    AudioContent,
    ChannelSubject,
    ContentPart,
    FileContent,
    ImageContent,
    InboundMessage,
    TextContent,
    VideoContent,
)

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "http://127.0.0.1:18765"
_DEFAULT_POLL_TIMEOUT_MS = 30000

# Event ``type`` values that carry a downloadable local file via media_path.
_MEDIA_TYPE_BY_EVENT = {
    "image": ImageContent,
    "video": VideoContent,
    "voice": AudioContent,
    "file": FileContent,
}

_SEEN_MAX = 1000

# Deferred-fetch scheme for inbound media the bridge has not decrypted yet.
# ``wca-media://<chat_id>/<local_id>`` is resolved in fetch_remote_media.
_LAZY_MEDIA_SCHEME = "wca-media://"

# Bridge supervision: respawn attempts need this many consecutive transport
# failures in the poll loop, and are rate-limited by the cooldown so a broken
# spawn command cannot loop forever.
_RESPAWN_AFTER_FAILURES = 3
_SPAWN_COOLDOWN_S = 60.0


@dataclass
class WechatAutoConfig(ChannelConfig):
    """wechatauto bridge channel configuration.

    Attributes:
        base_url: HTTP base of the running bridge (loopback only).
        token: Bearer token the bridge was started with.
        poll_timeout_ms: Long-poll idle window for ``GET /events``.
        verify_send: Ask the bridge to re-read the DB and confirm each send
            lands in the target chat. Off by default — the read-back uses a
            snapshot that lags while WeChat is actively writing.
        bridge_cmd: Optional command line used to spawn a local bridge when it
            is unreachable, e.g. ``wechatauto-bridge --token <token>``. Empty
            disables supervision and the channel only reports the failure.
        bridge_startup_timeout: Seconds to wait for a spawned bridge's first
            healthy ``/health`` response before giving up.
    """

    base_url: str = _DEFAULT_BASE_URL
    token: str = ""
    poll_timeout_ms: int = _DEFAULT_POLL_TIMEOUT_MS
    verify_send: bool = False
    bridge_cmd: str = ""
    bridge_startup_timeout: float = 30.0

    required_credentials = ("token",)


class WechatAutoChannel(BaseChannel):
    """WeChat channel driven by a local wechatauto bridge process.

    Requires a running ``wechatauto-bridge`` on the host with the WeChat
    client logged in. The bridge enforces a single-writer lock on the GUI
    send path, so outbound concurrency is safe by construction.
    """

    channel_type = "wechatauto"

    def __init__(
        self,
        processor: MessageProcessor,
        *,
        config: WechatAutoConfig,
        channel_id: str | None = None,
        tenant_id: str | None = None,
        debounce_seconds: float = 0.0,
        constraints: ChannelConstraints | None = None,
    ) -> None:
        super().__init__(
            processor,
            channel_id=channel_id,
            tenant_id=tenant_id,
            debounce_seconds=debounce_seconds,
            constraints=constraints,
            config=config,
        )
        self._config = config
        self._running = False
        self._poll_task: asyncio.Task[None] | None = None
        self._cursor = 0
        self._self_wxid = ""
        self._self_nick = ""
        self._connection_session: str | None = None
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._bridge_proc: subprocess.Popen[bytes] | None = None
        self._last_spawn_at = 0.0

    def _default_constraints(self) -> ChannelConstraints:
        return ChannelConstraints(
            reply_timeout=0,
            timeout_strategy="none",
            send_rate_limit=(5, 60.0),
            typing_keepalive_interval=0,
            show_thinking=False,
            show_tool_hints=False,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    # Note: bridge supervision (spawn-on-failure, cursor reset) rationale —
    # see .agents/notes/implemented/feature/2026-10-08-bridge-supervision.md
    async def start(self) -> None:
        """Verify bridge health, then start the events long-poll task."""
        self._running = True
        await self._ensure_http()
        api = await self._api()
        try:
            health = await api.health()
        except BridgeAPIError as exc:
            self._running = False
            if exc.status == 401:
                raise ChannelCredentialsError(self.channel_type, ["token"]) from exc
            raise
        except (aiohttp.ClientError, OSError) as exc:
            # Connection refused → the bridge process is down; respawn it when
            # bridge_cmd is configured. HTTP errors (401/5xx) mean the bridge
            # is alive and must not trigger a respawn.
            respawned = await self._restart_bridge(exc)
            if respawned is None:
                self._running = False
                raise
            health = respawned
        self._self_wxid = str(health.get("wxid") or "")
        self._self_nick = str(health.get("nickname") or "")
        self._connection_session = f"wca:{self._self_wxid}:{uuid.uuid4().hex[:8]}"
        self._poll_task = asyncio.create_task(self._poll_loop(), name=f"wechatauto-poll-{self._channel_id}")
        logger.info("WechatAutoChannel started: account=%s nick=%s", self._self_wxid, self._self_nick)

    async def stop(self) -> None:
        """Stop the poll task and release the HTTP session."""
        self._running = False
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None
        await self._close_http()
        logger.info("WechatAutoChannel stopped")

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    @staticmethod
    def _send_target(subject: ChannelSubject) -> str:
        meta = subject.metadata
        return str(meta.get("chat_id") or meta.get("to_handle") or subject.subject_id)

    async def _send_text(self, subject: ChannelSubject, text: str) -> None:
        """Send text through the bridge's UIA send path."""
        api = await self._api()
        await api.send_text(to=self._send_target(subject), text=text, verify=self._config.verify_send)

    async def _send_content(self, subject: ChannelSubject, parts: list[ContentPart]) -> None:
        """Send content parts in order."""
        for part in parts:
            if isinstance(part, TextContent) and part.text:
                await self._send_text(subject, part.text)
            elif isinstance(part, ImageContent | VideoContent | AudioContent | FileContent):
                await self._send_media(subject, part)

    async def _send_media(self, subject: ChannelSubject, media: ContentPart) -> None:
        """Send media via a temp file handed to the bridge's send_file."""
        sent = await self._deliver_media(subject, media)
        if not sent:
            await self._send_media_fallback(subject, media)

    async def _deliver_media(self, subject: ChannelSubject, media: ContentPart) -> bool:
        to = self._send_target(subject)
        try:
            data, mime = await self.load_media_bytes(media)
        except Exception:  # pylint: disable=broad-except
            logger.exception("WechatAutoChannel failed to load outbound media")
            return False
        if not data:
            return False

        ext = Path(media.filename).suffix if isinstance(media, FileContent) and media.filename else ""
        if not ext:
            ext = mimetypes.guess_extension(mime or "") or ".bin"
        tmp_path = ""
        try:
            fd, tmp_path = tempfile.mkstemp(prefix="wca_out_", suffix=ext)
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            api = await self._api()
            await api.send_file(
                to=to,
                path=tmp_path,
                image=isinstance(media, ImageContent),
                verify=self._config.verify_send,
            )
            logger.info("WechatAutoChannel media sent: type=%s to=%s", type(media).__name__, to[:16])
            return True
        except Exception:  # pylint: disable=broad-except
            logger.exception("WechatAutoChannel media send error")
            return False
        finally:
            if tmp_path:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_path)

    async def _send_media_fallback(self, subject: ChannelSubject, media: ContentPart) -> None:
        url = self._get_media_url(media)
        label = self._get_media_label(media)
        if url:
            await self._send_text(subject, f"[{label}: {url}]")
        elif getattr(media, "data", None) or getattr(media, "local_path", None):
            await self._send_text(subject, f"[{label} (local file)]")

    # ------------------------------------------------------------------
    # Inbound
    # ------------------------------------------------------------------

    async def fetch_remote_media(self, url: str) -> tuple[bytes, str]:
        """Read inbound media handed over by the bridge.

        ``file://`` paths are already-decrypted files; ``wca-media://``
        URLs ask the bridge to decrypt the message first (``GET /media``),
        then read the resulting local file.
        """
        if url.startswith(_LAZY_MEDIA_SCHEME):
            rest = url[len(_LAZY_MEDIA_SCHEME) :]
            chat, _, lid = rest.rpartition("/")
            api = await self._api()
            path = await api.download_media(chat=chat, local_id=int(lid))
            if not path:
                raise BridgeAPIError(f"bridge could not decrypt media: {url}")
            url = f"file://{path}"
        if url.startswith("file://"):
            path = url[len("file://") :]
            data = await asyncio.to_thread(Path(path).read_bytes)
            mime = mimetypes.guess_type(path)[0] or "application/octet-stream"
            return data, mime
        return await super().fetch_remote_media(url)

    def parse_inbound(self, raw_payload: Any) -> InboundMessage:
        """Parse a bridge event dict into an InboundMessage."""
        if isinstance(raw_payload, InboundMessage):
            return raw_payload

        ev = raw_payload if isinstance(raw_payload, dict) else {}
        chat_id = str(ev.get("chat_id") or "")
        is_group = str(ev.get("chat_type") or "") == "group"
        msg_type = str(ev.get("type") or "text")
        media_path = str(ev.get("media_path") or "")
        text = str(ev.get("text") or "")
        quoted = ev.get("quoted") if isinstance(ev.get("quoted"), dict) else None

        content: list[ContentPart] = []
        media_cls = _MEDIA_TYPE_BY_EVENT.get(msg_type)
        media_url = ""
        if media_cls is not None:
            if media_path:
                media_url = f"file://{media_path}"
            else:
                # Undownloaded media → deferred-fetch URL. The bridge decrypts
                # on demand via GET /media (post-gate lazy download); without
                # it the agent only ever saw "[image]" placeholders and had to
                # confabulate — observed 2026-10-08 as a shrimp photo answered
                # with a copper-price table.
                local_id = ev.get("local_id")
                if isinstance(local_id, int) and local_id > 0 and chat_id:
                    media_url = f"{_LAZY_MEDIA_SCHEME}{chat_id}/{local_id}"
        if media_url and media_cls is not None:
            kwargs: dict[str, Any] = {"url": media_url}
            if media_cls is FileContent and media_path:
                kwargs["filename"] = Path(media_path).name
            content.append(media_cls(**kwargs))
        elif text:
            if quoted:
                q_name = str(quoted.get("sender_name") or quoted.get("sender") or "")
                q_text = str(quoted.get("text") or "")[:200]
                text = f"[quoted {q_name}: {q_text}]\n{text}"
            content.append(TextContent(text=text))

        if not content:
            content.append(TextContent(text=f"[{msg_type}]"))

        session_id = self._connection_session or f"{self._channel_id}-unconnected"
        return InboundMessage(
            channel_id=self.channel_id,
            channel_type=self.channel_type,
            tenant_id=self._tenant_id,
            channel_subject=ChannelSubject(
                subject_id=chat_id,
                display_name=str(ev.get("chat_name") or ""),
                chat_type="group" if is_group else "direct",
                metadata={"chat_id": chat_id, "to_handle": chat_id},
            ),
            channel_session_id=session_id,
            content=content,
            metadata={
                "msg_id": str(ev.get("id") or ""),
                "chat_id": chat_id,
                "to_handle": chat_id,
                "sender_id": str(ev.get("sender_id") or ""),
                "sender_name": str(ev.get("sender_name") or ""),
                "bot_mentioned": self._is_mentioned(ev, quoted),
                "msg_type": msg_type,
            },
            timestamp=float(ev.get("timestamp") or time.time()),
        )

    def _is_mentioned(self, ev: dict[str, Any], quoted: dict[str, Any] | None) -> bool:
        """Authoritative group-mention check from bridge-projected fields.

        ``at_usernames`` comes from the message ``source`` XML ``<atuserlist>``
        (forgery-proof, unlike pasted ``@nick`` text); quoting the bot's own
        message counts as a mention, matching WeChat client semantics. When the
        field is absent entirely (unreadable source), fall back to matching a
        literal ``@nickname`` in the text — recall-favoring, same rule as V5's
        ``isSelfMentioned``: a present-but-empty list is a definitive "not
        mentioned" and must NOT fall back.
        """
        if not self._self_wxid:
            return False
        at_users = ev.get("at_usernames")
        if isinstance(at_users, list):
            if self._self_wxid in at_users:
                return True
        else:
            text = str(ev.get("text") or "")
            nick = self._self_nick.strip()
            # "@nick" must end at a boundary — "@李乐儿呀" is not a mention.
            if nick and re.search(r"@" + re.escape(nick) + r"(?=$|[^\w])", text):
                return True
        return bool(quoted and str(quoted.get("sender") or "") == self._self_wxid)

    def _is_duplicate(self, msg_id: str) -> bool:
        """OrderedDict LRU dedup on the bridge's stable event id."""
        if msg_id in self._seen:
            self._seen.move_to_end(msg_id)
            return True
        self._seen[msg_id] = None
        if len(self._seen) > _SEEN_MAX:
            self._seen.popitem(last=False)
        return False

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    async def _api(self) -> BridgeClient:
        session = await self._ensure_http()
        return BridgeClient(
            base_url=self._config.base_url,
            token=self._config.token,
            session=session,
            poll_timeout_ms=self._config.poll_timeout_ms,
        )

    async def _poll_loop(self) -> None:
        """Long-poll ``GET /events`` and enqueue each inbound event."""
        logger.info("WechatAutoChannel poll loop started: cursor=%d", self._cursor)
        failures = 0
        while self._running:
            try:
                api = await self._api()
                resp = await api.get_events(self._cursor)
                failures = 0
                next_cursor = resp.get("cursor")
                if isinstance(next_cursor, int):
                    self._cursor = next_cursor
                for item in resp.get("events") or []:
                    if not isinstance(item, dict):
                        continue
                    ev = item.get("event")
                    if not isinstance(ev, dict):
                        continue
                    self._dispatch_event(ev)
            except asyncio.CancelledError:
                break
            except TimeoutError:
                # Long-poll idle timeout is normal — loop again.
                continue
            except (aiohttp.ClientError, BridgeAPIError) as exc:
                failures += 1
                logger.warning("wechatauto poll failed: %s (%d)", exc, failures)
                if (
                    isinstance(exc, aiohttp.ClientError)
                    and failures >= _RESPAWN_AFTER_FAILURES
                    and await self._restart_bridge(exc) is not None
                ):
                    # A respawned bridge restarts its seq space — replay from
                    # cursor 0; the _seen LRU dedups already-dispatched events.
                    self._cursor = 0
                    failures = 0
                    continue
                await asyncio.sleep(min(2**failures, 60))
            except Exception:  # pylint: disable=broad-except
                # The poll loop must survive unexpected transport errors.
                failures += 1
                logger.exception("wechatauto poll error")
                await asyncio.sleep(min(2**failures, 60))

    # ------------------------------------------------------------------
    # Bridge supervision
    # ------------------------------------------------------------------

    def _launch_bridge(self, cmd: str) -> None:
        """Spawn the bridge detached so it survives this process's lifetime."""
        kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        args: str | list[str]
        if os.name == "nt":
            # Windows passes the raw command line to CreateProcess so quoted
            # paths work. CREATE_NO_WINDOW (not DETACHED_PROCESS): the bridge
            # exe is a packaged console app whose bootloader spawns a console
            # grandchild; DETACHED leaves it no console to inherit so it
            # allocates a visible one in the default terminal. A hidden
            # console on the bridge is inherited by the grandchild instead.
            # CREATE_BREAKAWAY_FROM_JOB keeps it out of our job object so
            # host teardown cannot take it down.
            args = cmd
            kwargs["creationflags"] = (
                getattr(subprocess, "CREATE_NO_WINDOW", 0)
                | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                | getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0)
            )
        else:
            args = shlex.split(cmd)
            kwargs["start_new_session"] = True
        self._bridge_proc = subprocess.Popen(args, **kwargs)
        logger.info("wechatauto bridge spawned: pid=%s", self._bridge_proc.pid)

    async def _wait_healthy(self) -> dict[str, Any] | None:
        """Poll ``/health`` until the bridge answers or the timeout expires."""
        deadline = time.monotonic() + self._config.bridge_startup_timeout
        while self._running and time.monotonic() < deadline:
            try:
                api = await self._api()
                return await api.health()
            except (aiohttp.ClientError, BridgeAPIError):
                await asyncio.sleep(1.0)
        return None

    async def _restart_bridge(self, cause: BaseException) -> dict[str, Any] | None:
        """Best-effort respawn of a dead bridge; returns health or None.

        ``None`` means supervision is disabled (no ``bridge_cmd``), the spawn
        is within cooldown, or the bridge never became healthy. The bridge is
        shared host infrastructure and is deliberately never killed — other
        consumers (Hermes adapter, CLI) may depend on it.
        """
        cmd = self._config.bridge_cmd
        now = time.monotonic()
        if not cmd or now - self._last_spawn_at < _SPAWN_COOLDOWN_S:
            return None
        self._last_spawn_at = now
        logger.warning("wechatauto bridge unreachable (%s); spawning: %s", cause, cmd)
        try:
            self._launch_bridge(cmd)
        except OSError:
            logger.exception("wechatauto bridge launch failed")
            return None
        return await self._wait_healthy()

    def _dispatch_event(self, ev: dict[str, Any]) -> None:
        """Filter and enqueue a single bridge event."""
        if ev.get("is_self"):
            return
        msg_id = str(ev.get("id") or "")
        if msg_id and self._is_duplicate(msg_id):
            return
        self.enqueue(ev)
