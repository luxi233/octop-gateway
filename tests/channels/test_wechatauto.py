"""Unit tests for the WeChat wechatauto-bridge channel.

Tests cover:
- Config defaults and credential validation
- Channel initialization
- Inbound parsing (DM text, group mention, media file://, quoted, placeholders)
- Event dispatch filtering (is_self drop, id dedup)
- Outbound payload shape for /send and /send_file (mocked HTTP)

No network calls — aiohttp sessions and the BridgeClient are mocked.
"""

from __future__ import annotations

import base64

import aiohttp
import pytest

from octop_gateway.channel import ChannelCredentialsError
from octop_gateway.channels.wechatauto import WechatAutoChannel, WechatAutoConfig
from octop_gateway.channels.wechatauto.api import BridgeAPIError
from octop_gateway.models import (
    ChannelSubject,
    FileContent,
    ImageContent,
    InboundMessage,
    MessageEvent,
    TextContent,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SELF_WXID = "wxid_self0001"


def _make_config(**kwargs) -> WechatAutoConfig:
    return WechatAutoConfig(token="test_token", **kwargs)


async def _noop_processor(msg: InboundMessage):
    yield MessageEvent.completed()


def _make_channel(**kwargs) -> WechatAutoChannel:
    ch = WechatAutoChannel(processor=_noop_processor, config=_make_config(**kwargs))
    ch._self_wxid = _SELF_WXID
    return ch


def _event(**overrides) -> dict:
    ev = {
        "id": "wxid_peer1:1001",
        "chat_id": "wxid_peer1",
        "chat_type": "dm",
        "chat_name": "Peer One",
        "sender_id": "wxid_peer1",
        "sender_name": "Peer One",
        "is_self": False,
        "type": "text",
        "text": "hello",
        "timestamp": 1700000000.0,
        "local_id": 1,
        "sort_seq": 1001,
        "media_path": None,
        "at_usernames": None,
        "quoted": None,
    }
    ev.update(overrides)
    return ev


class _FakeResponse:
    def __init__(self, status: int = 200, payload: dict | None = None) -> None:
        self.status = status
        self._payload = payload or {}

    async def json(self, content_type=None):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    """Records requests and serves canned responses keyed by call order.

    Entries may be exceptions, which are raised instead of returned —
    used to simulate transport-level failures like a dead bridge.
    """

    def __init__(self, responses: list) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, str, dict]] = []
        self.closed = False

    def _next(self):
        r = self._responses.pop(0) if self._responses else _FakeResponse(500, {})
        if isinstance(r, BaseException):
            raise r
        return r

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self._next()

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self._next()

    async def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestWechatAutoConfig:
    def test_defaults(self) -> None:
        cfg = WechatAutoConfig(token="t")
        assert cfg.base_url == "http://127.0.0.1:18765"
        assert cfg.poll_timeout_ms == 30000
        assert cfg.verify_send is False

    def test_missing_token(self) -> None:
        assert WechatAutoConfig().missing_credentials() == ["token"]
        assert WechatAutoConfig(token="t").missing_credentials() == []

    def test_from_dict_ignores_unknown(self) -> None:
        cfg = WechatAutoConfig.from_dict({"token": " abc ", "junk": 1})
        assert cfg.token == "abc"


class TestWechatAutoChannel:
    def test_init(self) -> None:
        ch = WechatAutoChannel(processor=_noop_processor, config=_make_config())
        assert ch.channel_type == "wechatauto"

    def test_parse_dm_text(self) -> None:
        ch = _make_channel()
        msg = ch.parse_inbound(_event())
        assert msg.channel_type == "wechatauto"
        assert msg.channel_subject.subject_id == "wxid_peer1"
        assert msg.channel_subject.chat_type == "direct"
        assert msg.metadata["msg_id"] == "wxid_peer1:1001"
        assert msg.metadata["bot_mentioned"] is False
        assert len(msg.content) == 1
        assert isinstance(msg.content[0], TextContent)
        assert msg.content[0].text == "hello"

    def test_parse_group_mention(self) -> None:
        ch = _make_channel()
        ev = _event(
            id="room@chatroom:5",
            chat_id="room@chatroom",
            chat_type="group",
            chat_name="Test Group",
            sender_id="wxid_member",
            at_usernames=[_SELF_WXID, "wxid_other"],
        )
        msg = ch.parse_inbound(ev)
        assert msg.channel_subject.subject_id == "room@chatroom"
        assert msg.channel_subject.chat_type == "group"
        assert msg.metadata["sender_id"] == "wxid_member"
        assert msg.metadata["bot_mentioned"] is True

    def test_parse_group_no_mention(self) -> None:
        ch = _make_channel()
        ev = _event(chat_id="room@chatroom", chat_type="group", at_usernames=["wxid_other"])
        msg = ch.parse_inbound(ev)
        assert msg.metadata["bot_mentioned"] is False

    def test_parse_quoted_self_is_mention(self) -> None:
        ch = _make_channel()
        ev = _event(
            chat_id="room@chatroom",
            chat_type="group",
            type="quote",
            quoted={"sender": _SELF_WXID, "sender_name": "me", "text": "orig"},
        )
        msg = ch.parse_inbound(ev)
        assert msg.metadata["bot_mentioned"] is True
        assert msg.content[0].text.startswith("[quoted me: orig]")

    def test_parse_quoted_other_not_mention(self) -> None:
        ch = _make_channel()
        ev = _event(
            chat_id="room@chatroom",
            chat_type="group",
            type="quote",
            quoted={"sender": "wxid_member", "sender_name": "m", "text": "orig"},
        )
        msg = ch.parse_inbound(ev)
        assert msg.metadata["bot_mentioned"] is False

    def test_mention_alias_fallback_when_atlist_absent(self) -> None:
        """at_usernames absent (unreadable source) → fall back to @nick text."""
        ch = _make_channel()
        ch._self_nick = "乐儿"
        ev = _event(
            chat_id="room@chatroom", chat_type="group",
            at_usernames=None, text="@乐儿 在吗",
        )
        assert ch.parse_inbound(ev).metadata["bot_mentioned"] is True

    def test_mention_alias_requires_boundary(self) -> None:
        ch = _make_channel()
        ch._self_nick = "乐儿"
        ev = _event(
            chat_id="room@chatroom", chat_type="group",
            at_usernames=None, text="@乐儿呀 你看",
        )
        assert ch.parse_inbound(ev).metadata["bot_mentioned"] is False

    def test_mention_no_fallback_when_atlist_empty(self) -> None:
        """Present-but-empty at_usernames is a definitive 'not mentioned'."""
        ch = _make_channel()
        ch._self_nick = "乐儿"
        ev = _event(
            chat_id="room@chatroom", chat_type="group",
            at_usernames=[], text="@乐儿 在吗",
        )
        assert ch.parse_inbound(ev).metadata["bot_mentioned"] is False

    def test_parse_image_with_media_path(self, tmp_path) -> None:
        img = tmp_path / "pic.jpg"
        img.write_bytes(b"\xff\xd8fake")
        ch = _make_channel()
        msg = ch.parse_inbound(_event(type="image", text="[image]", media_path=str(img)))
        assert isinstance(msg.content[0], ImageContent)
        assert msg.content[0].url == f"file://{img}"

    def test_parse_image_lazy_media_url(self) -> None:
        """Undownloaded media gets a deferred wca-media:// URL, not a placeholder."""
        ch = _make_channel()
        ev = _event(
            type="image", text="[image]", media_path=None, local_id=42,
            chat_id="room@chatroom", chat_type="group",
        )
        msg = ch.parse_inbound(ev)
        assert isinstance(msg.content[0], ImageContent)
        assert msg.content[0].url == "wca-media://room@chatroom/42"

    async def test_fetch_remote_media_lazy_url(self, tmp_path) -> None:
        img = tmp_path / "shot.jpg"
        img.write_bytes(b"\xff\xd8real-bytes")
        ch = _make_channel()
        session = _FakeSession([_FakeResponse(200, {"ok": True, "path": str(img)})])
        ch._http = session  # type: ignore[attr-defined]
        data, mime = await ch.fetch_remote_media("wca-media://room@chatroom/42")
        assert data == b"\xff\xd8real-bytes"
        assert "image" in mime
        method, url, kwargs = session.calls[0]
        assert method == "GET" and url.endswith("/media")
        assert kwargs["params"]["local_id"] == "42"

    async def test_fetch_remote_media_lazy_undecryptable(self) -> None:
        ch = _make_channel()
        ch._http = _FakeSession([_FakeResponse(200, {"ok": False, "path": None})])  # type: ignore[attr-defined]
        with pytest.raises(BridgeAPIError):
            await ch.fetch_remote_media("wca-media://room@chatroom/99")

    def _mentioned_group_text(self) -> dict:
        return _event(
            chat_id="room@chatroom", chat_type="group",
            at_usernames=[_SELF_WXID], text="@me 这个是什么", type="text",
        )

    async def test_preprocess_promotes_context_image_on_mention(self) -> None:
        """@ mention in a group pulls the freshest context image onto the turn."""
        ch = _make_channel()
        ch._http = _FakeSession([_FakeResponse(200, {  # type: ignore[attr-defined]
            "lines": ["a: hi"],
            "media": [
                {"path": r"C:\tmp\old.jpg", "type": "image"},
                {"path": r"C:\tmp\new.jpg", "type": "image"},
                {"path": r"C:\tmp\v.mp4", "type": "video"},
            ],
        })])
        msg = ch.parse_inbound(self._mentioned_group_text())
        await ch._preprocess_inbound(msg)
        images = [p for p in msg.content if isinstance(p, ImageContent)]
        assert len(images) == 2
        # newest first
        assert images[0].url == "file://C:\\tmp\\new.jpg"

    async def test_preprocess_skips_unmentioned_group(self) -> None:
        ch = _make_channel()
        ch._http = _FakeSession([])  # type: ignore[attr-defined]
        ev = _event(chat_id="room@chatroom", chat_type="group",
                    at_usernames=["wxid_other"], text="hello")
        msg = ch.parse_inbound(ev)
        await ch._preprocess_inbound(msg)
        assert not any(isinstance(p, ImageContent) for p in msg.content)

    async def test_preprocess_skips_dm(self) -> None:
        ch = _make_channel()
        ch._http = _FakeSession([])  # type: ignore[attr-defined]
        msg = ch.parse_inbound(_event(at_usernames=[_SELF_WXID]))
        await ch._preprocess_inbound(msg)
        assert not any(isinstance(p, ImageContent) for p in msg.content)

    async def test_preprocess_context_failure_degrades(self) -> None:
        ch = _make_channel()
        ch._http = _FakeSession([_FakeResponse(500, {})])  # type: ignore[attr-defined]
        msg = ch.parse_inbound(self._mentioned_group_text())
        await ch._preprocess_inbound(msg)  # must not raise
        assert not any(isinstance(p, ImageContent) for p in msg.content)

    async def test_error_event_not_leaked_to_chat(self) -> None:
        """ERROR events must not send internal diagnostics to the user."""
        ch = _make_channel()
        ch._http = _FakeSession([_FakeResponse(200, {"ok": True})])  # type: ignore[attr-defined]
        subject = ChannelSubject(subject_id="room@chatroom", chat_type="group")
        await ch._deliver_event(subject, MessageEvent.error_event("模型调用失败：'main'"))
        _method, url, kwargs = ch._http.calls[0]
        assert url.endswith("/send")
        body = kwargs["json"]
        assert "'main'" not in body.get("text", "")
        assert "模型调用失败" not in body.get("text", "")

    def test_parse_media_without_path_or_id_falls_back_to_text(self) -> None:
        ch = _make_channel()
        msg = ch.parse_inbound(_event(type="image", text="[image]", media_path=None, local_id=None))
        assert isinstance(msg.content[0], TextContent)
        assert msg.content[0].text == "[image]"

    def test_parse_passthrough_inbound(self) -> None:
        ch = _make_channel()
        msg = ch.parse_inbound(_event())
        assert ch.parse_inbound(msg) is msg

    def test_dispatch_drops_self(self) -> None:
        ch = _make_channel()
        enqueued: list[dict] = []
        ch._enqueue_callback = enqueued.append
        ch._dispatch_event(_event(is_self=True))
        assert enqueued == []

    def test_dispatch_dedup(self) -> None:
        ch = _make_channel()
        enqueued: list[dict] = []
        ch._enqueue_callback = enqueued.append
        ch._dispatch_event(_event())
        ch._dispatch_event(_event())
        ch._dispatch_event(_event(id="wxid_peer1:1002", text="second"))
        assert [e["id"] for e in enqueued] == ["wxid_peer1:1001", "wxid_peer1:1002"]

    def test_session_id_fallback(self) -> None:
        ch = WechatAutoChannel(processor=_noop_processor, config=_make_config(), channel_id="cid1")
        msg = ch.parse_inbound(_event())
        assert msg.channel_session_id == "cid1-unconnected"


class TestWechatAutoOutbound:
    @pytest.mark.asyncio
    async def test_send_text_payload(self) -> None:
        session = _FakeSession([_FakeResponse(200, {"ok": True, "results": [{"ok": True}]})])
        ch = _make_channel()
        ch._http = session
        subject = ChannelSubject(subject_id="wxid_peer1", metadata={"chat_id": "wxid_peer1"})
        await ch._send_text(subject, "hi there")
        method, url, kwargs = session.calls[0]
        assert method == "POST"
        assert url.endswith("/send")
        assert kwargs["json"] == {"to": "wxid_peer1", "text": "hi there", "verify": False}
        assert kwargs["headers"]["Authorization"] == "Bearer test_token"

    @pytest.mark.asyncio
    async def test_send_text_group_target(self) -> None:
        session = _FakeSession([_FakeResponse(200, {"ok": True})])
        ch = _make_channel()
        ch._http = session
        subject = ChannelSubject(subject_id="room@chatroom", metadata={"chat_id": "room@chatroom"})
        await ch._send_text(subject, "group reply")
        assert session.calls[0][2]["json"]["to"] == "room@chatroom"

    @pytest.mark.asyncio
    async def test_send_text_failure_raises(self) -> None:
        session = _FakeSession([_FakeResponse(502, {"ok": False})])
        ch = _make_channel()
        ch._http = session
        subject = ChannelSubject(subject_id="wxid_peer1", metadata={})
        with pytest.raises(BridgeAPIError):
            await ch._send_text(subject, "x")

    @pytest.mark.asyncio
    async def test_send_media_writes_temp_file(self, tmp_path) -> None:
        session = _FakeSession([_FakeResponse(200, {"ok": True})])
        ch = _make_channel()
        ch._http = session
        media = ImageContent(data=base64.b64encode(b"\xff\xd8img").decode(), mime_type="image/jpeg")
        subject = ChannelSubject(subject_id="wxid_peer1", metadata={})
        await ch._send_media(subject, media)
        _method, url, kwargs = session.calls[0]
        assert url.endswith("/send_file")
        assert kwargs["json"]["to"] == "wxid_peer1"
        assert kwargs["json"]["image"] is True

    @pytest.mark.asyncio
    async def test_send_media_failure_falls_back_to_marker(self) -> None:
        session = _FakeSession(
            [
                _FakeResponse(502, {"ok": False}),  # send_file fails
                _FakeResponse(200, {"ok": True}),  # fallback text
            ]
        )
        ch = _make_channel()
        ch._http = session
        media = FileContent(data=base64.b64encode(b"pdf").decode(), filename="a.pdf")
        subject = ChannelSubject(subject_id="wxid_peer1", metadata={})
        await ch._send_media(subject, media)
        assert session.calls[1][2]["json"]["to"] == "wxid_peer1"
        assert "File" in session.calls[1][2]["json"]["text"]


class TestWechatAutoInboundMedia:
    @pytest.mark.asyncio
    async def test_fetch_remote_media_file_scheme(self, tmp_path) -> None:
        f = tmp_path / "in.jpg"
        f.write_bytes(b"\xff\xd8bytes")
        ch = _make_channel()
        data, mime = await ch.fetch_remote_media(f"file://{f}")
        assert data == b"\xff\xd8bytes"
        assert mime == "image/jpeg"


class TestWechatAutoStart:
    @pytest.mark.asyncio
    async def test_start_health_401_raises_credentials(self) -> None:
        session = _FakeSession([_FakeResponse(401, {"ok": False, "error": "unauthorized"})])
        ch = _make_channel()
        ch._http = session
        with pytest.raises(ChannelCredentialsError):
            await ch.start()
        assert ch._poll_task is None

    @pytest.mark.asyncio
    async def test_start_health_sets_session(self) -> None:
        session = _FakeSession([_FakeResponse(200, {"ok": True, "wxid": _SELF_WXID, "nickname": "me"})])
        ch = _make_channel()
        ch._http = session
        await ch.start()
        try:
            assert ch._connection_session.startswith(f"wca:{_SELF_WXID}:")
            assert ch._poll_task is not None
        finally:
            await ch.stop()


class TestBridgeSupervision:
    """bridge_cmd supervision: spawn on unreachable bridge, cooldown, 401 exempt."""

    @pytest.mark.asyncio
    async def test_start_spawns_bridge_when_unreachable(self) -> None:
        session = _FakeSession(
            [
                aiohttp.ClientError("conn refused"),  # initial health fails
                _FakeResponse(200, {"ok": True, "wxid": _SELF_WXID, "nickname": "me"}),
            ]
        )
        ch = _make_channel(bridge_cmd="fake-bridge --token t", bridge_startup_timeout=5.0)
        ch._http = session
        spawned: list[str] = []
        ch._launch_bridge = lambda cmd: spawned.append(cmd)  # type: ignore[method-assign]
        await ch.start()
        try:
            assert spawned == ["fake-bridge --token t"]
            assert ch._connection_session.startswith(f"wca:{_SELF_WXID}:")
            assert ch._poll_task is not None
        finally:
            await ch.stop()

    @pytest.mark.asyncio
    async def test_start_unreachable_without_bridge_cmd_raises(self) -> None:
        session = _FakeSession([aiohttp.ClientError("conn refused")])
        ch = _make_channel()
        ch._http = session
        spawned: list[str] = []
        ch._launch_bridge = lambda cmd: spawned.append(cmd)  # type: ignore[method-assign]
        with pytest.raises(aiohttp.ClientError):
            await ch.start()
        assert spawned == []
        assert ch._poll_task is None

    @pytest.mark.asyncio
    async def test_start_401_never_spawns(self) -> None:
        session = _FakeSession([_FakeResponse(401, {"ok": False, "error": "unauthorized"})])
        ch = _make_channel(bridge_cmd="fake-bridge")
        ch._http = session
        spawned: list[str] = []
        ch._launch_bridge = lambda cmd: spawned.append(cmd)  # type: ignore[method-assign]
        with pytest.raises(ChannelCredentialsError):
            await ch.start()
        assert spawned == []

    @pytest.mark.asyncio
    async def test_restart_bridge_recovers_and_cools_down(self) -> None:
        session = _FakeSession(
            [
                _FakeResponse(200, {"ok": True, "wxid": "w", "nickname": "n"}),
            ]
        )
        ch = _make_channel(bridge_cmd="fake-bridge", bridge_startup_timeout=5.0)
        ch._http = session
        ch._running = True
        spawned: list[str] = []
        ch._launch_bridge = lambda cmd: spawned.append(cmd)  # type: ignore[method-assign]
        health = await ch._restart_bridge(aiohttp.ClientError("x"))
        assert health is not None and health["wxid"] == "w"
        # Second call inside the cooldown window must not spawn again.
        assert await ch._restart_bridge(aiohttp.ClientError("x")) is None
        assert spawned == ["fake-bridge"]

    @pytest.mark.asyncio
    async def test_restart_bridge_spawn_timeout_returns_none(self) -> None:
        session = _FakeSession([])  # health would return 500 — never healthy
        ch = _make_channel(bridge_cmd="fake-bridge", bridge_startup_timeout=0.0)
        ch._http = session
        ch._running = True
        spawned: list[str] = []
        ch._launch_bridge = lambda cmd: spawned.append(cmd)  # type: ignore[method-assign]
        assert await ch._restart_bridge(aiohttp.ClientError("x")) is None
        assert spawned == ["fake-bridge"]
