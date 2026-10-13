"""Harness-side patches vendored inside the channel package.

随渠道包分发: 注入 Octop / 重装渠道时补丁随包落地; 本包被 import 时
自应用 (__init__.py 末尾调用), 等不到宿主升级才丢。manager 侧
watchdog 复用本模块做周期核查 -- 同一份补丁块, marker 幂等互认。

补丁清单一览:
  1. 越界降级 -- send_file_to_user 把 workspace 外路径抛 PermissionError
     穿透 langgraph 杀掉整轮 (用户端只见 "处理出错"), 降级为
     ToolException 让模型读错误文本自纠。
  2. 群语义 docstring -- 原文 "Send a file to the user" 被模型在群聊
     语境读成 "仅私聊" 而拒绝调用 (2026-10-12 两轮实锤), 改为
     "current chat (群/私聊皆可)"。
"""

from __future__ import annotations

import importlib.util
import logging
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger(__name__)

_BOUNDARY_MARKER = "PATCH(local)"
_BOUNDARY_ANCHOR = "        local = workspace.materialize_local(file_path)\n"
_BOUNDARY_BLOCK = (
    "        try:\n"
    "            local = workspace.materialize_local(file_path)\n"
    "        except (PermissionError, ValueError) as exc:\n"
    "            # PATCH(local): out-of-workspace paths used to let\n"
    "            # PermissionError escape langgraph and kill the whole\n"
    "            # stream (user only saw a generic error). Degrade to\n"
    "            # ToolException so the model reads the message, copies\n"
    "            # the file into the workspace, and retries.\n"
    "            raise ToolException(\n"
    '                f"send_file_to_user: {exc}. Copy the file into the "\n'
    '                "workspace first, then retry with the workspace path."\n'
    "            ) from exc\n"
)

_DOC_MARKER = "current chat"
_DOC_ANCHOR = '        """Send a file to the user through the chat channel.\n'
_DOC_BLOCK = (
    '        """Send a file to the current chat through the channel.\n'
    "\n"
    "        Delivers to whatever conversation triggered this turn - DMs\n"
    "        and group chats alike; in a group it is posted into the group.\n"
)


def _send_file_target() -> Path | None:
    """Locate send_file.py via the octop_harness package location -- more
    robust than composing a home-relative path; covers portable packages,
    venv installs, and dev checkouts alike."""
    spec = importlib.util.find_spec("octop_harness")
    locs = getattr(spec, "submodule_search_locations", None) if spec else None
    if not locs:
        return None
    p = Path(locs[0]) / "builtin" / "tools" / "send_file.py"
    return p if p.exists() else None


def apply(target: Path | None = None, log: Callable[[str], None] | None = None) -> str | None:
    """Apply all patches to send_file.py; return "a+b" desc or None.

    Marker-idempotent: applied blocks are skipped; an anchor mismatch
    (upstream rewrite) only logs a warning and never writes blindly.
    The patch lands on disk immediately, but an already-imported
    send_file module only picks it up after the next process restart.
    """
    emit = log or logger.info
    if target is None:
        target = _send_file_target()
    if target is None or not target.exists():
        return None
    text = target.read_text(encoding="utf-8")
    applied: list[str] = []
    if _BOUNDARY_MARKER not in text:
        if _BOUNDARY_ANCHOR not in text:
            emit("send_file patch: boundary anchor mismatch (upstream rewritten?) -- needs manual review")
        else:
            text = text.replace(_BOUNDARY_ANCHOR, _BOUNDARY_BLOCK, 1)
            applied.append("boundary-degrade")
    if _DOC_MARKER not in text:
        if _DOC_ANCHOR not in text:
            emit("send_file patch: docstring anchor mismatch (upstream rewritten?) -- needs manual review")
        else:
            text = text.replace(_DOC_ANCHOR, _DOC_BLOCK, 1)
            applied.append("group-semantics docstring")
    if not applied:
        return None
    target.write_text(text, encoding="utf-8")
    for stale in (target.parent / "__pycache__").glob("send_file*.pyc"):
        stale.unlink(missing_ok=True)
    desc = "+".join(applied)
    emit(f"send_file patch: {desc} (re)applied; effective after restart")
    return desc
