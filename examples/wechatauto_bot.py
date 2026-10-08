"""微信 (WeChat via wechatauto bridge) AI Bot 示例.

Prerequisite: a running wechatauto-bridge on this host with WeChat 4.x logged in:
    python -m wechatauto_channels --token <token>   # http://127.0.0.1:18765

Usage:
    python examples/wechatauto_bot.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from backends.octop_harness import create_processor

from octop_gateway import ChannelManager, FileSystemMediaBackend
from octop_gateway.channels.wechatauto import WechatAutoConfig


async def main():
    media_backend = FileSystemMediaBackend("/tmp/octop-gateway/media")

    processor = create_processor(
        system_prompt="你是微信AI助手，回答友好简洁。",  # noqa: RUF001
        streaming=False,
        media_backend=media_backend,
    )

    manager = ChannelManager(processor=processor, workers_per_channel=4, media_backend=media_backend)
    await manager.start()

    channel_id = await manager.add_wechatauto_channel(
        WechatAutoConfig(
            base_url=os.environ.get("WECHATAUTO_BASE_URL", "http://127.0.0.1:18765"),
            token=os.environ["WECHATAUTO_TOKEN"],
        )
    )
    print(f"   channel_id: {channel_id}")
    print("🟢 微信 wechatauto Bot 在线! Ctrl+C 退出")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await manager.stop()
        print("⏹️  已停止")


if __name__ == "__main__":
    asyncio.run(main())
