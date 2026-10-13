"""WeChat (wechatauto bridge) channel package."""

from octop_gateway.channels.wechatauto.api import BridgeAPIError, BridgeClient
from octop_gateway.channels.wechatauto.channel import WechatAutoChannel, WechatAutoConfig

__all__ = [
    "BridgeAPIError",
    "BridgeClient",
    "WechatAutoChannel",
    "WechatAutoConfig",
]

# Vendored harness patches: self-apply when the channel package is loaded
# (idempotent); any failure must never block channel import.
try:
    from octop_gateway.channels.wechatauto._harness_patches import (
        apply as _apply_harness_patches,
    )

    _apply_harness_patches()
except Exception:
    pass
