"""WeChat (wechatauto bridge) channel package."""

from octop_gateway.channels.wechatauto.api import BridgeAPIError, BridgeClient
from octop_gateway.channels.wechatauto.channel import WechatAutoChannel, WechatAutoConfig

__all__ = [
    "BridgeAPIError",
    "BridgeClient",
    "WechatAutoChannel",
    "WechatAutoConfig",
]
