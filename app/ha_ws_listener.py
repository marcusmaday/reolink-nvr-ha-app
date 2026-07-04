"""
ha_ws_listener.py

Background Home Assistant WebSocket listener for direct Watchtower event intake.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

import aiohttp

logger = logging.getLogger(__name__)


class HomeAssistantWebSocketListener:
    def __init__(
        self,
        *,
        websocket_url: str,
        access_token: str,
        on_state_changed: Callable[[dict], Awaitable[None]],
    ) -> None:
        self._websocket_url = websocket_url
        self._access_token = access_token
        self._on_state_changed = on_state_changed
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run_forever(self) -> None:
        retry_delay = 3.0
        while not self._stop_event.is_set():
            try:
                await self._run_once()
                retry_delay = 3.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Home Assistant WebSocket listener disconnected: %s", exc)

            if self._stop_event.is_set():
                break

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=retry_delay)
            except asyncio.TimeoutError:
                pass
            retry_delay = min(retry_delay * 1.5, 30.0)

    async def _run_once(self) -> None:
        headers = {"Authorization": f"Bearer {self._access_token}"}
        timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=None)

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.ws_connect(self._websocket_url, heartbeat=30) as ws:
                auth_required = await ws.receive_json()
                if auth_required.get("type") != "auth_required":
                    raise RuntimeError(f"Unexpected HA websocket handshake: {auth_required}")

                await ws.send_json({"type": "auth", "access_token": self._access_token})
                auth_result = await ws.receive_json()
                if auth_result.get("type") != "auth_ok":
                    raise RuntimeError(f"HA websocket authentication failed: {auth_result}")

                await ws.send_json({"id": 1, "type": "supported_features", "features": {"coalesce_messages": 1}})
                await self._read_until_result(ws, expected_id=1)

                await ws.send_json({"id": 2, "type": "subscribe_events", "event_type": "state_changed"})
                await self._read_until_result(ws, expected_id=2)
                logger.info("Home Assistant WebSocket event subscription is active")

                async for msg in ws:
                    if self._stop_event.is_set():
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                            raise RuntimeError(f"HA websocket closed: {msg.type}")
                        continue

                    raw_payload = json.loads(msg.data)
                    for payload in self._iter_messages(raw_payload):
                        if payload.get("type") != "event" or payload.get("id") != 2:
                            continue
                        event = payload.get("event") or {}
                        if event.get("event_type") != "state_changed":
                            continue
                        await self._on_state_changed(event)

    async def _read_until_result(self, ws: aiohttp.ClientWebSocketResponse, *, expected_id: int) -> None:
        while True:
            raw_msg = await ws.receive_json()
            for msg in self._iter_messages(raw_msg):
                if msg.get("id") != expected_id:
                    continue
                if msg.get("type") != "result" or not msg.get("success"):
                    raise RuntimeError(f"Unexpected HA websocket result for id {expected_id}: {msg}")
                return

    @staticmethod
    def _iter_messages(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            return [payload]
        if isinstance(payload, list):
            messages: list[dict[str, Any]] = []
            for item in payload:
                if isinstance(item, dict):
                    messages.append(item)
                else:
                    logger.debug("Ignoring unexpected non-dict Home Assistant websocket message item: %r", item)
            return messages
        logger.debug("Ignoring unexpected Home Assistant websocket payload type: %r", type(payload).__name__)
        return []
