"""
ha_client.py

Lightweight Home Assistant Core API client for app-managed notifications.
"""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class HomeAssistantClientError(RuntimeError):
    """Raised when Home Assistant API calls fail."""


class HomeAssistantClient:
    def __init__(
        self,
        *,
        supervisor_token: str | None,
        base_url: str = "http://supervisor/core/api",
    ) -> None:
        self._supervisor_token = (supervisor_token or "").strip()
        self._base_url = base_url.rstrip("/")

    @property
    def enabled(self) -> bool:
        return bool(self._supervisor_token)

    @property
    def access_token(self) -> str:
        return self._supervisor_token

    @property
    def websocket_url(self) -> str:
        if self._base_url.startswith("http://"):
            return "ws://" + self._base_url[len("http://"):] + "/websocket"
        if self._base_url.startswith("https://"):
            return "wss://" + self._base_url[len("https://"):] + "/websocket"
        return self._base_url + "/websocket"

    async def list_mobile_app_notify_services(self) -> list[str]:
        if not self.enabled:
            return []

        services = await self._request("GET", "/services")
        notify_domain = next((item for item in services if item.get("domain") == "notify"), None)
        if not notify_domain:
            return []

        service_names: list[str] = []
        raw_services = notify_domain.get("services") or {}
        if isinstance(raw_services, dict):
            service_names = list(raw_services.keys())
        elif isinstance(raw_services, list):
            service_names = [str(name) for name in raw_services]

        return sorted(
            f"notify.{name}"
            for name in service_names
            if str(name).startswith("mobile_app_")
        )

    async def call_service(self, service_name: str, payload: dict[str, Any]) -> Any:
        if not self.enabled:
            raise HomeAssistantClientError("Home Assistant API access is not enabled for this app.")

        domain, _, service = service_name.partition(".")
        if not domain or not service:
            raise HomeAssistantClientError(f"Invalid Home Assistant service '{service_name}'")

        return await self._request("POST", f"/services/{domain}/{service}", json=payload)

    async def get_states(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        result = await self._request("GET", "/states")
        return result if isinstance(result, list) else []

    async def _request(self, method: str, path: str, json: dict[str, Any] | None = None) -> Any:
        if not self.enabled:
            raise HomeAssistantClientError("Missing SUPERVISOR_TOKEN")

        headers = {
            "Authorization": f"Bearer {self._supervisor_token}",
            "Content-Type": "application/json",
        }
        url = f"{self._base_url}{path}"
        timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_connect=10, sock_read=20)

        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.request(method, url, json=json) as resp:
                    if resp.status >= 400:
                        text = await resp.text()
                        raise HomeAssistantClientError(
                            f"Home Assistant API {method} {path} failed with {resp.status}: {text[:400]}"
                        )
                    if resp.content_type == "application/json":
                        return await resp.json()
                    return await resp.text()
        except aiohttp.ClientError as exc:
            logger.error("Home Assistant API request failed: %s", exc)
            raise HomeAssistantClientError(str(exc)) from exc
