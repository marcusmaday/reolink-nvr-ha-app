"""
settings_store.py

Persistent Watchtower app settings, including app-managed notification config.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


SUPPORTED_EVENT_TYPES = ["DOORBELL", "PERSON", "MOTION", "ANIMAL", "VEHICLE"]
DEFAULT_AI_EVENT_TYPES = ["DOORBELL", "PERSON", "ANIMAL", "VEHICLE"]


class NotificationRule(BaseModel):
    enabled: bool = True
    cooldown_seconds: int = 45
    title_template: str | None = None
    message_template: str | None = None


class DoorbellActionSettings(BaseModel):
    enabled: bool = False
    title: str = "Unlock Front Door"
    service: str = "lock.unlock"
    entity_id: str | None = None


class HomeAssistantCameraSourceSettings(BaseModel):
    person_entity_id: str | None = None
    doorbell_entity_id: str | None = None
    animal_entity_id: str | None = None
    vehicle_entity_id: str | None = None
    snapshot_camera_entity_id: str | None = None


class CameraAISettings(BaseModel):
    enabled: bool = False
    event_types: list[str] = Field(default_factory=lambda: list(DEFAULT_AI_EVENT_TYPES))


class KnownSubjectSettings(BaseModel):
    enabled: bool = True
    name: str = ""
    subject_type: str = "other"
    description: str = ""
    channels: list[int] = Field(default_factory=list)
    event_types: list[str] = Field(default_factory=list)


class AIEnrichmentSettings(BaseModel):
    enabled: bool = False
    provider: str = "openai"
    api_key: str | None = None
    model: str = "gpt-4.1-mini"
    detail: str = "low"
    timeout_seconds: int = 8
    confidence_threshold: float = 0.78
    daily_event_cap: int = 100
    include_fun_summary: bool = True
    fun_style: str = "playful"


class CameraNotificationSettings(BaseModel):
    channel: int
    camera_name: str | None = None
    enabled: bool = True
    notify_services: list[str] = Field(default_factory=list)
    rules: dict[str, NotificationRule] = Field(default_factory=dict)
    doorbell_action: DoorbellActionSettings = Field(default_factory=DoorbellActionSettings)
    ha_source: HomeAssistantCameraSourceSettings = Field(default_factory=HomeAssistantCameraSourceSettings)
    ai: CameraAISettings = Field(default_factory=CameraAISettings)


class ManagedNotificationSettings(BaseModel):
    enabled: bool = False
    default_notify_services: list[str] = Field(default_factory=list)
    preferred_test_service: str | None = None
    ai: AIEnrichmentSettings = Field(default_factory=AIEnrichmentSettings)
    known_subjects: list[KnownSubjectSettings] = Field(default_factory=list)
    cameras: list[CameraNotificationSettings] = Field(default_factory=list)


class WatchtowerSettings(BaseModel):
    notifications: ManagedNotificationSettings = Field(default_factory=ManagedNotificationSettings)


class SettingsStore:
    def __init__(self, path: str) -> None:
        self._path = Path(path)

    def load(self) -> WatchtowerSettings:
        if not self._path.exists():
            settings = WatchtowerSettings()
            self.save(settings)
            return settings

        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            return WatchtowerSettings.model_validate(raw)
        except Exception as exc:
            logger.warning("Failed to load settings from %s: %s", self._path, exc)
            settings = WatchtowerSettings()
            self.save(settings)
            return settings

    def save(self, settings: WatchtowerSettings) -> None:
        self._path.write_text(
            settings.model_dump_json(indent=2),
            encoding="utf-8",
        )

    def sync_notification_cameras(
        self,
        settings: WatchtowerSettings,
        *,
        channels: list[dict[str, Any]],
        participating_channels: set[int],
        allowed_event_types_by_channel: dict[int, set[str]],
    ) -> WatchtowerSettings:
        by_channel = {camera.channel: camera for camera in settings.notifications.cameras}
        synced: list[CameraNotificationSettings] = []

        for channel_info in channels:
            channel = int(channel_info["channel"])
            if channel not in participating_channels:
                continue

            existing = by_channel.get(channel)
            allowed_event_types = allowed_event_types_by_channel.get(channel, set(SUPPORTED_EVENT_TYPES))
            rule_map: dict[str, NotificationRule] = {}

            for event_type in SUPPORTED_EVENT_TYPES:
                current_rule = existing.rules.get(event_type) if existing else None
                if current_rule:
                    rule = current_rule.model_copy(deep=True)
                else:
                    default_cooldown = 0 if event_type == "DOORBELL" else 45
                    rule = NotificationRule(
                        enabled=event_type in allowed_event_types,
                        cooldown_seconds=default_cooldown,
                    )
                if event_type not in allowed_event_types:
                    rule.enabled = False
                rule_map[event_type] = rule

            synced.append(
                CameraNotificationSettings(
                    channel=channel,
                    camera_name=channel_info.get("name"),
                    enabled=existing.enabled if existing else True,
                    notify_services=list(existing.notify_services) if existing else [],
                    rules=rule_map,
                    doorbell_action=(
                        existing.doorbell_action.model_copy(deep=True)
                        if existing and existing.doorbell_action
                        else DoorbellActionSettings()
                    ),
                    ha_source=(
                        existing.ha_source.model_copy(deep=True)
                        if existing and existing.ha_source
                        else HomeAssistantCameraSourceSettings()
                    ),
                    ai=(
                        self._sync_camera_ai_settings(existing.ai, allowed_event_types)
                        if existing and existing.ai
                        else self._sync_camera_ai_settings(None, allowed_event_types)
                    ),
                )
            )

        settings.notifications.cameras = synced
        return settings

    @staticmethod
    def _sync_camera_ai_settings(
        existing: CameraAISettings | None,
        allowed_event_types: set[str],
    ) -> CameraAISettings:
        supported = [event_type for event_type in DEFAULT_AI_EVENT_TYPES if event_type in allowed_event_types]
        selected = [
            event_type
            for event_type in ((existing.event_types if existing else []) or supported)
            if event_type in supported
        ]
        if not selected and supported:
            selected = list(supported)
        return CameraAISettings(
            enabled=existing.enabled if existing else False,
            event_types=selected,
        )
