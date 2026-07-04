"""
Watchtower - FastAPI Backend

Camera event dashboard, clip playback, and live view for a Reolink NVR.
"""

import os
import html
import json
import logging
import asyncio
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Any
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse, urlencode

from fastapi import FastAPI, Query, HTTPException, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

from reolink_aio.api import Host
from reolink_aio.exceptions import ReolinkError

# Import our custom modules
from config import get_config, AppConfig
from nvr_client import NVRClient
from event_stream import EventStream, Event, EventType
from video_buffer import VideoBufferManager
from clip_generator import ClipGenerator, ClipMetadata
from timeline_index import TimelineIndex, TimelineEntry
from storage_manager import StorageManager
from rolling_buffer import RollingSegmentBuffer
from reolink_search import search_recordings, get_channels_info, EVENT_TYPE_MAP
from ai_enrichment import AIEnrichmentError, NotificationEnrichmentResult, OpenAIEnrichmentClient
from ha_client import HomeAssistantClient, HomeAssistantClientError
from ha_ws_listener import HomeAssistantWebSocketListener
from settings_store import (
    AIEnrichmentSettings,
    SUPPORTED_EVENT_TYPES,
    CameraNotificationSettings,
    CameraAISettings,
    DoorbellActionSettings,
    HomeAssistantCameraSourceSettings,
    KnownSubjectSettings,
    ManagedNotificationSettings,
    NotificationRule,
    SettingsStore,
    WatchtowerSettings,
)

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG", "false").lower() == "true" else logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

APP_NAME = "Watchtower"
APP_TAGLINE = "Recent camera events with player-first playback"
LIVE_PAGE_TITLE = "Watchtower"
APP_NAVIGATION_TARGET = "/app/15e0e6e5_watchtower"

APP_CONFIG: AppConfig = get_config()
if APP_CONFIG.api.debug:
    logging.getLogger().setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)

# ─── Config (from env / HA add-on options) ────────────────────────────────────
NVR_HOST = APP_CONFIG.nvr.host
NVR_PORT = APP_CONFIG.nvr.port
NVR_USERNAME = APP_CONFIG.nvr.username
NVR_PASSWORD = APP_CONFIG.nvr.password
NVR_SSL = APP_CONFIG.nvr.use_https
DEBUG = APP_CONFIG.api.debug
API_PORT = APP_CONFIG.api.port
API_HOST = APP_CONFIG.api.host
ALLOW_CORS = APP_CONFIG.api.allow_cors
LOCAL_CLIP_ENABLED = APP_CONFIG.video_buffer.enabled
CLIP_QUALITY = APP_CONFIG.video_buffer.clip_quality.lower()
LOCAL_CLIP_SECONDS = max(
    APP_CONFIG.video_buffer.clip_duration_before + APP_CONFIG.video_buffer.clip_duration_after,
    int(os.getenv("LOCAL_CLIP_SECONDS", "0")),
    1,
)
BUFFER_RETENTION_SECONDS = max(
    APP_CONFIG.video_buffer.buffer_size_seconds,
    LOCAL_CLIP_SECONDS + 20,
)
ABSOLUTE_MAX_BUFFER_AGE_SECONDS = max(int(os.getenv("ABSOLUTE_MAX_BUFFER_AGE_SECONDS", "300")), 60)
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")
ROLLING_SEGMENT_SECONDS = int(os.getenv("ROLLING_SEGMENT_SECONDS", "2"))
CLIP_DURATION_BEFORE = max(APP_CONFIG.video_buffer.clip_duration_before, 1)
CLIP_DURATION_AFTER = max(APP_CONFIG.video_buffer.clip_duration_after, 1)
BUFFER_CLIP_RETRY_ATTEMPTS = max(int(os.getenv("BUFFER_CLIP_RETRY_ATTEMPTS", "6")), 1)
BUFFER_CLIP_RETRY_DELAY_SECONDS = max(float(os.getenv("BUFFER_CLIP_RETRY_DELAY_SECONDS", "1")), 0.1)
ROLLING_BUFFER_MONITOR_INTERVAL_SECONDS = max(
    int(os.getenv("ROLLING_BUFFER_MONITOR_INTERVAL_SECONDS", "60")),
    30,
)
EVENT_DEDUPE_WINDOW_SECONDS = max(int(os.getenv("EVENT_DEDUPE_WINDOW_SECONDS", "8")), 1)
CLIPS_DIRECTORY = Path(APP_CONFIG.storage.clips_directory)
INDEX_FILE = APP_CONFIG.storage.index_file
SETTINGS_FILE = APP_CONFIG.storage.settings_file
RETENTION_DAYS = APP_CONFIG.storage.retention_days
MAX_STORAGE_MB = APP_CONFIG.storage.max_storage_mb
EXTERNAL_STORAGE_PATH = APP_CONFIG.storage.external_storage_path
WATCH_CHANNELS_CONFIG = APP_CONFIG.video_buffer.watch_channels
BUFFER_CHANNELS_CONFIG = APP_CONFIG.video_buffer.buffer_channels
DEFAULT_LIVE_CHANNEL_CONFIG = APP_CONFIG.video_buffer.default_live_channel
CAMERA_EVENT_TYPES_CONFIG = APP_CONFIG.video_buffer.camera_event_types

logger.info(
    "Clip timing configured: before=%ss after=%ss local_clip=%ss buffer_retention=%ss absolute_buffer_cap=%ss buffer_retries=%d delay=%ss dedupe_window=%ss watch_channels=%s buffer_channels=%s default_live_channel=%s camera_event_types=%s",
    CLIP_DURATION_BEFORE,
    CLIP_DURATION_AFTER,
    LOCAL_CLIP_SECONDS,
    BUFFER_RETENTION_SECONDS,
    ABSOLUTE_MAX_BUFFER_AGE_SECONDS,
    BUFFER_CLIP_RETRY_ATTEMPTS,
    BUFFER_CLIP_RETRY_DELAY_SECONDS,
    EVENT_DEDUPE_WINDOW_SECONDS,
    WATCH_CHANNELS_CONFIG,
    BUFFER_CHANNELS_CONFIG or "watch_channels",
    DEFAULT_LIVE_CHANNEL_CONFIG if DEFAULT_LIVE_CHANNEL_CONFIG is not None else "auto",
    CAMERA_EVENT_TYPES_CONFIG or "all",
)

# Global NVR host instance
nvr_host: Optional[Host] = None
timeline_index: Optional[TimelineIndex] = None
ui_clients: list[WebSocket] = []
clip_tasks: set[asyncio.Task] = set()
rolling_buffers: dict[int, RollingSegmentBuffer] = {}
rolling_buffer_monitor_task: Optional[asyncio.Task] = None
storage_manager: Optional[StorageManager] = None
available_channels: list[dict[str, Any]] = []
participating_channels: set[int] = set()
buffered_channels: set[int] = set()
default_live_channel: Optional[int] = None
allowed_event_types_by_channel: dict[int, set[str]] = {}
settings_store: Optional[SettingsStore] = None
watchtower_settings: WatchtowerSettings = WatchtowerSettings()
ha_client = HomeAssistantClient(supervisor_token=os.getenv("SUPERVISOR_TOKEN"))
notification_delivery_history: dict[tuple[int, str], datetime] = {}
ha_ws_listener_task: Optional[asyncio.Task] = None
ha_ws_listener: Optional[HomeAssistantWebSocketListener] = None
ha_state_event_history: dict[str, datetime] = {}
ai_enrichment_history: dict[str, int] = {}


# ─── Pydantic models ──────────────────────────────────────────────────────────

class Clip(BaseModel):
    timestamp:        str
    end_timestamp:    str
    duration_seconds: int
    event_type:       str
    trigger:          str
    file_name:        str
    stream_url:       Optional[str] = None
    download_url:     Optional[str] = None


class SearchResponse(BaseModel):
    channel:     int
    start_date:  str
    end_date:    str
    event_type:  Optional[str]
    clips:       List[Clip]
    total_clips: int


class ChannelInfo(BaseModel):
    channel: int
    name:    str
    enabled: bool
    model:   Optional[str] = None
    participating: bool = False
    buffered: bool = False
    default_live: bool = False
    allowed_event_types: list[str] = Field(default_factory=list)


class DeviceInfo(BaseModel):
    model:            str
    firmware_version: str
    nvr_name:         str
    mac_address:      str
    is_nvr:           bool
    num_channels:     int


class CameraSelectionInfo(BaseModel):
    available_channels: list[ChannelInfo]
    participating_channels: list[int]
    buffered_channels: list[int]
    default_live_channel: Optional[int] = None
    supported_event_types: list[str] = Field(default_factory=list)


class HomeAssistantStatus(BaseModel):
    enabled: bool
    discovered_mobile_notify_services: list[str] = Field(default_factory=list)
    websocket_listener_running: bool = False


class HomeAssistantEntitySummary(BaseModel):
    entity_id: str
    friendly_name: str
    domain: str
    state: str


class HomeAssistantEntityCatalog(BaseModel):
    binary_sensors: list[HomeAssistantEntitySummary] = Field(default_factory=list)
    cameras: list[HomeAssistantEntitySummary] = Field(default_factory=list)


class NotificationTestRequest(BaseModel):
    service: str
    title: str = "Watchtower Test"
    message: str = "Watchtower managed notifications are connected."


class NotificationConfigResponse(BaseModel):
    settings: ManagedNotificationSettings
    home_assistant: HomeAssistantStatus


class DoorbellActionRequest(BaseModel):
    channel: int
    event_id: Optional[str] = None


class HealthCheck(BaseModel):
    status:        str
    nvr_connected: bool
    nvr_host:      str


class EventIngestRequest(BaseModel):
    event_type: str
    channel: int
    timestamp: Optional[str] = None
    event_id: Optional[str] = None
    source: str = "home_assistant"
    title: Optional[str] = None
    message: Optional[str] = None
    camera_name: Optional[str] = None
    snapshot_url: Optional[str] = None
    clip_url: Optional[str] = None
    stream_url: Optional[str] = None
    download_url: Optional[str] = None
    live_url: Optional[str] = None
    duration_seconds: Optional[int] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RecentEvent(BaseModel):
    entry_id: str
    timestamp: str
    channel: int
    event_type: str
    clip_url: Optional[str] = None
    thumbnail_url: Optional[str] = None
    clip_status: Optional[str] = None
    title: Optional[str] = None
    message: Optional[str] = None
    camera_name: Optional[str] = None
    source: Optional[str] = None
    duration_seconds: Optional[int] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def _parse_channel_selection(raw_value: Optional[str], valid_channels: set[int], fallback: Optional[set[int]] = None) -> set[int]:
    if fallback is None:
        fallback = set(valid_channels)

    raw = (raw_value or "").strip()
    if not raw or raw.lower() == "all":
        return set(fallback)

    selected: set[int] = set()
    for chunk in raw.split(","):
        value = chunk.strip()
        if not value:
            continue
        try:
            channel = int(value)
        except ValueError:
            logger.warning("Ignoring invalid channel selection token '%s'", value)
            continue
        if channel not in valid_channels:
            logger.warning("Ignoring configured channel %s because it is not available on the NVR", channel)
            continue
        selected.add(channel)

    return selected or set(fallback)


def _channel_name(channel: int) -> Optional[str]:
    for info in available_channels:
        if info.get("channel") == channel:
            return info.get("name")
    if nvr_host:
        try:
            return nvr_host.camera_name(channel)
        except Exception:
            return None
    return None


def _sorted_channels(channels: set[int]) -> list[int]:
    return sorted(channels)


def _supported_event_types() -> list[str]:
    return list(SUPPORTED_EVENT_TYPES)


def _default_event_type_set() -> set[str]:
    return set(_supported_event_types())


def _sorted_event_types(event_types: set[str]) -> list[str]:
    order = {name: index for index, name in enumerate(_supported_event_types())}
    return sorted(event_types, key=lambda item: order.get(item, len(order)))


def _normalize_camera_name(name: Optional[str]) -> str:
    return (name or "").strip().casefold()


def _parse_camera_event_type_selection(
    raw_value: Optional[str],
    valid_channels: set[int],
) -> dict[int, set[str]]:
    default_event_types = _default_event_type_set()
    allowed: dict[int, set[str]] = {channel: set(default_event_types) for channel in valid_channels}

    raw = (raw_value or "").strip()
    if not raw or raw.lower() == "all":
        return allowed

    default_override: Optional[set[str]] = None
    channel_overrides: dict[int, set[str]] = {}

    for chunk in raw.split(";"):
        token = chunk.strip()
        if not token:
            continue
        if ":" not in token:
            logger.warning(
                "Ignoring invalid camera_event_types token '%s'. Expected format like 'all:PERSON,DOORBELL;1:PERSON,ANIMAL'.",
                token,
            )
            continue

        channel_token, event_tokens = token.split(":", 1)
        channel_key = channel_token.strip().lower()
        parsed_event_types: set[str] = set()
        for raw_event_type in event_tokens.split(","):
            normalized = _normalize_event_type(raw_event_type)
            if normalized is None:
                cleaned = raw_event_type.strip()
                if cleaned:
                    logger.warning("Ignoring invalid event type '%s' in camera_event_types", cleaned)
                continue
            parsed_event_types.add(normalized)

        if not parsed_event_types:
            logger.warning("Ignoring camera_event_types token '%s' because it did not contain any valid event types", token)
            continue

        if channel_key == "all":
            default_override = parsed_event_types
            continue

        try:
            channel = int(channel_key)
        except ValueError:
            logger.warning("Ignoring invalid channel '%s' in camera_event_types", channel_token.strip())
            continue
        if channel not in valid_channels:
            logger.warning("Ignoring camera_event_types override for unavailable channel %s", channel)
            continue
        channel_overrides[channel] = parsed_event_types

    if default_override is not None:
        allowed = {channel: set(default_override) for channel in valid_channels}

    for channel, event_types in channel_overrides.items():
        allowed[channel] = set(event_types)

    return allowed


def _channel_allowed_event_types(channel: int) -> set[str]:
    return allowed_event_types_by_channel.get(channel, _default_event_type_set())


def _channel_allows_event_type(channel: int, event_type: Optional[str]) -> bool:
    normalized = _normalize_event_type(event_type)
    if normalized is None:
        return False
    return normalized in _channel_allowed_event_types(channel)


def _sync_watchtower_settings() -> None:
    global watchtower_settings
    if not settings_store:
        return
    watchtower_settings = settings_store.sync_notification_cameras(
        watchtower_settings,
        channels=available_channels,
        participating_channels=participating_channels,
        allowed_event_types_by_channel=allowed_event_types_by_channel,
    )
    settings_store.save(watchtower_settings)


def _notification_camera_settings(channel: int) -> Optional[CameraNotificationSettings]:
    for camera in watchtower_settings.notifications.cameras:
        if camera.channel == channel:
            return camera
    return None


def _default_event_title_message(event_type: str, camera_name: str) -> tuple[str, str]:
    if event_type == "DOORBELL":
        return "Doorbell", "Someone is at the door."
    if event_type == "PERSON":
        return "Person Detected", f"Person detected at {camera_name}."
    if event_type == "ANIMAL":
        return "Animal Detected", f"Animal detected at {camera_name}."
    if event_type == "VEHICLE":
        return "Vehicle Detected", f"Vehicle detected at {camera_name}."
    return f"{event_type.title()} Detected", f"{event_type.title()} detected at {camera_name}."


def _camera_ha_source_settings(channel: int) -> Optional[HomeAssistantCameraSourceSettings]:
    camera_settings = _notification_camera_settings(channel)
    if not camera_settings:
        return None
    return camera_settings.ha_source


def _camera_ai_settings(channel: int) -> Optional[CameraAISettings]:
    camera_settings = _notification_camera_settings(channel)
    if not camera_settings:
        return None
    return camera_settings.ai


def _ai_settings() -> AIEnrichmentSettings:
    return watchtower_settings.notifications.ai


def _ai_api_key() -> str:
    settings_key = (_ai_settings().api_key or "").strip()
    env_key = os.getenv("OPENAI_API_KEY", "").strip()
    return settings_key or env_key


def _event_supports_ai(event_type: str) -> bool:
    return event_type in {"DOORBELL", "PERSON", "ANIMAL", "VEHICLE"}


def _normalize_subject_name(value: Optional[str]) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    return cleaned.casefold()


def _filtered_known_subjects(channel: int, event_type: str) -> list[KnownSubjectSettings]:
    subjects: list[KnownSubjectSettings] = []
    seen_names: set[str] = set()
    for subject in watchtower_settings.notifications.known_subjects:
        if not subject.enabled:
            continue
        name = (subject.name or "").strip()
        description = (subject.description or "").strip()
        if not name or not description:
            continue
        if subject.channels and channel not in subject.channels:
            continue
        if subject.event_types and event_type not in subject.event_types:
            continue
        normalized_name = _normalize_subject_name(name)
        if normalized_name in seen_names:
            continue
        seen_names.add(normalized_name)
        subjects.append(subject)
    return subjects


def _ai_event_count_key(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _ai_daily_cap_remaining() -> bool:
    settings = _ai_settings()
    cap = max(int(settings.daily_event_cap), 0)
    if cap <= 0:
        return True
    key = _ai_event_count_key(datetime.now(timezone.utc))
    return ai_enrichment_history.get(key, 0) < cap


def _record_ai_enrichment_attempt() -> None:
    key = _ai_event_count_key(datetime.now(timezone.utc))
    ai_enrichment_history[key] = ai_enrichment_history.get(key, 0) + 1


def _snapshot_url_to_local_path(snapshot_url: Optional[str]) -> Optional[Path]:
    raw = (snapshot_url or "").strip()
    if not raw:
        return None
    if raw.startswith("/config/"):
        return Path(raw)
    if raw.startswith("/local/"):
        return Path("/config/www") / raw[len("/local/"):]
    if raw.startswith("file://"):
        parsed = urlparse(raw)
        return Path(parsed.path)
    return None


async def _wait_for_snapshot_path(snapshot_path: Path, *, attempts: int = 5, delay_seconds: float = 0.2) -> bool:
    for _ in range(max(attempts, 1)):
        if snapshot_path.exists():
            return True
        await asyncio.sleep(max(delay_seconds, 0.05))
    return snapshot_path.exists()


async def _load_snapshot_bytes(channel: int, snapshot_url: Optional[str]) -> tuple[Optional[bytes], Optional[str]]:
    raw = (snapshot_url or "").strip()
    if not raw:
        return None, None

    snapshot_path = _snapshot_url_to_local_path(raw)
    if snapshot_path and await _wait_for_snapshot_path(snapshot_path):
        return snapshot_path.read_bytes(), snapshot_path.name

    if raw.startswith("/local/"):
        try:
            binary = await ha_client.fetch_bytes(raw)
            if binary:
                return binary, Path(raw).name
        except HomeAssistantClientError as exc:
            logger.info("Failed to fetch snapshot bytes for AI from Home Assistant at %s: %s", raw, exc)

    source = _camera_ha_source_settings(channel)
    snapshot_camera_entity_id = (source.snapshot_camera_entity_id or "").strip() if source else ""
    if snapshot_camera_entity_id:
        try:
            binary = await ha_client.fetch_camera_proxy_image(snapshot_camera_entity_id)
            if binary:
                logger.info(
                    "Loaded snapshot bytes for AI from camera proxy %s on channel %d",
                    snapshot_camera_entity_id,
                    channel,
                )
                return binary, f"{snapshot_camera_entity_id.replace('.', '_')}.jpg"
        except HomeAssistantClientError as exc:
            logger.info(
                "Failed to fetch camera proxy snapshot bytes for AI from %s: %s",
                snapshot_camera_entity_id,
                exc,
            )

    return None, snapshot_path.name if snapshot_path else Path(raw).name


def _compose_enriched_notification_message(
    *,
    enrichment: NotificationEnrichmentResult,
    confidence_threshold: float,
    include_fun_summary: bool,
) -> tuple[Optional[str], dict[str, str]]:
    known_subject_name = (enrichment.known_subject_name or "").strip()
    safe_summary = (enrichment.safe_summary or "").strip()
    fun_summary = (enrichment.fun_summary or "").strip()
    chosen_summary = fun_summary if include_fun_summary and fun_summary else safe_summary
    known_subject_confidence = float(enrichment.known_subject_confidence or 0.0)
    identity_confident = (
        known_subject_name
        and known_subject_name.casefold() != "unknown"
        and known_subject_confidence >= confidence_threshold
    )

    if identity_confident and known_subject_name and chosen_summary:
        if known_subject_name.casefold() not in chosen_summary.casefold():
            if safe_summary and include_fun_summary and fun_summary:
                chosen_summary = f"{known_subject_name} may be in view. {fun_summary}"
            else:
                chosen_summary = f"{known_subject_name} may be in view. {chosen_summary}"
    elif not chosen_summary:
        return None, {}

    return chosen_summary, {
        "ai_summary": safe_summary,
        "ai_fun_summary": fun_summary,
        "known_subject_name": known_subject_name if identity_confident else "",
        "primary_subject": (enrichment.primary_subject or "").strip(),
        "activity": (enrichment.activity or "").strip(),
    }


def _entity_mapping_for_state_change(entity_id: str) -> Optional[tuple[int, str]]:
    normalized = (entity_id or "").strip().lower()
    if not normalized:
        return None

    for channel in _sorted_channels(participating_channels):
        source = _camera_ha_source_settings(channel)
        if not source:
            continue
        mappings = {
            "PERSON": source.person_entity_id,
            "DOORBELL": source.doorbell_entity_id,
            "ANIMAL": source.animal_entity_id,
            "VEHICLE": source.vehicle_entity_id,
        }
        for event_type, mapped_entity_id in mappings.items():
            if (mapped_entity_id or "").strip().lower() == normalized:
                return channel, event_type
    return None


async def _capture_snapshot_for_channel(channel: int, event_type: str, event_timestamp: datetime) -> Optional[str]:
    source = _camera_ha_source_settings(channel)
    snapshot_camera_entity_id = (source.snapshot_camera_entity_id or "").strip() if source else ""
    if not snapshot_camera_entity_id:
        return None

    file_stamp = event_timestamp.strftime("%Y%m%dT%H%M%S")
    file_name = f"watchtower_{channel}_{event_type.lower()}_{file_stamp}.jpg"
    snapshot_file = f"/config/www/tmp/{file_name}"
    snapshot_url = f"/local/tmp/{file_name}"

    try:
        await ha_client.call_service(
            "camera.snapshot",
            {
                "entity_id": snapshot_camera_entity_id,
                "filename": snapshot_file,
            },
        )
        await asyncio.sleep(0.5)
    except HomeAssistantClientError as exc:
        logger.warning("Failed to capture Home Assistant snapshot for channel %d: %s", channel, exc)
        return None

    return snapshot_url


def _render_notification_text(
    template: Optional[str],
    fallback: str,
    *,
    entry: TimelineEntry,
    extra_context: Optional[dict[str, str]] = None,
) -> str:
    if not template:
        return fallback

    metadata = entry.metadata or {}
    context = extra_context or {}
    try:
        return template.format(
            camera_name=metadata.get("camera_name") or _channel_name(entry.channel) or f"Channel {entry.channel}",
            event_type=entry.event_type,
            channel=entry.channel,
            timestamp=entry.timestamp.isoformat(),
            title=metadata.get("title") or fallback,
            message=metadata.get("message") or fallback,
            ai_summary=context.get("ai_summary", ""),
            ai_fun_summary=context.get("ai_fun_summary", ""),
            known_subject_name=context.get("known_subject_name", ""),
            primary_subject=context.get("primary_subject", ""),
            activity=context.get("activity", ""),
        )
    except Exception as exc:
        logger.warning("Failed to render notification template '%s': %s", template, exc)
        return fallback


def _build_managed_app_event_url(entry: TimelineEntry) -> str:
    raw_base = APP_NAVIGATION_TARGET
    if raw_base.startswith("homeassistant://"):
        base = raw_base
    elif "://" in raw_base:
        host_and_path = raw_base.split("://", 1)[1]
        base = f"homeassistant://navigate/{host_and_path.split('/', 1)[1]}" if "/" in host_and_path else "homeassistant://navigate/"
    elif raw_base.startswith("/"):
        base = f"homeassistant://navigate{raw_base}"
    else:
        base = f"homeassistant://navigate/{raw_base}"
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}{urlencode({'event_id': entry.entry_id})}"


def _camera_doorbell_action(channel: int) -> Optional[DoorbellActionSettings]:
    camera_settings = _notification_camera_settings(channel)
    if not camera_settings:
        return None
    action = camera_settings.doorbell_action
    if not action.enabled or not action.service.strip() or not (action.entity_id or "").strip():
        return None
    return action


def _build_managed_unlock_url(channel: int, entry_id: Optional[str] = None) -> str:
    base = "/app/doorbell-action"
    query = {"channel": channel}
    if entry_id:
        query["event_id"] = entry_id
    return f"{base}?{urlencode(query)}"


async def _execute_doorbell_action(channel: int, event_id: Optional[str] = None) -> dict[str, Any]:
    if not ha_client.enabled:
        raise HTTPException(status_code=503, detail="Home Assistant API access is not enabled for Watchtower.")
    if not watchtower_settings.notifications.enabled:
        raise HTTPException(status_code=409, detail="Watchtower-managed notifications are disabled.")

    action = _camera_doorbell_action(channel)
    if not action:
        raise HTTPException(status_code=404, detail=f"No doorbell action is configured for channel {channel}.")

    payload = {"entity_id": action.entity_id.strip()}
    try:
        await ha_client.call_service(action.service.strip(), payload)
    except HomeAssistantClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    logger.info(
        "Executed doorbell action for channel %d via %s (%s) event=%s",
        channel,
        action.service,
        action.entity_id,
        event_id or "n/a",
    )
    return {
        "status": "executed",
        "channel": channel,
        "service": action.service.strip(),
        "entity_id": action.entity_id.strip(),
        "event_id": event_id,
        "title": action.title,
    }


async def _maybe_enrich_notification(entry: TimelineEntry) -> tuple[Optional[str], dict[str, str]]:
    settings = _ai_settings()
    if not settings.enabled:
        logger.debug("AI enrichment skipped for %s: globally disabled", entry.entry_id)
        return None, {}
    if settings.provider.strip().lower() != "openai":
        logger.info("AI enrichment skipped for %s: unsupported provider '%s'", entry.entry_id, settings.provider)
        return None, {}
    if not _event_supports_ai(entry.event_type):
        logger.debug("AI enrichment skipped for %s: unsupported event type %s", entry.entry_id, entry.event_type)
        return None, {}
    if not _ai_daily_cap_remaining():
        logger.info("AI enrichment skipped for %s: daily cap reached", entry.entry_id)
        return None, {}

    camera_ai = _camera_ai_settings(entry.channel)
    if not camera_ai or not camera_ai.enabled or entry.event_type not in (camera_ai.event_types or []):
        logger.info(
            "AI enrichment skipped for %s: camera %d is not configured for AI on %s",
            entry.entry_id,
            entry.channel,
            entry.event_type,
        )
        return None, {}

    api_key = _ai_api_key()
    if not api_key:
        logger.info("AI enrichment skipped for %s: no OpenAI API key configured", entry.entry_id)
        return None, {}

    metadata = entry.metadata or {}
    snapshot_url = metadata.get("snapshot_url") or entry.thumbnail_path
    if not snapshot_url:
        logger.info("AI enrichment skipped for %s: no snapshot path available", entry.entry_id)
        return None, {}
    snapshot_bytes, snapshot_name = await _load_snapshot_bytes(entry.channel, snapshot_url)
    if not snapshot_bytes:
        snapshot_path = _snapshot_url_to_local_path(snapshot_url)
        logger.info(
            "AI enrichment skipped for %s: snapshot bytes could not be loaded from %s",
            entry.entry_id,
            snapshot_path or snapshot_url,
        )
        return None, {}

    known_subjects = _filtered_known_subjects(entry.channel, entry.event_type)
    camera_name = metadata.get("camera_name") or _channel_name(entry.channel) or f"Channel {entry.channel}"
    logger.info(
        "Attempting AI enrichment for %s with model %s and %d known subject(s)",
        entry.entry_id,
        settings.model.strip() or "gpt-4.1-mini",
        len(known_subjects),
    )

    try:
        _record_ai_enrichment_attempt()
        enrichment = await OpenAIEnrichmentClient(
            api_key=api_key,
            timeout_seconds=settings.timeout_seconds,
        ).analyze_snapshot(
            image_bytes=snapshot_bytes,
            image_name=snapshot_name,
            event_type=entry.event_type,
            camera_name=camera_name,
            settings=settings,
            known_subjects=known_subjects,
        )
    except AIEnrichmentError as exc:
        logger.warning("AI enrichment skipped for %s: %s", entry.entry_id, exc)
        return None, {}
    except Exception as exc:
        logger.warning("Unexpected AI enrichment error for %s: %s", entry.entry_id, exc)
        return None, {}

    message, context = _compose_enriched_notification_message(
        enrichment=enrichment,
        confidence_threshold=max(min(float(settings.confidence_threshold), 1.0), 0.0),
        include_fun_summary=settings.include_fun_summary and settings.fun_style != "off",
    )
    logger.info(
        "AI enrichment completed for %s: known_subject=%s primary_subject=%s",
        entry.entry_id,
        (enrichment.known_subject_name or "unknown").strip() or "unknown",
        (enrichment.primary_subject or "unknown").strip() or "unknown",
    )
    return message, context


async def _send_managed_notifications(entry: TimelineEntry) -> None:
    if not ha_client.enabled or not watchtower_settings.notifications.enabled:
        return

    camera_settings = _notification_camera_settings(entry.channel)
    if not camera_settings or not camera_settings.enabled:
        return

    rule = camera_settings.rules.get(entry.event_type)
    if not rule or not rule.enabled:
        return

    notify_services = camera_settings.notify_services or watchtower_settings.notifications.default_notify_services
    if not notify_services:
        logger.info("Managed notifications enabled but no notify services are configured for channel %d", entry.channel)
        return

    now = datetime.now()
    history_key = (entry.channel, entry.event_type)
    last_sent = notification_delivery_history.get(history_key)
    cooldown_seconds = max(int(rule.cooldown_seconds), 0)
    if last_sent and (now - last_sent).total_seconds() < cooldown_seconds:
        logger.info(
            "Skipping managed notification for %s due to cooldown (%ss)",
            entry.entry_id,
            cooldown_seconds,
        )
        return

    metadata = entry.metadata or {}
    enriched_message, enrichment_context = await _maybe_enrich_notification(entry)
    title = _render_notification_text(
        rule.title_template,
        metadata.get("title") or f"{entry.event_type.title()} detected",
        entry=entry,
        extra_context=enrichment_context,
    )
    message = _render_notification_text(
        rule.message_template,
        enriched_message or metadata.get("message") or f"{entry.event_type.title()} detected",
        entry=entry,
        extra_context=enrichment_context,
    )
    snapshot_url = metadata.get("snapshot_url") or entry.thumbnail_path
    app_event_url = _build_managed_app_event_url(entry)
    payload = {
        "title": title,
        "message": message,
        "data": {
            "image": f"{snapshot_url}?v={int(now.timestamp())}" if snapshot_url else None,
            "clickAction": app_event_url,
            "actions": [
                {"action": "URI", "title": "View Event Clip", "uri": app_event_url},
            ],
        },
    }
    doorbell_action = _camera_doorbell_action(entry.channel) if entry.event_type == "DOORBELL" else None
    if doorbell_action:
        payload["data"]["actions"].insert(
            0,
            {
                "action": "URI",
                "title": doorbell_action.title,
                "uri": _build_managed_unlock_url(entry.channel, entry.entry_id),
            },
        )
    if payload["data"]["image"] is None:
        payload["data"].pop("image", None)

    for service in notify_services:
        try:
            await ha_client.call_service(service, payload)
        except HomeAssistantClientError as exc:
            logger.error("Managed notification via %s failed for %s: %s", service, entry.entry_id, exc)
            return

    notification_delivery_history[history_key] = now
    logger.info("Managed notifications sent for %s via %s", entry.entry_id, notify_services)


async def _handle_ha_state_changed(event: dict[str, Any]) -> None:
    event_data = event.get("data") or {}
    entity_id = event_data.get("entity_id")
    mapping = _entity_mapping_for_state_change(entity_id or "")
    if not mapping:
        return

    new_state = event_data.get("new_state") or {}
    old_state = event_data.get("old_state") or {}
    if str(new_state.get("state")).lower() != "on":
        return
    if str(old_state.get("state")).lower() == "on":
        return

    channel, event_type = mapping
    if not _channel_is_participating(channel) or not _channel_allows_event_type(channel, event_type):
        return

    try:
        event_timestamp_raw = new_state.get("last_changed") or event.get("time_fired")
        event_timestamp = datetime.fromisoformat(str(event_timestamp_raw).replace("Z", "+00:00"))
    except Exception:
        event_timestamp = datetime.now(timezone.utc)

    history_key = f"{entity_id}:{new_state.get('state')}:{event_type}"
    last_seen = ha_state_event_history.get(history_key)
    if last_seen and abs((event_timestamp - last_seen).total_seconds()) < EVENT_DEDUPE_WINDOW_SECONDS:
        return
    ha_state_event_history[history_key] = event_timestamp

    camera_name = _channel_name(channel) or f"Channel {channel}"
    title, message = _default_event_title_message(event_type, camera_name)
    snapshot_url = await _capture_snapshot_for_channel(channel, event_type, event_timestamp)

    entry, created = _create_event_entry(
        channel=channel,
        event_type=event_type,
        timestamp=event_timestamp,
        camera_name=camera_name,
        source="home_assistant_ws",
        title=title,
        message=message,
        snapshot_url=snapshot_url,
    )
    await _broadcast_recent_event(entry)
    if created:
        _schedule_clip_generation(entry)
        asyncio.create_task(_send_managed_notifications(entry))


async def _start_ha_ws_listener() -> None:
    global ha_ws_listener_task, ha_ws_listener
    if not ha_client.enabled or not ha_client.access_token:
        return
    if ha_ws_listener_task and not ha_ws_listener_task.done():
        return

    ha_ws_listener = HomeAssistantWebSocketListener(
        websocket_url=ha_client.websocket_url,
        access_token=ha_client.access_token,
        on_state_changed=_handle_ha_state_changed,
    )
    ha_ws_listener_task = asyncio.create_task(ha_ws_listener.run_forever(), name="ha-ws-listener")
    logger.info("Home Assistant WebSocket listener task started")


async def _stop_ha_ws_listener() -> None:
    global ha_ws_listener_task, ha_ws_listener
    if ha_ws_listener:
        ha_ws_listener.stop()
    if ha_ws_listener_task:
        ha_ws_listener_task.cancel()
        try:
            await ha_ws_listener_task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning("Error while stopping Home Assistant WebSocket listener: %s", exc)
    ha_ws_listener_task = None
    ha_ws_listener = None


def _resolve_ingest_channel(payload_channel: int, camera_name: Optional[str]) -> int:
    normalized_name = _normalize_camera_name(camera_name)

    if normalized_name:
        name_matches = [
            info["channel"]
            for info in available_channels
            if info.get("enabled", True)
            and _channel_is_participating(info["channel"])
            and _normalize_camera_name(info.get("name")) == normalized_name
        ]
        if len(name_matches) == 1:
            resolved_channel = name_matches[0]
            if resolved_channel != payload_channel:
                logger.warning(
                    "Resolved ingest channel mismatch for camera '%s': payload channel=%d, actual channel=%d",
                    camera_name,
                    payload_channel,
                    resolved_channel,
                )
            return resolved_channel

    if _channel_is_participating(payload_channel):
        return payload_channel

    if payload_channel > 0 and _channel_is_participating(payload_channel - 1):
        logger.warning(
            "Assuming one-based channel numbering for ingest payload channel=%d; using channel=%d instead",
            payload_channel,
            payload_channel - 1,
        )
        return payload_channel - 1

    return payload_channel


def _resolve_default_live_channel() -> Optional[int]:
    if DEFAULT_LIVE_CHANNEL_CONFIG is not None and DEFAULT_LIVE_CHANNEL_CONFIG in participating_channels:
        return DEFAULT_LIVE_CHANNEL_CONFIG
    if participating_channels:
        return min(participating_channels)
    return None


def _channel_is_participating(channel: int) -> bool:
    return channel in participating_channels


def _channel_has_buffer(channel: int) -> bool:
    return channel in buffered_channels and channel in rolling_buffers


def _resolve_live_channel(channel: Optional[int] = None) -> Optional[int]:
    if channel is not None:
        return channel if _channel_is_participating(channel) else None
    return default_live_channel


def _channel_info_payload(channel_info: dict[str, Any]) -> ChannelInfo:
    channel = channel_info["channel"]
    return ChannelInfo(
        **channel_info,
        participating=channel in participating_channels,
        buffered=channel in buffered_channels,
        default_live=channel == default_live_channel,
        allowed_event_types=_sorted_event_types(_channel_allowed_event_types(channel)) if channel in participating_channels else [],
    )


async def _load_home_assistant_entity_catalog() -> HomeAssistantEntityCatalog:
    if not ha_client.enabled:
        return HomeAssistantEntityCatalog()

    states = await ha_client.get_states()
    binary_sensors: list[HomeAssistantEntitySummary] = []
    cameras: list[HomeAssistantEntitySummary] = []

    for state in states:
        entity_id = str(state.get("entity_id") or "").strip()
        if "." not in entity_id:
            continue
        domain = entity_id.split(".", 1)[0]
        friendly_name = str((state.get("attributes") or {}).get("friendly_name") or entity_id)
        summary = HomeAssistantEntitySummary(
            entity_id=entity_id,
            friendly_name=friendly_name,
            domain=domain,
            state=str(state.get("state") or ""),
        )
        if domain == "binary_sensor":
            binary_sensors.append(summary)
        elif domain == "camera":
            cameras.append(summary)

    binary_sensors.sort(key=lambda item: (item.friendly_name.casefold(), item.entity_id))
    cameras.sort(key=lambda item: (item.friendly_name.casefold(), item.entity_id))
    return HomeAssistantEntityCatalog(binary_sensors=binary_sensors, cameras=cameras)


async def _rolling_buffer_monitor_loop() -> None:
    while True:
        await asyncio.sleep(ROLLING_BUFFER_MONITOR_INTERVAL_SECONDS)
        if not rolling_buffers:
            continue
        for channel, buffer in list(rolling_buffers.items()):
            try:
                if buffer.is_running():
                    continue
                logger.warning(
                    "Rolling buffer monitor detected a stopped recorder for channel %d; restarting",
                    channel,
                )
                await buffer.restart()
                logger.info("Rolling buffer restarted for channel %d", channel)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Rolling buffer monitor error for channel %d: %s", channel, e)


# ─── Lifespan (startup / shutdown) ───────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global nvr_host, timeline_index, rolling_buffers, rolling_buffer_monitor_task, storage_manager
    global available_channels, participating_channels, buffered_channels, default_live_channel, allowed_event_types_by_channel
    global settings_store, watchtower_settings

    logger.info("Starting Watchtower...")
    logger.info("Connecting to NVR at %s:%s", NVR_HOST, NVR_PORT)
    settings_store = SettingsStore(SETTINGS_FILE)
    watchtower_settings = settings_store.load()
    logger.info("Home Assistant API access: %s", "enabled" if ha_client.enabled else "disabled")

    try:
        nvr_host = Host(
            host=NVR_HOST,
            port=NVR_PORT,
            username=NVR_USERNAME,
            password=NVR_PASSWORD,
            use_https=NVR_SSL,
        )
        await nvr_host.get_host_data()
        logger.info("Connected to NVR: %s (%s channels)", nvr_host.nvr_name, nvr_host.num_channels)
        available_channels = await get_channels_info(nvr_host)
        valid_channels = {info["channel"] for info in available_channels if info.get("enabled", True)}
        participating_channels = _parse_channel_selection(WATCH_CHANNELS_CONFIG, valid_channels)
        buffered_channels = _parse_channel_selection(BUFFER_CHANNELS_CONFIG, valid_channels, fallback=participating_channels)
        buffered_channels &= participating_channels
        allowed_event_types_by_channel = _parse_camera_event_type_selection(
            CAMERA_EVENT_TYPES_CONFIG,
            participating_channels,
        )
        default_live_channel = _resolve_default_live_channel()
        logger.info(
            "Available enabled cameras: %s",
            [
                {
                    "channel": info["channel"],
                    "name": info.get("name"),
                    "model": info.get("model"),
                }
                for info in available_channels
                if info.get("enabled", True)
            ],
        )
        logger.info(
            "Active camera config resolved: participating=%s buffered=%s default_live=%s",
            _sorted_channels(participating_channels),
            _sorted_channels(buffered_channels),
            default_live_channel if default_live_channel is not None else "none",
        )
        logger.info(
            "Camera event types resolved: %s",
            {
                channel: _sorted_event_types(_channel_allowed_event_types(channel))
                for channel in _sorted_channels(participating_channels)
            },
        )
        _sync_watchtower_settings()
    except ReolinkError as e:
        logger.error("Failed to connect to NVR: %s", e)
        nvr_host = None
        available_channels = []
        participating_channels = set()
        buffered_channels = set()
        default_live_channel = None
        allowed_event_types_by_channel = {}
    except Exception as e:
        logger.error("Unexpected error during startup: %s", e)
        nvr_host = None
        available_channels = []
        participating_channels = set()
        buffered_channels = set()
        default_live_channel = None
        allowed_event_types_by_channel = {}

    timeline_index = TimelineIndex(index_file=INDEX_FILE)
    logger.info("Timeline index initialized at %s", INDEX_FILE)

    storage_manager = StorageManager(
        clips_directory=str(CLIPS_DIRECTORY),
        timeline_index=timeline_index,
        retention_days=RETENTION_DAYS,
        max_storage_mb=MAX_STORAGE_MB,
        external_storage_path=EXTERNAL_STORAGE_PATH,
        buffer_retention_seconds=BUFFER_RETENTION_SECONDS,
        absolute_buffer_max_age_seconds=ABSOLUTE_MAX_BUFFER_AGE_SECONDS,
    )
    try:
        await storage_manager.start()
    except Exception as e:
        logger.error("Failed to start storage manager: %s", e)
        storage_manager = None

    rolling_buffers = {}
    if LOCAL_CLIP_ENABLED and nvr_host:
        for channel in _sorted_channels(buffered_channels):
            try:
                buffer = RollingSegmentBuffer(
                    nvr_client=nvr_host,
                    channel=channel,
                    storage_dir=str(CLIPS_DIRECTORY / "rolling_buffer" / f"channel_{channel}"),
                    segment_seconds=ROLLING_SEGMENT_SECONDS,
                    retention_seconds=BUFFER_RETENTION_SECONDS,
                    max_segment_age_seconds=ABSOLUTE_MAX_BUFFER_AGE_SECONDS,
                    ffmpeg_bin=FFMPEG_BIN,
                    stream=_preferred_stream(),
                )
                await buffer.start()
                rolling_buffers[channel] = buffer
            except Exception as e:
                logger.error("Failed to start rolling buffer for channel %d: %s", channel, e)

    if rolling_buffers:
        rolling_buffer_monitor_task = asyncio.create_task(_rolling_buffer_monitor_loop())

    await _start_ha_ws_listener()

    yield  # ← app runs here

    logger.info("Shutting down Watchtower...")
    await _stop_ha_ws_listener()
    if rolling_buffer_monitor_task:
        rolling_buffer_monitor_task.cancel()
        try:
            await rolling_buffer_monitor_task
        except asyncio.CancelledError:
            pass
        rolling_buffer_monitor_task = None
    for channel, buffer in list(rolling_buffers.items()):
        try:
            await buffer.stop()
        except Exception as e:
            logger.error("Error stopping rolling buffer for channel %d: %s", channel, e)
    rolling_buffers = {}
    if storage_manager:
        try:
            await storage_manager.stop()
        except Exception as e:
            logger.error("Error stopping storage manager: %s", e)
    if nvr_host:
        try:
            await nvr_host.logout()
        except Exception as e:
            logger.error("Error during logout: %s", e)

# ─── App init ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title=APP_NAME,
    description="Camera event dashboard, clip playback, and live view for a Reolink NVR",
    version="0.5.4",
    lifespan=lifespan,
)

if ALLOW_CORS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )


# ─── Helper ───────────────────────────────────────────────────────────────────

def _require_nvr():
    if not nvr_host:
        raise HTTPException(status_code=503, detail="NVR not connected. Check add-on logs.")


def _preferred_stream() -> str:
    return "main" if CLIP_QUALITY == "high" else "sub"


def _normalize_event_type(event_type: Optional[str]) -> Optional[str]:
    if event_type is None:
        return None
    normalized = event_type.strip().upper()
    return normalized if normalized in EVENT_TYPE_MAP else None


def _normalize_datetime_for_compare(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _current_datetime_like(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    return datetime.now()


def _pick_earlier_timestamp(first: datetime, second: datetime) -> datetime:
    return first if _normalize_datetime_for_compare(first) <= _normalize_datetime_for_compare(second) else second


def _merge_timeline_entries(existing: TimelineEntry, incoming: TimelineEntry) -> TimelineEntry:
    merged_metadata = dict(existing.metadata or {})
    incoming_metadata = incoming.metadata or {}

    existing_status = merged_metadata.get("clip_status")
    incoming_status = incoming_metadata.get("clip_status")
    status_rank = {"failed": 0, "pending": 1, "generating": 2, "ready": 3}
    if status_rank.get(incoming_status, -1) > status_rank.get(existing_status, -1):
        merged_metadata["clip_status"] = incoming_status
    elif existing_status is None and incoming_status is not None:
        merged_metadata["clip_status"] = incoming_status

    for key, value in incoming_metadata.items():
        if key == "clip_status" or value in (None, "", [], {}):
            continue
        if key in {"clip_file", "clip_source", "clip_url", "download_url", "stream_url", "live_url", "duration_seconds"}:
            if status_rank.get(incoming_status, -1) >= status_rank.get(existing_status, -1):
                merged_metadata[key] = value
            continue
        merged_metadata[key] = value

    return TimelineEntry(
        entry_id=existing.entry_id,
        timestamp=_pick_earlier_timestamp(existing.timestamp, incoming.timestamp),
        channel=existing.channel,
        event_type=existing.event_type,
        clip_path=existing.clip_path or incoming.clip_path,
        thumbnail_path=existing.thumbnail_path or incoming.thumbnail_path,
        metadata=merged_metadata,
    )


def _upsert_or_merge_timeline_entry(entry: TimelineEntry) -> tuple[TimelineEntry, bool]:
    if not timeline_index:
        return entry, True

    match = timeline_index.find_recent_entry(
        channel=entry.channel,
        event_type=entry.event_type,
        timestamp=entry.timestamp,
        window_seconds=EVENT_DEDUPE_WINDOW_SECONDS,
    )

    if match and match.entry_id != entry.entry_id:
        merged_entry = _merge_timeline_entries(match, entry)
        timeline_index.upsert_entry(merged_entry)
        return merged_entry, False

    timeline_index.upsert_entry(entry)
    return entry, True


def _timeline_entry_to_recent(entry: TimelineEntry) -> dict[str, Any]:
    metadata = entry.metadata or {}
    clip_source = entry.clip_path or metadata.get("clip_url") or metadata.get("download_url") or metadata.get("stream_url")
    clip_url = f"api/events/{entry.entry_id}/clip" if clip_source else None
    thumbnail_url = entry.thumbnail_path or metadata.get("thumbnail_url") or metadata.get("snapshot_url")
    return {
        "entry_id": entry.entry_id,
        "timestamp": entry.timestamp.isoformat(),
        "channel": entry.channel,
        "event_type": entry.event_type,
        "clip_url": clip_url,
        "raw_clip_url": clip_source,
        "thumbnail_url": thumbnail_url,
        "clip_status": metadata.get("clip_status"),
        "title": metadata.get("title"),
        "message": metadata.get("message"),
        "camera_name": metadata.get("camera_name"),
        "source": metadata.get("source"),
        "duration_seconds": metadata.get("duration_seconds"),
        "metadata": metadata,
    }


def _parse_onvif_event_notifications(data: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(data)
    except Exception as e:
        logger.debug("Failed to parse ONVIF webhook payload: %s", e)
        return []

    namespace_wsn = "{http://docs.oasis-open.org/wsn/b-2}"
    namespace_schema = "{http://www.onvif.org/ver10/schema}"

    parsed: list[dict[str, Any]] = []
    for message in root.iter(f"{namespace_wsn}NotificationMessage"):
        topic_element = message.find(f"{namespace_wsn}Topic[@Dialect='http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet']")
        if topic_element is None or not topic_element.text:
            continue

        rule = Path(topic_element.text).name
        if rule not in {"Motion", "MotionAlarm", "FaceDetect", "PeopleDetect", "VehicleDetect", "DogCatDetect", "Package", "Visitor"}:
            continue

        channel = None
        source_element = message.find(f".//{namespace_schema}SimpleItem[@Name='Source']")
        if source_element is None:
            source_element = message.find(f".//{namespace_schema}SimpleItem[@Name='VideoSourceConfigurationToken']")
        if source_element is not None and "Value" in source_element.attrib:
            try:
                channel = int(source_element.attrib["Value"])
            except ValueError:
                channel = None

        if channel is None:
            continue

        if rule in {"PeopleDetect", "FaceDetect", "Package"}:
            event_type = "PERSON"
        elif rule == "DogCatDetect":
            event_type = "ANIMAL"
        elif rule == "Visitor":
            event_type = "DOORBELL"
        elif rule in {"Motion", "MotionAlarm"}:
            event_type = "MOTION"
        elif rule == "VehicleDetect":
            event_type = "VEHICLE"
        else:
            event_type = None

        if event_type is None:
            continue

        parsed.append({
            "channel": channel,
            "event_type": event_type,
            "rule": rule,
        })

    return parsed


def _create_event_entry(
    *,
    channel: int,
    event_type: str,
    timestamp: Optional[datetime] = None,
    camera_name: Optional[str] = None,
    source: str = "nvr_webhook",
    title: Optional[str] = None,
    message: Optional[str] = None,
    snapshot_url: Optional[str] = None,
    live_url: Optional[str] = None,
) -> tuple[TimelineEntry, bool]:
    event_timestamp = timestamp or datetime.now()
    entry = TimelineEntry(
        entry_id=f"{channel}_{event_timestamp.isoformat()}_{event_type}",
        timestamp=event_timestamp,
        channel=channel,
        event_type=event_type,
        clip_path=None,
        thumbnail_path=snapshot_url,
        metadata={
            "title": title or f"{event_type.title()} detected",
            "message": message or f"{event_type.title()} detected on channel {channel}",
            "camera_name": camera_name,
            "source": source,
            "snapshot_url": snapshot_url,
            "live_url": live_url,
            "clip_status": "pending",
            "clip_source": "local_rtsp",
        },
    )
    return _upsert_or_merge_timeline_entry(entry)


async def _broadcast_recent_event(entry: TimelineEntry):
    if not ui_clients:
        return
    if not _channel_is_participating(entry.channel) or not _channel_allows_event_type(entry.channel, entry.event_type):
        return

    payload = {
        "type": "event",
        "event": _timeline_entry_to_recent(entry),
    }

    alive_clients: list[WebSocket] = []
    for socket in list(ui_clients):
        try:
            await socket.send_json(payload)
            alive_clients.append(socket)
        except Exception:
            continue

    ui_clients[:] = alive_clients


async def _resolve_clip_for_event(
    channel: int,
    event_type: str,
    timestamp: datetime,
    stream: str = "sub",
    lookback_seconds: int = 90,
    lookahead_seconds: int = 180,
    retries: int = 2,
    retry_delay_seconds: float = 2.0,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not nvr_host:
        return None, None, None

    target_ts = timestamp

    async def _search_once() -> list[dict]:
        start_dt = target_ts - timedelta(seconds=lookback_seconds)
        end_dt = target_ts + timedelta(seconds=lookahead_seconds)
        return await search_recordings(
            host=nvr_host,
            channel=channel,
            start_dt=start_dt,
            end_dt=end_dt,
            event_type=event_type,
            stream=stream,
        )

    clips: list[dict] = []
    last_error: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            clips = await _search_once()
            if clips:
                break
        except Exception as e:
            last_error = e
            logger.debug("Unable to resolve clip for event (attempt %d/%d): %s", attempt + 1, retries + 1, e)

        if attempt < retries:
            await asyncio.sleep(retry_delay_seconds)

    if not clips:
        if last_error:
            logger.debug("Unable to resolve clip for event after retries: %s", last_error)
        return None, None, None

    def _distance_seconds(clip: dict[str, Any]) -> float:
        try:
            clip_ts = _normalize_datetime_for_compare(datetime.fromisoformat(clip["timestamp"]))
        except Exception:
            return float("inf")
        return abs((_normalize_datetime_for_compare(clip_ts) - _normalize_datetime_for_compare(target_ts)).total_seconds())

    best_clip = min(clips, key=_distance_seconds)
    return (
        best_clip.get("download_url") or best_clip.get("stream_url"),
        best_clip.get("stream_url"),
        best_clip.get("download_url"),
    )


async def _hydrate_event_clip(entry: TimelineEntry) -> Optional[TimelineEntry]:
    metadata = entry.metadata or {}
    if metadata.get("clip_status") in {"pending", "generating", "failed"} and not entry.clip_path:
        return entry

    if entry.clip_path or (entry.metadata or {}).get("clip_url") or (entry.metadata or {}).get("download_url") or (entry.metadata or {}).get("stream_url"):
        return entry

    try:
        resolved_clip, resolved_stream, resolved_download = await _resolve_clip_for_event(
            channel=entry.channel,
            event_type=entry.event_type,
            timestamp=entry.timestamp,
        )
    except Exception as e:
        logger.debug("On-demand clip resolution failed for %s: %s", entry.entry_id, e)
        return entry

    if not (resolved_clip or resolved_stream or resolved_download):
        return entry

    updated_metadata = dict(entry.metadata or {})
    if resolved_stream:
        updated_metadata.setdefault("stream_url", resolved_stream)
    if resolved_download:
        updated_metadata.setdefault("download_url", resolved_download)

    updated_entry = TimelineEntry(
        entry_id=entry.entry_id,
        timestamp=entry.timestamp,
        channel=entry.channel,
        event_type=entry.event_type,
        clip_path=resolved_clip or resolved_download or resolved_stream,
        thumbnail_path=entry.thumbnail_path,
        metadata=updated_metadata,
    )
    timeline_index.upsert_entry(updated_entry)
    return updated_entry


def _event_clip_storage_path(channel: int, timestamp: datetime, event_type: str) -> Path:
    base_dir = CLIPS_DIRECTORY / "event_clips"
    day_dir = base_dir / f"channel_{channel}" / timestamp.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    clip_name = f"{timestamp.strftime('%Y%m%dT%H%M%S')}_{event_type.lower()}.mp4"
    return day_dir / clip_name


async def _generate_buffered_event_clip(entry: TimelineEntry) -> None:
    rolling_buffer = rolling_buffers.get(entry.channel)
    if not rolling_buffer:
        logger.debug("Rolling buffer unavailable for %s", entry.entry_id)
        return

    clip_path = _event_clip_storage_path(entry.channel, entry.timestamp, entry.event_type)
    clip_start = entry.timestamp - timedelta(seconds=max(CLIP_DURATION_BEFORE, 1))
    clip_end = entry.timestamp + timedelta(seconds=max(CLIP_DURATION_AFTER, 1))
    logger.debug(
        "Buffered clip window for %s: event=%s start=%s end=%s before=%ss after=%ss",
        entry.entry_id,
        entry.timestamp.isoformat(),
        clip_start.isoformat(),
        clip_end.isoformat(),
        CLIP_DURATION_BEFORE,
        CLIP_DURATION_AFTER,
    )
    try:
        logger.debug("Rolling buffer stats before clip for %s: %s", entry.entry_id, rolling_buffer.get_stats())
    except Exception:
        pass

    window_ready_at = _normalize_datetime_for_compare(clip_end)
    current_time = _normalize_datetime_for_compare(_current_datetime_like(clip_end))
    if current_time < window_ready_at:
        wait_seconds = max((window_ready_at - current_time).total_seconds(), 0.0)
        logger.debug(
            "Waiting %.1fs for buffered window to complete before stitching %s",
            wait_seconds,
            entry.entry_id,
        )
        await asyncio.sleep(wait_seconds)

    generating_metadata = dict(entry.metadata or {})
    generating_metadata["clip_status"] = "generating"
    generating_metadata["clip_source"] = "rolling_rtsp"
    generating_entry = TimelineEntry(
        entry_id=entry.entry_id,
        timestamp=entry.timestamp,
        channel=entry.channel,
        event_type=entry.event_type,
        clip_path=entry.clip_path,
        thumbnail_path=entry.thumbnail_path,
        metadata=generating_metadata,
    )
    timeline_index.upsert_entry(generating_entry)
    await _broadcast_recent_event(generating_entry)

    try:
        result_path = None
        last_error: Optional[str] = None
        for attempt in range(1, BUFFER_CLIP_RETRY_ATTEMPTS + 1):
            logger.debug(
                "Attempt %d/%d to build buffered clip for %s",
                attempt,
                BUFFER_CLIP_RETRY_ATTEMPTS,
                entry.entry_id,
            )
            result_path = await rolling_buffer.build_clip(clip_start, clip_end, str(clip_path))
            if result_path:
                break
            try:
                stats = rolling_buffer.get_stats()
                logger.debug("Rolling buffer stats after attempt %d for %s: %s", attempt, entry.entry_id, stats)
            except Exception:
                pass
            last_error = "No buffered segments were available for the requested window"
            if attempt < BUFFER_CLIP_RETRY_ATTEMPTS:
                await asyncio.sleep(BUFFER_CLIP_RETRY_DELAY_SECONDS)

        if not result_path:
            raise RuntimeError(last_error or "No buffered segments were available for the requested window")

        ready_metadata = dict(entry.metadata or {})
        ready_metadata["clip_status"] = "ready"
        ready_metadata["clip_source"] = "rolling_rtsp"
        ready_metadata["clip_file"] = result_path
        ready_metadata["duration_seconds"] = max(int((clip_end - clip_start).total_seconds()), 1)
        ready_entry = TimelineEntry(
            entry_id=entry.entry_id,
            timestamp=entry.timestamp,
            channel=entry.channel,
            event_type=entry.event_type,
            clip_path=result_path,
            thumbnail_path=entry.thumbnail_path,
            metadata=ready_metadata,
        )
        timeline_index.upsert_entry(ready_entry)
        await _broadcast_recent_event(ready_entry)
        logger.info("Buffered clip generated for %s: %s", entry.entry_id, result_path)
    except Exception as e:
        failed_metadata = dict(entry.metadata or {})
        failed_metadata["clip_status"] = "failed"
        failed_metadata["clip_source"] = "rolling_rtsp"
        failed_metadata["clip_error"] = str(e)
        failed_entry = TimelineEntry(
            entry_id=entry.entry_id,
            timestamp=entry.timestamp,
            channel=entry.channel,
            event_type=entry.event_type,
            clip_path=entry.clip_path,
            thumbnail_path=entry.thumbnail_path,
            metadata=failed_metadata,
        )
        timeline_index.upsert_entry(failed_entry)
        await _broadcast_recent_event(failed_entry)
        logger.error("Failed to generate buffered clip for %s: %s", entry.entry_id, e)

        # Fall back to a direct RTSP capture so a clip still exists while the
        # ring buffer warms up or if segment stitching misses the event window.
        await _capture_direct_event_clip(entry, clip_path)


async def _capture_direct_event_clip(entry: TimelineEntry, clip_path: Path) -> None:
    if not nvr_host:
        return

    try:
        rtsp_url = await nvr_host.get_rtsp_stream_source(entry.channel, stream=_preferred_stream())
    except Exception as e:
        logger.error("Direct RTSP fallback failed for %s: %s", entry.entry_id, e)
        return

    if not rtsp_url:
        logger.error("Direct RTSP fallback has no stream for %s", entry.entry_id)
        return

    clip_seconds = max(LOCAL_CLIP_SECONDS, 1)
    try:
        if clip_path.exists():
            clip_path.unlink()

        cmd = [
            FFMPEG_BIN,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-rtsp_transport",
            "tcp",
            "-i",
            rtsp_url,
            "-t",
            str(clip_seconds),
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "28",
            "-pix_fmt",
            "yuv420p",
            "-an",
            "-movflags",
            "+faststart",
            str(clip_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError((stderr or b"").decode("utf-8", "ignore").strip() or f"ffmpeg exited with {proc.returncode}")
        if not clip_path.exists() or clip_path.stat().st_size == 0:
            raise RuntimeError("ffmpeg completed but clip file was not created")

        ready_metadata = dict(entry.metadata or {})
        ready_metadata["clip_status"] = "ready"
        ready_metadata["clip_source"] = "direct_rtsp"
        ready_metadata["clip_file"] = str(clip_path)
        ready_metadata["duration_seconds"] = clip_seconds
        ready_entry = TimelineEntry(
            entry_id=entry.entry_id,
            timestamp=entry.timestamp,
            channel=entry.channel,
            event_type=entry.event_type,
            clip_path=str(clip_path),
            thumbnail_path=entry.thumbnail_path,
            metadata=ready_metadata,
        )
        timeline_index.upsert_entry(ready_entry)
        await _broadcast_recent_event(ready_entry)
        logger.info("Direct RTSP fallback clip generated for %s: %s", entry.entry_id, clip_path)
    except Exception as e:
        logger.error("Direct RTSP fallback failed for %s: %s", entry.entry_id, e)


def _schedule_clip_generation(entry: TimelineEntry) -> None:
    if not LOCAL_CLIP_ENABLED:
        return

    if entry.channel not in buffered_channels:
        logger.info(
            "Channel %d is not configured for buffered clips; using direct fallback for %s",
            entry.channel,
            entry.entry_id,
        )
        clip_path = _event_clip_storage_path(entry.channel, entry.timestamp, entry.event_type)
        task = asyncio.create_task(_capture_direct_event_clip(entry, clip_path))
        clip_tasks.add(task)

        def _done_direct(t: asyncio.Task) -> None:
            clip_tasks.discard(t)

        task.add_done_callback(_done_direct)
        return

    if entry.channel not in rolling_buffers:
        logger.warning(
            "Rolling buffer is not available for channel %d; cannot generate pre-roll clip for %s",
            entry.channel,
            entry.entry_id,
        )
        return

    task = asyncio.create_task(_generate_buffered_event_clip(entry))
    clip_tasks.add(task)

    def _done_callback(t: asyncio.Task) -> None:
        clip_tasks.discard(t)

    task.add_done_callback(_done_callback)


def _is_http_url(value: Optional[str]) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in ("http", "https")


def _entry_media_source(entry: TimelineEntry, kind: str) -> Optional[str]:
    metadata = entry.metadata or {}
    if kind == "clip":
        return (
            entry.clip_path
            or metadata.get("clip_url")
            or metadata.get("download_url")
            or metadata.get("stream_url")
        )
    return None


async def _proxy_http_media(url: str):
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=120, connect=15, sock_connect=15, sock_read=120)
    last_error: Optional[Exception] = None

    for attempt in range(3):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as upstream:
                    if upstream.status >= 400:
                        text = await upstream.text()
                        raise HTTPException(
                            status_code=upstream.status,
                            detail=text[:500] or f"Upstream media request failed: {upstream.status}",
                        )

                    content_type = upstream.headers.get("Content-Type", "application/octet-stream")

                    async def body_iter():
                        async for chunk in upstream.content.iter_chunked(64 * 1024):
                            yield chunk

                    return StreamingResponse(body_iter(), media_type=content_type)
        except aiohttp.ServerDisconnectedError as e:
            last_error = e
            logger.warning("Upstream media server disconnected on attempt %d/3: %s", attempt + 1, e)
        except aiohttp.ClientError as e:
            last_error = e
            logger.warning("Upstream media request failed on attempt %d/3: %s", attempt + 1, e)

    raise HTTPException(status_code=502, detail=f"Unable to fetch media from source: {last_error}")


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/", summary="API root")
async def root(request: Request):
    accept = request.headers.get("accept", "")
    if "text/html" in accept or "application/xhtml+xml" in accept:
        return HTMLResponse(_dashboard_html_v2())
    return {
        "name": APP_NAME,
        "version": "0.5.4",
        "status": "running",
        "docs": "/docs",
        "health": "/api/health",
        "app": "/app",
    }


@app.get("/api/health", response_model=HealthCheck, summary="Health check")
async def health_check():
    return HealthCheck(
        status="ok" if nvr_host else "error",
        nvr_connected=nvr_host is not None,
        nvr_host=NVR_HOST,
    )


@app.get("/api/device/info", response_model=DeviceInfo, summary="NVR device information")
async def get_device_info():
    _require_nvr()
    try:
        return DeviceInfo(
            model=nvr_host.model or "Unknown",
            firmware_version=nvr_host.sw_version or "Unknown",
            nvr_name=nvr_host.nvr_name or "Unknown",
            mac_address=nvr_host.mac_address or "Unknown",
            is_nvr=nvr_host.is_nvr,
            num_channels=nvr_host.num_channels,
        )
    except Exception as e:
        logger.error("Error getting device info: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/camera-config", response_model=CameraSelectionInfo, summary="Configured participating cameras")
async def get_camera_config():
    _require_nvr()
    return CameraSelectionInfo(
        available_channels=[_channel_info_payload(info) for info in available_channels],
        participating_channels=_sorted_channels(participating_channels),
        buffered_channels=_sorted_channels(buffered_channels),
        default_live_channel=default_live_channel,
        supported_event_types=_supported_event_types(),
    )


@app.get("/api/home-assistant/status", response_model=HomeAssistantStatus, summary="Home Assistant API status for Watchtower")
async def get_home_assistant_status():
    services = await ha_client.list_mobile_app_notify_services() if ha_client.enabled else []
    return HomeAssistantStatus(
        enabled=ha_client.enabled,
        discovered_mobile_notify_services=services,
        websocket_listener_running=bool(ha_ws_listener_task and not ha_ws_listener_task.done()),
    )


@app.get("/api/home-assistant/entities", response_model=HomeAssistantEntityCatalog, summary="Discover Home Assistant entities for Watchtower")
async def get_home_assistant_entities():
    if not ha_client.enabled:
        raise HTTPException(status_code=503, detail="Home Assistant API access is not enabled for Watchtower.")
    return await _load_home_assistant_entity_catalog()


@app.get("/api/notifications/config", response_model=NotificationConfigResponse, summary="Managed notification settings")
async def get_notification_config():
    services = await ha_client.list_mobile_app_notify_services() if ha_client.enabled else []
    return NotificationConfigResponse(
        settings=watchtower_settings.notifications,
        home_assistant=HomeAssistantStatus(
            enabled=ha_client.enabled,
            discovered_mobile_notify_services=services,
            websocket_listener_running=bool(ha_ws_listener_task and not ha_ws_listener_task.done()),
        ),
    )


@app.put("/api/notifications/config", response_model=NotificationConfigResponse, summary="Update managed notification settings")
async def update_notification_config(settings: ManagedNotificationSettings):
    global watchtower_settings
    watchtower_settings.notifications = settings
    _sync_watchtower_settings()
    services = await ha_client.list_mobile_app_notify_services() if ha_client.enabled else []
    return NotificationConfigResponse(
        settings=watchtower_settings.notifications,
        home_assistant=HomeAssistantStatus(
            enabled=ha_client.enabled,
            discovered_mobile_notify_services=services,
            websocket_listener_running=bool(ha_ws_listener_task and not ha_ws_listener_task.done()),
        ),
    )


@app.post("/api/notifications/test", summary="Send a managed notification test")
async def send_notification_test(payload: NotificationTestRequest):
    if not ha_client.enabled:
        raise HTTPException(status_code=503, detail="Home Assistant API access is not enabled for Watchtower.")

    try:
        await ha_client.call_service(
            payload.service,
            {
                "title": payload.title,
                "message": payload.message,
            },
        )
    except HomeAssistantClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {"status": "sent", "service": payload.service}


@app.post("/api/doorbell-action/unlock", summary="Execute a configured doorbell unlock action")
async def execute_doorbell_unlock(payload: DoorbellActionRequest):
    return await _execute_doorbell_action(payload.channel, payload.event_id)


@app.get("/api/channels", response_model=List[ChannelInfo], summary="List all camera channels")
async def get_channels():
    _require_nvr()
    try:
        channels = await get_channels_info(nvr_host)
        return [_channel_info_payload(ch) for ch in channels]
    except Exception as e:
        logger.error("Error getting channels: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search", response_model=SearchResponse, summary="Search recordings by date and event type")
async def search(
    channel: int = Query(..., description="Camera channel number (0-based)"),
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD (defaults to start_date)"),
    event_type: Optional[str] = Query(
        None,
        description=f"Filter by event type: {', '.join(EVENT_TYPE_MAP.keys())}",
    ),
    stream: str = Query("sub", description="Stream quality: 'sub' (default) or 'main'"),
):
    """
    Search NVR recordings by date range and optional event type.

    Returns a list of clips with timestamps, duration, and stream URLs.

    Event types:
    - **DOORBELL** — Doorbell button press (visitor)
    - **PERSON**   — Person / human detection
    - **MOTION**   — Motion detection (all motion)
    - **ANIMAL**   — Animal / pet detection
    - **VEHICLE**  — Vehicle detection
    - *(omit for all recordings)*
    """
    _require_nvr()

    # Validate channel
    if channel < 0 or channel >= nvr_host.num_channels:
        raise HTTPException(
            status_code=400,
            detail=f"Channel {channel} out of range. NVR has {nvr_host.num_channels} channels (0-based).",
        )
    if not _channel_is_participating(channel):
        raise HTTPException(status_code=403, detail=f"Channel {channel} is not enabled in Watchtower.")

    # Validate dates
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt   = datetime.strptime(end_date or start_date, "%Y-%m-%d")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="start_date must be ≤ end_date")

    # Validate event type
    if event_type:
        event_type_upper = event_type.upper()
        if event_type_upper not in EVENT_TYPE_MAP:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid event_type '{event_type}'. Must be one of: {', '.join(EVENT_TYPE_MAP.keys())}",
            )
        if not _channel_allows_event_type(channel, event_type_upper):
            raise HTTPException(
                status_code=403,
                detail=f"Event type '{event_type_upper}' is not enabled for channel {channel} in Watchtower.",
            )
        event_type = event_type_upper

    # Validate stream
    if stream not in ("sub", "main"):
        raise HTTPException(status_code=400, detail="stream must be 'sub' or 'main'")

    logger.info(
        "Search: channel=%s, start=%s, end=%s, event_type=%s, stream=%s",
        channel, start_date, end_date or start_date, event_type, stream,
    )

    try:
        raw_clips = await search_recordings(
            host=nvr_host,
            channel=channel,
            start_dt=start_dt,
            end_dt=end_dt,
            event_type=event_type,
            stream=stream,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ReolinkError as e:
        logger.error("Reolink API error during search: %s", e)
        raise HTTPException(status_code=502, detail=f"NVR API error: {e}")
    except Exception as e:
        logger.exception("Unexpected error during search")
        raise HTTPException(status_code=500, detail=str(e))

    clips = [Clip(**c) for c in raw_clips if _channel_allows_event_type(channel, c.get("event_type"))]

    for clip in clips:
        if _normalize_event_type(clip.event_type) is None:
            continue

        entry_id = f"{channel}_{clip.timestamp}_{clip.event_type}"
        entry = TimelineEntry(
            entry_id=entry_id,
            timestamp=datetime.fromisoformat(clip.timestamp),
            channel=channel,
            event_type=clip.event_type,
            clip_path=clip.download_url or clip.stream_url,
            thumbnail_path=None,
            metadata={
                "title": f"{clip.event_type.title()} event",
                "message": f"{clip.event_type.title()} detected on channel {channel}",
                "end_timestamp": clip.end_timestamp,
                "duration_seconds": clip.duration_seconds,
                "trigger": clip.trigger,
                "file_name": clip.file_name,
                "stream_url": clip.stream_url,
                "download_url": clip.download_url,
                "source": "search",
            },
        )
        timeline_index.upsert_entry(entry)
        await _broadcast_recent_event(entry)

    return SearchResponse(
        channel=channel,
        start_date=start_date,
        end_date=end_date or start_date,
        event_type=event_type,
        clips=clips,
        total_clips=len(clips),
    )

@app.get("/api/timeline", summary="Get event timeline")
async def get_timeline(
    hours: int = Query(24, description="How many hours back to query"),
    channel: Optional[int] = Query(None, description="Filter by channel"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    limit: int = Query(100, description="Maximum number of entries to return"),
):
    if event_type is not None and _normalize_event_type(event_type) is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid event_type '{event_type}'. Must be one of: {', '.join(EVENT_TYPE_MAP.keys())}",
        )
    if channel is not None and not _channel_is_participating(channel):
        raise HTTPException(status_code=403, detail=f"Channel {channel} is not enabled in Watchtower.")
    normalized_event_type = _normalize_event_type(event_type)
    if channel is not None and normalized_event_type is not None and not _channel_allows_event_type(channel, normalized_event_type):
        raise HTTPException(
            status_code=403,
            detail=f"Event type '{normalized_event_type}' is not enabled for channel {channel} in Watchtower.",
        )
    entries = timeline_index.get_entries(
        channel=channel,
        event_type=normalized_event_type,
        since=datetime.now() - __import__('datetime').timedelta(hours=hours),
        limit=limit if channel is not None else max(limit * 5, limit),
    )
    entries = [
        entry for entry in entries
        if _channel_is_participating(entry.channel) and _channel_allows_event_type(entry.channel, entry.event_type)
    ]
    return {
        "hours": hours,
        "channel": channel,
        "event_type": normalized_event_type,
        "total": len(entries),
        "entries": [e.to_dict() for e in entries],
    }


@app.get("/api/events/recent", summary="Get recent player-ready events")
@app.get("/app/api/events/recent", include_in_schema=False)
async def get_recent_events(
    limit: int = Query(20, description="Maximum number of events to return"),
    channel: Optional[int] = Query(None, description="Filter by channel"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
):
    normalized_event_type = _normalize_event_type(event_type)
    if event_type is not None and normalized_event_type is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid event_type '{event_type}'. Must be one of: {', '.join(EVENT_TYPE_MAP.keys())}",
        )
    if channel is not None and not _channel_is_participating(channel):
        raise HTTPException(status_code=403, detail=f"Channel {channel} is not enabled in Watchtower.")
    if channel is not None and normalized_event_type is not None and not _channel_allows_event_type(channel, normalized_event_type):
        raise HTTPException(
            status_code=403,
            detail=f"Event type '{normalized_event_type}' is not enabled for channel {channel} in Watchtower.",
        )
    entries = timeline_index.get_entries(
        channel=channel,
        event_type=normalized_event_type,
        limit=limit if channel is not None else max(limit * 5, limit),
    )
    entries = [
        entry for entry in entries
        if _channel_is_participating(entry.channel) and _channel_allows_event_type(entry.channel, entry.event_type)
    ]
    return {
        "limit": limit,
        "channel": channel,
        "event_type": normalized_event_type,
        "total": len(entries),
        "events": [_timeline_entry_to_recent(entry) for entry in entries],
    }


@app.post("/api/events/ingest", summary="Ingest a live event from Home Assistant or another source")
@app.post("/app/api/events/ingest", include_in_schema=False)
async def ingest_event(payload: EventIngestRequest):
    _require_nvr()

    event_type = _normalize_event_type(payload.event_type)
    if event_type is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid event_type '{payload.event_type}'. Must be one of: {', '.join(EVENT_TYPE_MAP.keys())}",
        )

    if payload.channel < 0:
        raise HTTPException(
            status_code=400,
            detail=f"Channel {payload.channel} out of range. NVR channels must be non-negative.",
        )
    resolved_channel = _resolve_ingest_channel(payload.channel, payload.camera_name)
    if resolved_channel < 0 or resolved_channel >= nvr_host.num_channels:
        raise HTTPException(
            status_code=400,
            detail=f"Channel {payload.channel} out of range. NVR has {nvr_host.num_channels} channels (0-based).",
        )
    if not _channel_is_participating(resolved_channel):
        raise HTTPException(
            status_code=403,
            detail=(
                f"Channel {payload.channel} is not enabled in Watchtower."
                if resolved_channel == payload.channel
                else f"Resolved channel {resolved_channel} is not enabled in Watchtower."
            ),
        )
    if not _channel_allows_event_type(resolved_channel, event_type):
        raise HTTPException(
            status_code=403,
            detail=f"Event type '{event_type}' is not enabled for channel {resolved_channel} in Watchtower.",
        )

    try:
        event_timestamp = datetime.fromisoformat(payload.timestamp) if payload.timestamp else datetime.now()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {e}")

    clip_url = payload.clip_url or payload.download_url or payload.stream_url
    stream_url = payload.stream_url
    download_url = payload.download_url
    clip_status = "ready" if clip_url else "pending"
    clip_source = "provided" if clip_url else "local_rtsp"

    entry = TimelineEntry(
        entry_id=payload.event_id or f"{resolved_channel}_{event_timestamp.isoformat()}_{event_type}",
        timestamp=event_timestamp,
        channel=resolved_channel,
        event_type=event_type,
        clip_path=clip_url,
        thumbnail_path=payload.snapshot_url,
        metadata={
            **payload.metadata,
            "title": payload.title or f"{event_type.title()} detected",
            "message": payload.message or f"{event_type.title()} detected on channel {resolved_channel}",
            "camera_name": payload.camera_name,
            "source": payload.source,
            "snapshot_url": payload.snapshot_url,
            "stream_url": stream_url or payload.stream_url,
            "download_url": download_url or payload.download_url,
            "live_url": payload.live_url,
            "duration_seconds": payload.duration_seconds,
            "clip_status": clip_status,
            "clip_source": clip_source,
            "requested_channel": payload.channel,
        },
    )
    entry, created = _upsert_or_merge_timeline_entry(entry)
    await _broadcast_recent_event(entry)

    if created and not clip_url:
        _schedule_clip_generation(entry)
    if created:
        asyncio.create_task(_send_managed_notifications(entry))

    return {
        "status": "accepted",
        "event": _timeline_entry_to_recent(entry),
    }


@app.post("/api/webhook/reolink", summary="Receive a Reolink ONVIF webhook event")
@app.post("/app/api/webhook/reolink", include_in_schema=False)
async def receive_reolink_webhook(request: Request):
    _require_nvr()
    body = (await request.body()).decode("utf-8", errors="ignore")
    if not body.strip():
        raise HTTPException(status_code=400, detail="Empty webhook payload")

    try:
        event_channels = await nvr_host.ONVIF_event_callback(body)
    except Exception as e:
        logger.error("Failed to process ONVIF webhook payload: %s", e)
        raise HTTPException(status_code=400, detail=f"Invalid ONVIF payload: {e}")

    parsed_events = _parse_onvif_event_notifications(body)
    if not parsed_events:
        return {"status": "ignored", "detail": "No supported event types found"}

    created_events: list[dict[str, Any]] = []
    for parsed in parsed_events:
        if event_channels and parsed["channel"] not in event_channels:
            continue
        if not _channel_is_participating(parsed["channel"]):
            continue
        if not _channel_allows_event_type(parsed["channel"], parsed["event_type"]):
            continue
        camera_name = None
        try:
            camera_name = nvr_host.camera_name(parsed["channel"])
        except Exception:
            camera_name = None
        entry, is_new = _create_event_entry(
            channel=parsed["channel"],
            event_type=parsed["event_type"],
            camera_name=camera_name,
            source="nvr_webhook",
            title=f"{parsed['event_type'].title()} detected",
            message=f"{parsed['event_type'].title()} detected on channel {parsed['channel']}",
        )
        await _broadcast_recent_event(entry)
        if is_new:
            _schedule_clip_generation(entry)
            asyncio.create_task(_send_managed_notifications(entry))
        created_events.append(_timeline_entry_to_recent(entry))

    if not created_events:
        return {"status": "ignored", "detail": "No matching channels found"}

    return {"status": "accepted", "events": created_events}


@app.get("/api/timeline/{entry_id}", summary="Get a single timeline entry")
@app.get("/app/api/timeline/{entry_id}", include_in_schema=False)
async def get_timeline_entry(entry_id: str):
    entry = timeline_index.get_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Timeline entry '{entry_id}' not found")
    if not _channel_is_participating(entry.channel) or not _channel_allows_event_type(entry.channel, entry.event_type):
        raise HTTPException(status_code=404, detail=f"Timeline entry '{entry_id}' not found")
    return _timeline_entry_to_recent(entry)


@app.get("/api/events/{entry_id}/clip", summary="Get playable clip for a timeline event")
@app.get("/app/api/events/{entry_id}/clip", include_in_schema=False)
async def get_event_clip(entry_id: str):
    entry = timeline_index.get_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Timeline entry '{entry_id}' not found")

    entry = await _hydrate_event_clip(entry)

    source = _entry_media_source(entry, "clip")
    if not source:
        raise HTTPException(status_code=404, detail="No clip source stored for this event")

    if _is_http_url(source):
        return await _proxy_http_media(source)

    source_path = Path(source)
    if source_path.exists() and source_path.is_file():
        return FileResponse(source_path)

    raise HTTPException(
        status_code=422,
        detail=f"Clip source is not browser-playable: {source}",
    )


@app.get("/api/events/{entry_id}/live", summary="Open a live view for a timeline event")
@app.get("/app/api/events/{entry_id}/live", include_in_schema=False)
async def get_event_live(entry_id: str):
    entry = timeline_index.get_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Timeline entry '{entry_id}' not found")

    source = _entry_media_source(entry, "live")
    if not source:
        raise HTTPException(status_code=404, detail="No live source stored for this event")

    if source.startswith("/"):
        return RedirectResponse(source)

    if _is_http_url(source):
        return RedirectResponse(source)

    raise HTTPException(
        status_code=422,
        detail=f"Live source is not browser-playable: {source}",
    )


async def _live_mjpeg_stream(channel: int, stream: str = "sub"):
    if not nvr_host:
        raise HTTPException(status_code=503, detail="NVR not connected")

    try:
        rtsp_url = await nvr_host.get_rtsp_stream_source(channel, stream=stream)
    except Exception as e:
        logger.error("Failed to resolve live RTSP source for channel %s: %s", channel, e)
        raise HTTPException(status_code=502, detail=f"Failed to resolve live stream: {e}")

    if not rtsp_url:
        raise HTTPException(status_code=404, detail=f"No live stream available for channel {channel}")

    async def frame_generator():
        import asyncio

        proc = await asyncio.create_subprocess_exec(
            FFMPEG_BIN,
            "-hide_banner",
            "-loglevel",
            "error",
            "-rtsp_transport",
            "tcp",
            "-i",
            rtsp_url,
            "-an",
            "-vf",
            "scale=-2:720",
            "-r",
            "8",
            "-f",
            "image2pipe",
            "-vcodec",
            "mjpeg",
            "pipe:1",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        buffer = bytearray()
        try:
            while True:
                chunk = await proc.stdout.read(64 * 1024)
                if not chunk:
                    break
                buffer.extend(chunk)
                while True:
                    start = buffer.find(b"\xff\xd8")
                    end = buffer.find(b"\xff\xd9", start + 2 if start != -1 else 0)
                    if start == -1 or end == -1 or end <= start:
                        break
                    frame = bytes(buffer[start:end + 2])
                    del buffer[:end + 2]
                    yield (
                        b"--frame\r\n"
                        b"Content-Type: image/jpeg\r\n"
                        + f"Content-Length: {len(frame)}\r\n\r\n".encode()
                        + frame
                        + b"\r\n"
                    )
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except Exception:
                    proc.kill()
                    await proc.wait()

    return StreamingResponse(
        frame_generator(),
        media_type="multipart/x-mixed-replace; boundary=frame",
    )


@app.get("/api/live/{channel}/mjpeg", summary="Live MJPEG stream for a camera channel")
@app.get("/app/api/live/{channel}/mjpeg", include_in_schema=False)
async def get_live_mjpeg(channel: int, stream: str = Query("sub", description="Stream quality: 'sub' or 'main'")):
    if channel < 0 or channel >= (nvr_host.num_channels if nvr_host else 0):
        raise HTTPException(status_code=400, detail="Invalid channel")
    if not _channel_is_participating(channel):
        raise HTTPException(status_code=403, detail=f"Channel {channel} is not enabled in Watchtower.")
    if stream not in ("sub", "main"):
        raise HTTPException(status_code=400, detail="stream must be 'sub' or 'main'")
    return await _live_mjpeg_stream(channel, stream=stream)


def _live_dashboard_html(channel: int, event_type: Optional[str] = None) -> str:
    event_label = html.escape(event_type) if event_type else ""
    camera_label = html.escape(_channel_name(channel) or f"Channel {channel}")
    subtitle = f"{event_label} • {camera_label}" if event_label else camera_label
    camera_link_items: list[str] = []
    for info in available_channels:
        info_channel = info.get("channel")
        if info_channel not in participating_channels:
            continue
        info_name = html.escape(info.get("name") or f"Channel {info_channel}")
        active_class = " active" if info_channel == channel else ""
        camera_link_items.append(
            f'<a class="chip{active_class}" href="/app/live?channel={info_channel}">{info_name}</a>'
        )
    camera_links = "".join(camera_link_items)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{LIVE_PAGE_TITLE}</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0f1115;
      --panel: #171b22;
      --line: #263041;
      --text: #e6edf3;
      --muted: #9aa7b7;
      --accent: #5aa9ff;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ height: 100%; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    main {{
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      padding: 0.75rem;
    }}
    .topbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem;
      padding: 0.75rem 0;
    }}
    .title {{
      display: flex;
      flex-direction: column;
      gap: 0.2rem;
      min-width: 0;
    }}
    .title strong {{
      font-size: 1.1rem;
      line-height: 1.2;
    }}
    .title span {{
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 0.4rem 0.7rem;
      color: var(--text);
      text-decoration: none;
      background: rgba(255,255,255,0.03);
      white-space: nowrap;
    }}
    .chip.active {{
      border-color: var(--accent);
      color: var(--accent);
    }}
    .camera-list {{
      display: flex;
      gap: 0.5rem;
      overflow-x: auto;
      padding-bottom: 0.15rem;
    }}
    .viewer {{
      flex: 1;
      min-height: 0;
      border: 1px solid var(--line);
      border-radius: 0.75rem;
      overflow: hidden;
      background: #000;
    }}
    .viewer img {{
      width: 100%;
      height: 100%;
      object-fit: contain;
      display: block;
    }}
    .meta {{
      display: grid;
      gap: 0.35rem;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
    }}
  </style>
</head>
<body>
  <main>
    <div class="topbar">
      <div class="title">
        <strong>{LIVE_PAGE_TITLE}</strong>
        <span>{subtitle}</span>
      </div>
      <a class="chip" href="/app">Back to events</a>
    </div>
    <div class="camera-list">{camera_links}</div>
    <div class="viewer">
      <img src="api/live/{channel}/mjpeg" alt="Live stream for channel {channel}">
    </div>
    <div class="meta">
      <div>Live stream is served inside the app.</div>
      <div>If the stream is unavailable, reload after a few seconds.</div>
    </div>
    <div class="actions">
      <a class="chip" href="/app">Events</a>
      <a class="chip" href="api/live/{channel}/mjpeg?stream=main" target="_blank" rel="noreferrer">Open main stream</a>
    </div>
  </main>
</body>
</html>"""


@app.get("/live", response_class=HTMLResponse, summary="Open the live camera page")
@app.get("/live/", response_class=HTMLResponse, include_in_schema=False)
@app.get("/app/live", response_class=HTMLResponse, include_in_schema=False)
@app.get("/app/live/", response_class=HTMLResponse, include_in_schema=False)
async def app_live(channel: Optional[int] = Query(None, description="Camera channel number"), event_type: Optional[str] = Query(None)):
    channel = _resolve_live_channel(channel)
    if channel is None:
        raise HTTPException(status_code=400, detail="Invalid channel")
    return HTMLResponse(_live_dashboard_html(channel=channel, event_type=event_type))


def _dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Watchtower</title>
    <style>
    :root {
      color-scheme: dark;
      --bg: #0f1115;
      --panel: #171b22;
      --line: #263041;
      --text: #e6edf3;
      --muted: #9aa7b7;
      --accent: #5aa9ff;
      --warn: #ffcb6b;
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap: 12px;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      background: rgba(15,17,21,.96);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    header h1 { margin: 0; font-size: 18px; line-height: 1.2; }
    header .meta { color: var(--muted); font-size: 13px; line-height: 1.3; }
    main {
      display: grid;
      grid-template-columns: minmax(320px, 360px) minmax(0, 1fr);
      height: calc(100vh - 65px);
      overflow: hidden;
    }
    aside {
      border-right: 1px solid var(--line);
      background: var(--panel);
      overflow: auto;
      -webkit-overflow-scrolling: touch;
      min-height: 0;
    }
    section.player {
      display: grid;
      grid-template-rows: auto minmax(0, 1fr) auto;
      min-height: 0;
      overflow: hidden;
    }
    .toolbar {
      display:flex;
      gap: 8px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      align-items:center;
      flex-wrap: wrap;
    }
    .chip, button {
      border: 1px solid var(--line);
      background: #12161d;
      color: var(--text);
      min-height: 42px;
      padding: 10px 12px;
      border-radius: 10px;
      cursor: pointer;
      font: inherit;
      touch-action: manipulation;
    }
    .chip.active { border-color: var(--accent); color: var(--accent); }
    .events { list-style:none; margin:0; padding:0; }
    .event {
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      cursor: pointer;
      min-height: 64px;
    }
    .event:hover { background: rgba(90,169,255,.08); }
    .event.active { background: rgba(90,169,255,.15); }
    .event .top { display:flex; justify-content:space-between; gap: 12px; font-size: 14px; align-items: flex-start; }
    .event .time { color: var(--muted); font-size: 12px; margin-top: 4px; line-height: 1.3; }
    .badge { font-size: 11px; text-transform: uppercase; letter-spacing: .04em; padding: 3px 7px; border-radius: 999px; background: rgba(90,169,255,.16); color: #d5ebff; white-space: nowrap; }
    .badge.doorbell { background: rgba(255,203,107,.18); color: #ffe4a0; }
    .player-wrap { padding: 16px; display:grid; gap: 14px; min-height: 0; overflow: hidden; }
    video, img.preview {
      width: 100%;
      max-height: 64vh;
      background: #000;
      border: 1px solid var(--line);
      border-radius: 10px;
      object-fit: contain;
    }
    .details {
      display: grid;
      gap: 8px;
      padding: 0 16px 16px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
      overflow: hidden;
    }
    .details strong { color: var(--text); }
    .detail-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px 10px;
      align-items: center;
    }
    .detail-pill {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.3rem 0.55rem;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255,255,255,0.03);
      color: var(--text);
      font-size: 12px;
    }
    .detail-pill.doorbell {
      background: rgba(255,203,107,.18);
      color: #ffe4a0;
    }
    .detail-note {
      color: var(--muted);
      font-size: 13px;
    }
    .empty { padding: 20px; color: var(--muted); }
    .row { display:flex; gap: 8px; flex-wrap: wrap; align-items:center; }
    .muted { color: var(--muted); }
    a.chip { display: inline-flex; align-items: center; text-decoration: none; }
    @media (max-width: 900px) {
      main {
        grid-template-columns: 1fr;
        grid-template-rows: minmax(340px, 58vh) minmax(0, 1fr);
      }
      aside { border-right: 0; border-bottom: 1px solid var(--line); order: 2; }
      section.player { order: 1; }
      header { align-items: flex-start; flex-direction: column; }
      .toolbar { overflow-x: auto; flex-wrap: nowrap; }
      video, img.preview {
        max-height: none;
        height: min(100%, 58vh);
      }
      .player-wrap { padding: 12px; }
      .details { padding: 0 12px 12px; font-size: 13px; }
      .event { padding: 16px; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Watchtower</h1>
      <div class="meta">Recent camera events with player-first playback</div>
    </div>
    <div class="meta" id="status">Connecting…</div>
  </header>
  <main>
    <aside>
      <div class="toolbar" id="eventFilters"></div>
      <div class="toolbar" id="cameraFilters">
        <button class="chip active" data-channel-filter="ALL">All Cameras</button>
      </div>
      <ul id="events" class="events"></ul>
    </aside>
    <section class="player">
      <div class="toolbar">
        <div class="row">
          <button id="refresh">Refresh</button>
          <span class="muted" id="count">0 events</span>
        </div>
        <a id="openLive" class="chip" href="/app/live">Open Live</a>
      </div>
      <div class="player-wrap">
        <video id="player" controls playsinline preload="metadata"></video>
        <img id="snapshot" class="preview" alt="Event snapshot" hidden>
      </div>
      <div class="details" id="details">
        <div class="empty">No events loaded.</div>
      </div>
    </section>
  </main>
  <script>
    const knownEventTypes = ['PERSON', 'DOORBELL', 'MOTION', 'ANIMAL', 'VEHICLE'];
    const eventTypeLabels = {
      PERSON: 'Person',
      DOORBELL: 'Doorbell',
      MOTION: 'Motion',
      ANIMAL: 'Animal',
      VEHICLE: 'Vehicle',
    };
    const state = { events: [], channels: [], supportedEventTypes: knownEventTypes, filter: 'ALL', channel: 'ALL', selected: null, socket: null, defaultLiveChannel: null };
    const deepLink = new URLSearchParams(window.location.search);
    const requestedEventType = (() => {
      const raw = (deepLink.get('event_type') || '').trim().toUpperCase();
      return knownEventTypes.includes(raw) ? raw : null;
    })();
    const requestedChannel = (() => {
      const raw = (deepLink.get('channel') || '').trim();
      if (!raw) return 'ALL';
      const parsed = Number.parseInt(raw, 10);
      return Number.isFinite(parsed) ? parsed : 'ALL';
    })();
    const requestedEventId = (deepLink.get('event_id') || '').trim() || null;
    if (requestedEventType) state.filter = requestedEventType;
    state.channel = requestedChannel;
    const elEvents = document.getElementById('events');
    const elCount = document.getElementById('count');
    const elStatus = document.getElementById('status');
    const elEventFilters = document.getElementById('eventFilters');
    const elCameraFilters = document.getElementById('cameraFilters');
    const elOpenLive = document.getElementById('openLive');
    const player = document.getElementById('player');
    const snapshot = document.getElementById('snapshot');
    const details = document.getElementById('details');

    function apiUrl(path) {
      return new URL(path, window.location.href).toString();
    }

    function wsUrl(path) {
      const url = new URL(path, window.location.href);
      url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      return url.toString();
    }

    function badgeClass(eventType) {
      return eventType === 'DOORBELL' ? 'badge doorbell' : 'badge';
    }

    function renderEventFilters() {
      const enabledEventTypes = state.supportedEventTypes.filter(eventType =>
        state.channels.some(info => (info.allowed_event_types || []).includes(eventType))
      );
      if (state.filter !== 'ALL' && !enabledEventTypes.includes(state.filter)) {
        state.filter = 'ALL';
      }
      const buttons = [
        `<button class="chip ${state.filter === 'ALL' ? 'active' : ''}" data-filter="ALL">All</button>`,
        ...enabledEventTypes.map(eventType => `
          <button class="chip ${state.filter === eventType ? 'active' : ''}" data-filter="${eventType}">
            ${escapeHtml(eventTypeLabels[eventType] || eventType)}
          </button>
        `),
      ];
      elEventFilters.innerHTML = buttons.join('');
    }

    function formatTime(ts) {
      try { return new Date(ts).toLocaleString(); } catch (e) { return ts; }
    }

    function sortNewestFirst(events) {
      return [...events].sort((a, b) => {
        const aTime = Date.parse(a.timestamp || '') || 0;
        const bTime = Date.parse(b.timestamp || '') || 0;
        if (bTime !== aTime) return bTime - aTime;
        return String(b.entry_id || '').localeCompare(String(a.entry_id || ''));
      });
    }

    function escapeHtml(value) {
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function cacheBust(url, token) {
      if (!url) return '';
      const separator = url.includes('?') ? '&' : '?';
      return `${url}${separator}v=${encodeURIComponent(token)}`;
    }

    function activeChannelName(channel) {
      const info = state.channels.find(item => item.channel === channel);
      return info?.name || `Channel ${channel}`;
    }

    function updateLiveLink(entry = null) {
      const channel = entry?.channel ?? (state.channel !== 'ALL' ? state.channel : state.defaultLiveChannel);
      elOpenLive.href = channel === null || channel === undefined ? '/app/live' : `/app/live?channel=${channel}`;
    }

    function renderChannelFilters() {
      const buttons = [
        `<button class="chip ${state.channel === 'ALL' ? 'active' : ''}" data-channel-filter="ALL">All Cameras</button>`,
        ...state.channels.map(info => `
          <button class="chip ${state.channel === info.channel ? 'active' : ''}" data-channel-filter="${info.channel}">
            ${escapeHtml(info.name || `Channel ${info.channel}`)}
          </button>
        `),
      ];
      elCameraFilters.innerHTML = buttons.join('');
    }

    function visibleEvents() {
      let events = state.filter === 'ALL' ? state.events : state.events.filter(e => e.event_type === state.filter);
      if (state.channel !== 'ALL') {
        events = events.filter(e => e.channel === state.channel);
      }
      return sortNewestFirst(events);
    }

    function render() {
      renderEventFilters();
      renderChannelFilters();
      const events = visibleEvents();
      elCount.textContent = `${events.length} event${events.length === 1 ? '' : 's'}`;
      elEvents.innerHTML = events.map(e => `
        <li class="event ${state.selected === e.entry_id ? 'active' : ''}" data-id="${escapeHtml(e.entry_id)}">
          <div class="top">
            <strong>${e.title || e.event_type}</strong>
            <span class="${badgeClass(e.event_type)}">${e.event_type}</span>
          </div>
          <div class="time">${formatTime(e.timestamp)}${e.camera_name ? ` • ${e.camera_name}` : ''}</div>
        </li>`).join('') || '<li class="empty">No recent events.</li>';

      if ((!state.selected || !events.find(e => e.entry_id === state.selected)) && events.length) {
        selectEvent(events[0].entry_id, false);
        return;
      }
      updateLiveLink(state.events.find(e => e.entry_id === state.selected) || null);
    }

    function setStatus(text) { elStatus.textContent = text; }

    async function loadChannels() {
      const resp = await fetch(apiUrl('api/camera-config'), { cache: 'no-store' });
      const data = await resp.json();
      state.channels = (data.available_channels || []).filter(info => info.participating);
      state.supportedEventTypes = data.supported_event_types || knownEventTypes;
      state.defaultLiveChannel = data.default_live_channel;
      if (state.channel !== 'ALL' && !state.channels.find(info => info.channel === state.channel)) {
        state.channel = 'ALL';
      }
      renderEventFilters();
      renderChannelFilters();
      updateLiveLink();
    }

    async function loadRecent() {
      const resp = await fetch(apiUrl('api/events/recent?limit=50'), { cache: 'no-store' });
      const data = await resp.json();
      state.events = sortNewestFirst(data.events || []);
      if (requestedEventId) {
        const requestedEvent = await loadRequestedEvent();
        if (requestedEvent) {
          state.events = sortNewestFirst([requestedEvent, ...state.events.filter(e => e.entry_id !== requestedEvent.entry_id)]);
        }
      }
      state.selected = requestedEventId && state.events.find(e => e.entry_id === requestedEventId)
        ? requestedEventId
        : (state.events.length ? state.events[0].entry_id : null);
      render();
      if (state.selected) selectEvent(state.selected, false);
    }

    async function loadRequestedEvent() {
      if (!requestedEventId) return null;
      const existing = state.events.find(e => e.entry_id === requestedEventId);
      if (existing) return existing;
      try {
        const resp = await fetch(apiUrl(`api/timeline/${encodeURIComponent(requestedEventId)}`), { cache: 'no-store' });
        if (!resp.ok) return null;
        return await resp.json();
      } catch (err) {
        return null;
      }
    }

    function selectEvent(id, userInitiated = true) {
      const entry = state.events.find(e => e.entry_id === id);
      if (!entry) return;
      state.selected = id;
      render();
      updateLiveLink(entry);
      const clipUrl = entry.clip_url;
      const snapshotUrl = cacheBust(entry.thumbnail_url || entry.metadata?.snapshot_url, entry.entry_id);
      if (clipUrl) {
        snapshot.hidden = true;
        player.hidden = false;
        player.pause();
        player.removeAttribute('src');
        player.load();
        player.poster = snapshotUrl || '';
        player.src = clipUrl;
        player.load();
        if (userInitiated) player.play().catch(() => {});
      } else if (snapshotUrl) {
        player.pause();
        player.removeAttribute('src');
        player.load();
        player.removeAttribute('poster');
        snapshot.src = snapshotUrl;
        snapshot.hidden = false;
        player.hidden = true;
      }
      details.innerHTML = `
        <div class="detail-meta">
          <span class="detail-pill ${entry.event_type === 'DOORBELL' ? 'doorbell' : ''}">${escapeHtml(entry.event_type)}</span>
          <span class="detail-pill">${escapeHtml(formatTime(entry.timestamp))}</span>
          <span class="detail-pill">${escapeHtml(entry.camera_name ? entry.camera_name : activeChannelName(entry.channel))}</span>
          ${entry.clip_status && entry.clip_status !== 'ready' ? `<span class="detail-pill">${escapeHtml(`Clip ${entry.clip_status}`)}</span>` : ''}
        </div>
        ${entry.message && entry.message !== entry.title ? `<div class="detail-note">${escapeHtml(entry.message)}</div>` : ''}
      `;
    }

    elEvents.addEventListener('click', (ev) => {
      const li = ev.target.closest('.event');
      if (!li) return;
      selectEvent(li.dataset.id, true);
    });

    document.querySelectorAll('[data-filter]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('[data-filter]').forEach(el => el.classList.remove('active'));
        btn.classList.add('active');
        state.filter = btn.dataset.filter;
        render();
      });
    });

    elCameraFilters.addEventListener('click', (ev) => {
      const btn = ev.target.closest('[data-channel-filter]');
      if (!btn) return;
      const value = btn.dataset.channelFilter;
      state.channel = value === 'ALL' ? 'ALL' : Number.parseInt(value, 10);
      render();
    });

    document.getElementById('refresh').addEventListener('click', loadRecent);

    function connectSocket() {
      const socket = new WebSocket(wsUrl('ws/events'));
      state.socket = socket;
      socket.onopen = () => setStatus('Live');
      socket.onclose = () => { setStatus('Reconnecting…'); setTimeout(connectSocket, 1500); };
      socket.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.type === 'hello' && Array.isArray(msg.events)) {
          state.events = sortNewestFirst(msg.events);
          render();
          if (state.selected) selectEvent(state.selected, false);
          return;
        }
        if (msg.type === 'event' && msg.event) {
          state.events = sortNewestFirst([msg.event, ...state.events.filter(e => e.entry_id !== msg.event.entry_id)]);
          state.selected = msg.event.entry_id;
          render();
          selectEvent(msg.event.entry_id, false);
        }
      };
    }

    Promise.all([loadChannels(), loadRecent()]).then(() => {
      connectSocket();
    }).catch(err => {
      setStatus('Offline');
      details.innerHTML = `<div class="empty">Failed to load recent events: ${err}</div>`;
    });
  </script>
</body>
</html>"""


def _dashboard_html_v2() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Watchtower</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0f1115;
      --panel: #171b22;
      --panel-2: #12161d;
      --line: #263041;
      --text: #e6edf3;
      --muted: #9aa7b7;
      --accent: #5aa9ff;
      --warn: #ffcb6b;
      --success: #7ed6a5;
      --danger: #ff8a80;
      --shadow: rgba(0, 0, 0, 0.45);
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    [hidden] { display: none !important; }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      background: rgba(15, 17, 21, 0.96);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    header h1 { margin: 0; font-size: 18px; line-height: 1.2; }
    header .meta { color: var(--muted); font-size: 13px; line-height: 1.3; }
    main {
      display: grid;
      grid-template-columns: minmax(320px, 360px) minmax(0, 1fr);
      height: calc(100vh - 65px);
      overflow: hidden;
    }
    aside {
      border-right: 1px solid var(--line);
      background: var(--panel);
      overflow: auto;
      -webkit-overflow-scrolling: touch;
      min-height: 0;
    }
    section.player {
      display: grid;
      grid-template-rows: auto minmax(0, 1fr) auto;
      min-height: 0;
      overflow: hidden;
    }
    .toolbar {
      display: flex;
      gap: 8px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      align-items: center;
      flex-wrap: wrap;
    }
    .toolbar.space-between { justify-content: space-between; }
    .chip,
    button,
    input,
    select,
    textarea {
      border: 1px solid var(--line);
      background: var(--panel-2);
      color: var(--text);
      border-radius: 10px;
      font: inherit;
    }
    .chip,
    button {
      min-height: 42px;
      padding: 10px 12px;
      cursor: pointer;
      touch-action: manipulation;
    }
    button.primary {
      background: rgba(90, 169, 255, 0.16);
      border-color: rgba(90, 169, 255, 0.6);
      color: #d5ebff;
    }
    button.ghost {
      background: transparent;
    }
    button:disabled {
      opacity: 0.55;
      cursor: not-allowed;
    }
    .chip.active { border-color: var(--accent); color: var(--accent); }
    .events { list-style: none; margin: 0; padding: 0; }
    .event {
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      cursor: pointer;
      min-height: 64px;
    }
    .event:hover { background: rgba(90, 169, 255, 0.08); }
    .event.active { background: rgba(90, 169, 255, 0.15); }
    .event .top {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      font-size: 14px;
      align-items: flex-start;
    }
    .event .time {
      color: var(--muted);
      font-size: 12px;
      margin-top: 4px;
      line-height: 1.3;
    }
    .badge {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      padding: 3px 7px;
      border-radius: 999px;
      background: rgba(90, 169, 255, 0.16);
      color: #d5ebff;
      white-space: nowrap;
    }
    .badge.doorbell { background: rgba(255, 203, 107, 0.18); color: #ffe4a0; }
    .badge.ok { background: rgba(126, 214, 165, 0.16); color: #baf2d0; }
    .badge.warn { background: rgba(255, 203, 107, 0.18); color: #ffe4a0; }
    .badge.error { background: rgba(255, 138, 128, 0.18); color: #ffc7c2; }
    .player-wrap {
      padding: 16px;
      display: grid;
      gap: 14px;
      min-height: 0;
      overflow: hidden;
    }
    video,
    img.preview {
      width: 100%;
      max-height: 64vh;
      background: #000;
      border: 1px solid var(--line);
      border-radius: 10px;
      object-fit: contain;
    }
    .details {
      display: grid;
      gap: 8px;
      padding: 0 16px 16px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
      overflow: hidden;
    }
    .detail-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px 10px;
      align-items: center;
    }
    .detail-pill {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.3rem 0.55rem;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.03);
      color: var(--text);
      font-size: 12px;
    }
    .detail-pill.doorbell {
      background: rgba(255, 203, 107, 0.18);
      color: #ffe4a0;
    }
    .detail-note { color: var(--muted); font-size: 13px; }
    .empty { padding: 20px; color: var(--muted); }
    .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .muted { color: var(--muted); }
    a.chip {
      display: inline-flex;
      align-items: center;
      text-decoration: none;
    }
    .settings-shell {
      position: fixed;
      inset: 0;
      display: flex;
      justify-content: flex-end;
      background: rgba(3, 6, 10, 0.6);
      backdrop-filter: blur(4px);
      z-index: 20;
    }
    .settings-panel {
      width: min(680px, 100vw);
      height: 100vh;
      background: #10151c;
      border-left: 1px solid var(--line);
      box-shadow: -20px 0 60px var(--shadow);
      display: grid;
      grid-template-rows: auto minmax(0, 1fr) auto;
      overflow: hidden;
    }
    .settings-header,
    .settings-footer {
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      background: rgba(16, 21, 28, 0.98);
    }
    .settings-footer {
      border-bottom: 0;
      border-top: 1px solid var(--line);
    }
    .settings-body {
      overflow: auto;
      padding: 16px;
      display: grid;
      gap: 16px;
      align-content: start;
    }
    .settings-card {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--panel);
      padding: 14px;
      display: grid;
      gap: 12px;
    }
    .settings-card h3,
    .settings-card h4 {
      margin: 0;
      font-size: 15px;
    }
    .settings-card p {
      margin: 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    .field {
      display: grid;
      gap: 6px;
    }
    .field label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    input[type="text"],
    input[type="number"],
    textarea,
    select {
      width: 100%;
      min-height: 42px;
      padding: 10px 12px;
    }
    textarea {
      min-height: 80px;
      resize: vertical;
    }
    .toggle {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.03);
    }
    .toggle input {
      width: 20px;
      height: 20px;
      accent-color: var(--accent);
    }
    .service-grid,
    .event-rule-grid {
      display: grid;
      gap: 10px;
    }
    .service-option,
    .event-rule {
      display: grid;
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(255, 255, 255, 0.03);
    }
    .inline-check {
      display: flex;
      gap: 10px;
      align-items: center;
    }
    .inline-check input {
      width: 18px;
      height: 18px;
      accent-color: var(--accent);
      flex: 0 0 auto;
    }
    .rule-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 110px;
      gap: 10px;
      align-items: center;
    }
    .camera-card {
      display: grid;
      gap: 12px;
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: rgba(255, 255, 255, 0.02);
    }
    .camera-card.disabled {
      opacity: 0.68;
    }
    .camera-header {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }
    .camera-title {
      display: grid;
      gap: 4px;
    }
    .camera-title small {
      color: var(--muted);
      font-size: 12px;
    }
    .service-note,
    .settings-note {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }
    .header-actions {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }
    .spacer { flex: 1 1 auto; }
    @media (max-width: 900px) {
      main {
        grid-template-columns: 1fr;
        grid-template-rows: minmax(340px, 58vh) minmax(0, 1fr);
      }
      aside {
        border-right: 0;
        border-bottom: 1px solid var(--line);
        order: 2;
      }
      section.player { order: 1; }
      header {
        align-items: flex-start;
        flex-direction: column;
      }
      .toolbar { overflow-x: auto; flex-wrap: nowrap; }
      video,
      img.preview {
        max-height: none;
        height: min(100%, 58vh);
      }
      .player-wrap { padding: 12px; }
      .details { padding: 0 12px 12px; font-size: 13px; }
      .event { padding: 16px; }
      .settings-panel { width: 100vw; }
      .rule-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Watchtower</h1>
      <div class="meta">Recent camera events with player-first playback</div>
    </div>
    <div class="header-actions">
      <button id="openSettings" class="chip">Notification Settings</button>
      <div class="meta" id="status">Connecting...</div>
    </div>
  </header>
  <main>
    <aside>
      <div class="toolbar" id="eventFilters"></div>
      <div class="toolbar" id="cameraFilters">
        <button class="chip active" data-channel-filter="ALL">All Cameras</button>
      </div>
      <ul id="events" class="events"></ul>
    </aside>
    <section class="player">
      <div class="toolbar space-between">
        <div class="row">
          <button id="refresh">Refresh</button>
          <span class="muted" id="count">0 events</span>
        </div>
        <div class="row">
          <a id="openLive" class="chip" href="/app/live">Open Live</a>
        </div>
      </div>
      <div class="player-wrap">
        <video id="player" controls playsinline preload="metadata"></video>
        <img id="snapshot" class="preview" alt="Event snapshot" hidden>
      </div>
      <div class="details" id="details">
        <div class="empty">No events loaded.</div>
      </div>
    </section>
  </main>

  <div id="settingsShell" class="settings-shell" hidden>
    <div class="settings-panel">
      <div class="settings-header">
        <div class="row">
          <div>
            <strong>Managed Notifications</strong>
            <div class="meta">Configure mobile notifications directly in Watchtower.</div>
          </div>
          <div class="spacer"></div>
          <button id="closeSettings" class="ghost">Close</button>
        </div>
      </div>
      <div class="settings-body" id="settingsBody"></div>
      <div class="settings-footer">
        <div class="row">
          <span class="muted" id="settingsStatus">Loading settings...</span>
          <div class="spacer"></div>
          <button id="sendTestNotification" class="ghost">Send Test</button>
          <button id="saveSettings" class="primary">Save Settings</button>
        </div>
      </div>
    </div>
  </div>

  <script>
    const knownEventTypes = ['PERSON', 'DOORBELL', 'MOTION', 'ANIMAL', 'VEHICLE'];
    const eventTypeLabels = {
      PERSON: 'Person',
      DOORBELL: 'Doorbell',
      MOTION: 'Motion',
      ANIMAL: 'Animal',
      VEHICLE: 'Vehicle',
    };
    const state = {
      events: [],
      channels: [],
      supportedEventTypes: knownEventTypes,
      filter: 'ALL',
      channel: 'ALL',
      selected: null,
      socket: null,
      defaultLiveChannel: null,
      notifications: null,
      haStatus: null,
      haEntities: { binary_sensors: [], cameras: [] },
      settingsLoaded: false,
      settingsSaving: false,
    };

    const deepLink = new URLSearchParams(window.location.search);
    const requestedEventType = (() => {
      const raw = (deepLink.get('event_type') || '').trim().toUpperCase();
      return knownEventTypes.includes(raw) ? raw : null;
    })();
    const requestedChannel = (() => {
      const raw = (deepLink.get('channel') || '').trim();
      if (!raw) return 'ALL';
      const parsed = Number.parseInt(raw, 10);
      return Number.isFinite(parsed) ? parsed : 'ALL';
    })();
    const requestedEventId = (deepLink.get('event_id') || '').trim() || null;
    if (requestedEventType) state.filter = requestedEventType;
    state.channel = requestedChannel;

    const elEvents = document.getElementById('events');
    const elCount = document.getElementById('count');
    const elStatus = document.getElementById('status');
    const elEventFilters = document.getElementById('eventFilters');
    const elCameraFilters = document.getElementById('cameraFilters');
    const elOpenLive = document.getElementById('openLive');
    const player = document.getElementById('player');
    const snapshot = document.getElementById('snapshot');
    const details = document.getElementById('details');
    const settingsShell = document.getElementById('settingsShell');
    const settingsBody = document.getElementById('settingsBody');
    const settingsStatus = document.getElementById('settingsStatus');
    const saveSettingsButton = document.getElementById('saveSettings');
    const sendTestButton = document.getElementById('sendTestNotification');

    function apiUrl(path) {
      return new URL(path, window.location.href).toString();
    }

    function wsUrl(path) {
      const url = new URL(path, window.location.href);
      url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      return url.toString();
    }

    function badgeClass(eventType) {
      return eventType === 'DOORBELL' ? 'badge doorbell' : 'badge';
    }

    function formatTime(ts) {
      try { return new Date(ts).toLocaleString(); } catch (e) { return ts; }
    }

    function sortNewestFirst(events) {
      return [...events].sort((a, b) => {
        const aTime = Date.parse(a.timestamp || '') || 0;
        const bTime = Date.parse(b.timestamp || '') || 0;
        if (bTime !== aTime) return bTime - aTime;
        return String(b.entry_id || '').localeCompare(String(a.entry_id || ''));
      });
    }

    function escapeHtml(value) {
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function cacheBust(url, token) {
      if (!url) return '';
      const separator = url.includes('?') ? '&' : '?';
      return `${url}${separator}v=${encodeURIComponent(token)}`;
    }

    function activeChannelName(channel) {
      const info = state.channels.find(item => item.channel === channel);
      return info?.name || `Channel ${channel}`;
    }

    function updateLiveLink(entry = null) {
      const channel = entry?.channel ?? (state.channel !== 'ALL' ? state.channel : state.defaultLiveChannel);
      elOpenLive.href = channel === null || channel === undefined ? '/app/live' : `/app/live?channel=${channel}`;
    }

    function normalizeRule(eventType, rule = {}) {
      return {
        enabled: !!rule.enabled,
        cooldown_seconds: Number.isFinite(Number(rule.cooldown_seconds)) ? Math.max(Number(rule.cooldown_seconds), 0) : (eventType === 'DOORBELL' ? 0 : 45),
        title_template: typeof rule.title_template === 'string' ? rule.title_template : '',
        message_template: typeof rule.message_template === 'string' ? rule.message_template : '',
      };
    }

    function emptyNotificationSettings() {
      return {
        enabled: false,
        default_notify_services: [],
        preferred_test_service: '',
        ai: {
          enabled: false,
          provider: 'openai',
          api_key: '',
          model: 'gpt-4.1-mini',
          detail: 'low',
          timeout_seconds: 8,
          confidence_threshold: 0.78,
          daily_event_cap: 100,
          include_fun_summary: true,
          fun_style: 'playful',
        },
        known_subjects: [],
        cameras: [],
      };
    }

    function emptyKnownSubject() {
      return {
        enabled: true,
        name: '',
        subject_type: 'other',
        description: '',
        channels: [],
        event_types: [],
      };
    }

    function channelAllowedEventTypes(channel) {
      const info = state.channels.find(item => item.channel === channel);
      return (info?.allowed_event_types || state.supportedEventTypes).filter(eventType => state.supportedEventTypes.includes(eventType));
    }

    function syncNotificationSettings() {
      const base = state.notifications || emptyNotificationSettings();
      const byChannel = new Map((base.cameras || []).map(camera => [camera.channel, camera]));
      const syncedCameras = state.channels.map((channelInfo) => {
        const existing = byChannel.get(channelInfo.channel) || {};
        const allowedEventTypes = channelAllowedEventTypes(channelInfo.channel);
        const rules = {};
        for (const eventType of allowedEventTypes) {
          rules[eventType] = normalizeRule(eventType, existing.rules?.[eventType]);
          if (existing.rules?.[eventType] === undefined) {
            rules[eventType].enabled = true;
          }
        }
        return {
          channel: channelInfo.channel,
          camera_name: existing.camera_name || channelInfo.name || `Channel ${channelInfo.channel}`,
          enabled: existing.enabled !== undefined ? !!existing.enabled : true,
          notify_services: Array.isArray(existing.notify_services) ? [...existing.notify_services] : [],
          doorbell_action: {
            enabled: !!existing.doorbell_action?.enabled,
            title: typeof existing.doorbell_action?.title === 'string' && existing.doorbell_action.title.trim() ? existing.doorbell_action.title.trim() : 'Unlock Front Door',
            service: typeof existing.doorbell_action?.service === 'string' && existing.doorbell_action.service.trim() ? existing.doorbell_action.service.trim() : 'lock.unlock',
            entity_id: typeof existing.doorbell_action?.entity_id === 'string' ? existing.doorbell_action.entity_id.trim() : '',
          },
          ha_source: {
            person_entity_id: typeof existing.ha_source?.person_entity_id === 'string' ? existing.ha_source.person_entity_id.trim() : '',
            doorbell_entity_id: typeof existing.ha_source?.doorbell_entity_id === 'string' ? existing.ha_source.doorbell_entity_id.trim() : '',
            animal_entity_id: typeof existing.ha_source?.animal_entity_id === 'string' ? existing.ha_source.animal_entity_id.trim() : '',
            vehicle_entity_id: typeof existing.ha_source?.vehicle_entity_id === 'string' ? existing.ha_source.vehicle_entity_id.trim() : '',
            snapshot_camera_entity_id: typeof existing.ha_source?.snapshot_camera_entity_id === 'string' ? existing.ha_source.snapshot_camera_entity_id.trim() : '',
          },
          ai: {
            enabled: !!existing.ai?.enabled,
            event_types: Array.isArray(existing.ai?.event_types)
              ? existing.ai.event_types.filter(eventType => allowedEventTypes.includes(eventType))
              : allowedEventTypes.filter(eventType => ['DOORBELL', 'PERSON', 'ANIMAL', 'VEHICLE'].includes(eventType)),
          },
          rules,
        };
      });
      state.notifications = {
        enabled: !!base.enabled,
        default_notify_services: Array.isArray(base.default_notify_services) ? [...base.default_notify_services] : [],
        preferred_test_service: typeof base.preferred_test_service === 'string' ? base.preferred_test_service.trim() : '',
        ai: {
          enabled: !!base.ai?.enabled,
          provider: typeof base.ai?.provider === 'string' && base.ai.provider.trim() ? base.ai.provider.trim() : 'openai',
          api_key: typeof base.ai?.api_key === 'string' ? base.ai.api_key : '',
          model: typeof base.ai?.model === 'string' && base.ai.model.trim() ? base.ai.model.trim() : 'gpt-4.1-mini',
          detail: base.ai?.detail === 'high' ? 'high' : 'low',
          timeout_seconds: Number.isFinite(Number(base.ai?.timeout_seconds)) ? Math.max(Number(base.ai.timeout_seconds), 3) : 8,
          confidence_threshold: Number.isFinite(Number(base.ai?.confidence_threshold)) ? Math.min(Math.max(Number(base.ai.confidence_threshold), 0), 1) : 0.78,
          daily_event_cap: Number.isFinite(Number(base.ai?.daily_event_cap)) ? Math.max(Number(base.ai.daily_event_cap), 0) : 100,
          include_fun_summary: base.ai?.include_fun_summary !== false,
          fun_style: typeof base.ai?.fun_style === 'string' && ['off', 'mild', 'playful'].includes(base.ai.fun_style) ? base.ai.fun_style : 'playful',
        },
        known_subjects: Array.isArray(base.known_subjects)
          ? base.known_subjects.map(subject => ({
              enabled: subject?.enabled !== false,
              name: typeof subject?.name === 'string' ? subject.name.trim() : '',
              subject_type: typeof subject?.subject_type === 'string' && subject.subject_type.trim() ? subject.subject_type.trim() : 'other',
              description: typeof subject?.description === 'string' ? subject.description.trim() : '',
              channels: Array.isArray(subject?.channels)
                ? subject.channels.map(value => Number.parseInt(value, 10)).filter(value => Number.isFinite(value))
                : [],
              event_types: Array.isArray(subject?.event_types)
                ? subject.event_types.filter(eventType => state.supportedEventTypes.includes(eventType))
                : [],
            }))
          : [],
        cameras: syncedCameras,
      };
    }

    function notificationCamera(channel) {
      return state.notifications?.cameras?.find(camera => camera.channel === channel) || null;
    }

    function availableNotifyServices() {
      return state.haStatus?.discovered_mobile_notify_services || [];
    }

    function serviceCheckboxList(selectedServices, attrs) {
      const services = availableNotifyServices();
      if (!services.length) {
        return '<div class="settings-note">No mobile app notify services were discovered yet. Open the Home Assistant companion app on your phone first, then reload Watchtower.</div>';
      }
      return `<div class="service-grid">${services.map(service => `
        <label class="service-option">
          <span class="inline-check">
            <input type="checkbox" ${attrs} value="${escapeHtml(service)}" ${selectedServices.includes(service) ? 'checked' : ''}>
            <span>${escapeHtml(service)}</span>
          </span>
        </label>
      `).join('')}</div>`;
    }

    function entityOptions(entities) {
      return (entities || []).map(entity => `
        <option value="${escapeHtml(entity.entity_id)}">${escapeHtml(entity.friendly_name)} (${escapeHtml(entity.entity_id)})</option>
      `).join('');
    }

    function renderEventFilters() {
      const enabledEventTypes = state.supportedEventTypes.filter(eventType =>
        state.channels.some(info => (info.allowed_event_types || []).includes(eventType))
      );
      if (state.filter !== 'ALL' && !enabledEventTypes.includes(state.filter)) {
        state.filter = 'ALL';
      }
      const buttons = [
        `<button class="chip ${state.filter === 'ALL' ? 'active' : ''}" data-filter="ALL">All</button>`,
        ...enabledEventTypes.map(eventType => `
          <button class="chip ${state.filter === eventType ? 'active' : ''}" data-filter="${eventType}">
            ${escapeHtml(eventTypeLabels[eventType] || eventType)}
          </button>
        `),
      ];
      elEventFilters.innerHTML = buttons.join('');
    }

    function renderChannelFilters() {
      const buttons = [
        `<button class="chip ${state.channel === 'ALL' ? 'active' : ''}" data-channel-filter="ALL">All Cameras</button>`,
        ...state.channels.map(info => `
          <button class="chip ${state.channel === info.channel ? 'active' : ''}" data-channel-filter="${info.channel}">
            ${escapeHtml(info.name || `Channel ${info.channel}`)}
          </button>
        `),
      ];
      elCameraFilters.innerHTML = buttons.join('');
    }

    function visibleEvents() {
      let events = state.filter === 'ALL' ? state.events : state.events.filter(e => e.event_type === state.filter);
      if (state.channel !== 'ALL') {
        events = events.filter(e => e.channel === state.channel);
      }
      return sortNewestFirst(events);
    }

    function haStatusBadge() {
      if (!state.haStatus) {
        return '<span class="badge warn">Checking...</span>';
      }
      if (state.haStatus.enabled) {
        return '<span class="badge ok">Connected</span>';
      }
      return '<span class="badge error">Unavailable</span>';
    }

    function renderNotificationSettings() {
      if (!settingsShell || !settingsBody) return;
      if (!state.settingsLoaded || !state.notifications) {
        settingsBody.innerHTML = '<div class="settings-card"><p>Loading notification settings...</p></div>';
        return;
      }

      const settings = state.notifications;
      const aiSettings = settings.ai || emptyNotificationSettings().ai;
      const discoveredServices = availableNotifyServices();
      const binarySensorOptions = entityOptions(state.haEntities.binary_sensors);
      const cameraOptions = entityOptions(state.haEntities.cameras);
      const testServiceOptions = discoveredServices.map(service => `<option value="${escapeHtml(service)}">${escapeHtml(service)}</option>`).join('');
      const knownSubjectCards = (settings.known_subjects || []).map((subject, index) => {
        const scopedAllChannels = !subject.channels || !subject.channels.length;
        const scopedAllEvents = !subject.event_types || !subject.event_types.length;
        return `
          <div class="event-rule">
            <div class="rule-row">
              <label class="inline-check">
                <input type="checkbox" data-known-subject-enabled="${index}" ${subject.enabled ? 'checked' : ''}>
                <span><strong>Known subject ${index + 1}</strong></span>
              </label>
              <button type="button" class="ghost" data-remove-known-subject="${index}">Remove</button>
            </div>
            <div class="field">
              <label>Name</label>
              <input type="text" data-known-subject-name="${index}" value="${escapeHtml(subject.name || '')}" placeholder="Fozzie">
            </div>
            <div class="field">
              <label>Type</label>
              <select data-known-subject-type="${index}">
                ${['dog', 'person', 'role', 'vehicle', 'other'].map(type => `
                  <option value="${type}" ${subject.subject_type === type ? 'selected' : ''}>${escapeHtml(type)}</option>
                `).join('')}
              </select>
            </div>
            <div class="field">
              <label>Description</label>
              <textarea data-known-subject-description="${index}" placeholder="Brown dog with a lighter chest who usually hangs out by the backyard fence.">${escapeHtml(subject.description || '')}</textarea>
            </div>
            <div class="field">
              <label>Applies to cameras</label>
              <div class="service-grid">
                <label class="service-option">
                  <span class="inline-check">
                    <input type="checkbox" data-known-subject-all-channels="${index}" ${scopedAllChannels ? 'checked' : ''}>
                    <span>All cameras</span>
                  </span>
                </label>
                ${state.channels.map(channelInfo => `
                  <label class="service-option">
                    <span class="inline-check">
                      <input type="checkbox" data-known-subject-channel="${index}" value="${channelInfo.channel}" ${subject.channels?.includes(channelInfo.channel) ? 'checked' : ''}>
                      <span>${escapeHtml(channelInfo.name || `Channel ${channelInfo.channel}`)}</span>
                    </span>
                  </label>
                `).join('')}
              </div>
            </div>
            <div class="field">
              <label>Applies to event types</label>
              <div class="service-grid">
                <label class="service-option">
                  <span class="inline-check">
                    <input type="checkbox" data-known-subject-all-events="${index}" ${scopedAllEvents ? 'checked' : ''}>
                    <span>All supported events</span>
                  </span>
                </label>
                ${state.supportedEventTypes.filter(eventType => ['DOORBELL', 'PERSON', 'ANIMAL', 'VEHICLE'].includes(eventType)).map(eventType => `
                  <label class="service-option">
                    <span class="inline-check">
                      <input type="checkbox" data-known-subject-event="${index}" value="${eventType}" ${subject.event_types?.includes(eventType) ? 'checked' : ''}>
                      <span>${escapeHtml(eventTypeLabels[eventType] || eventType)}</span>
                    </span>
                  </label>
                `).join('')}
              </div>
            </div>
          </div>
        `;
      }).join('');

      settingsBody.innerHTML = `
        <datalist id="haBinarySensorOptions">${binarySensorOptions}</datalist>
        <datalist id="haCameraOptions">${cameraOptions}</datalist>
        <div class="settings-card">
          <div class="row">
            <h3>Home Assistant Connection</h3>
            ${haStatusBadge()}
          </div>
          <p>Watchtower can send mobile notifications through the Home Assistant Supervisor proxy. This only works for mobile app notify services.</p>
          <div class="field">
            <label>Discovered Mobile App Services</label>
            <div class="settings-note">${discoveredServices.length ? escapeHtml(discoveredServices.join(', ')) : 'None discovered yet.'}</div>
          </div>
          <div class="field">
            <label>Direct Event Listener</label>
            <div class="settings-note">${state.haStatus?.websocket_listener_running ? 'Running and listening for Home Assistant state changes.' : 'Not connected yet. Watchtower will retry automatically while the add-on is running.'}</div>
          </div>
        </div>

        <div class="settings-card">
          <div class="toggle">
            <div>
              <strong>Enable Watchtower-managed notifications</strong>
              <div class="settings-note">When enabled, Watchtower sends notifications itself after event ingest and clip generation starts.</div>
            </div>
            <input type="checkbox" id="notificationsEnabled" ${settings.enabled ? 'checked' : ''}>
          </div>

          <div class="field">
            <label>Default notify services</label>
            ${serviceCheckboxList(settings.default_notify_services || [], 'data-default-service')}
            <div class="settings-note">These services are used by any camera still set to inherit the default service list.</div>
          </div>

          <div class="field">
            <label for="testService">Test notification service</label>
            <select id="testService" ${!discoveredServices.length ? 'disabled' : ''}>
              ${testServiceOptions || '<option value="">No services available</option>'}
            </select>
          </div>
        </div>

        <div class="settings-card">
          <div class="toggle">
            <div>
              <strong>Enable AI snapshot descriptions</strong>
              <div class="settings-note">When enabled, Watchtower can send selected snapshots to OpenAI to add a short, fun description before the notification is delivered.</div>
            </div>
            <input type="checkbox" id="aiEnabled" ${aiSettings.enabled ? 'checked' : ''}>
          </div>
          <div class="field">
            <label>Provider</label>
            <input type="text" value="OpenAI" disabled>
          </div>
          <div class="field">
            <label for="aiApiKey">OpenAI API key</label>
            <input type="password" id="aiApiKey" value="${escapeHtml(aiSettings.api_key || '')}" placeholder="sk-...">
            <div class="settings-note">Stored in Watchtower settings on this Home Assistant system. Leave it blank if you prefer to supply OPENAI_API_KEY through the runtime environment.</div>
          </div>
          <div class="field">
            <label for="aiModel">Model</label>
            <input type="text" id="aiModel" value="${escapeHtml(aiSettings.model || 'gpt-4.1-mini')}" placeholder="gpt-4.1-mini">
          </div>
          <div class="field">
            <label for="aiDetail">Image detail</label>
            <select id="aiDetail">
              <option value="low" ${aiSettings.detail !== 'high' ? 'selected' : ''}>Low cost</option>
              <option value="high" ${aiSettings.detail === 'high' ? 'selected' : ''}>Higher detail</option>
            </select>
          </div>
          <div class="field">
            <label for="aiTimeout">Timeout (seconds)</label>
            <input type="number" id="aiTimeout" min="3" step="1" value="${escapeHtml(aiSettings.timeout_seconds)}">
          </div>
          <div class="field">
            <label for="aiConfidence">Identity confidence threshold</label>
            <input type="number" id="aiConfidence" min="0" max="1" step="0.01" value="${escapeHtml(aiSettings.confidence_threshold)}">
          </div>
          <div class="field">
            <label for="aiDailyCap">Daily AI event cap</label>
            <input type="number" id="aiDailyCap" min="0" step="1" value="${escapeHtml(aiSettings.daily_event_cap)}">
            <div class="settings-note">Set to 0 for no in-memory cap. This helps keep API usage predictable.</div>
          </div>
          <div class="field">
            <label for="aiFunStyle">Fun style</label>
            <select id="aiFunStyle">
              <option value="off" ${aiSettings.fun_style === 'off' ? 'selected' : ''}>Off</option>
              <option value="mild" ${aiSettings.fun_style === 'mild' ? 'selected' : ''}>Mild</option>
              <option value="playful" ${aiSettings.fun_style === 'playful' ? 'selected' : ''}>Playful</option>
            </select>
          </div>
          <label class="inline-check">
            <input type="checkbox" id="aiIncludeFunSummary" ${aiSettings.include_fun_summary ? 'checked' : ''}>
            <span>Allow playful notification copy when the model is confident</span>
          </label>
        </div>

        <div class="settings-card">
          <div class="row">
            <h3>Known subjects</h3>
            <button type="button" class="ghost" data-add-known-subject>Add Subject</button>
          </div>
          <p>Use this to teach Watchtower about recurring people, pets, or roles in plain English. A short description like "brown dog" or "mail carrier with mail bag" is usually enough for a lightweight first pass.</p>
          ${knownSubjectCards || '<div class="settings-note">No known subjects yet. Add one if you want Watchtower to try naming your dogs or spotting recurring visitors.</div>'}
        </div>

        <div class="settings-card">
          <h3>Per-camera rules</h3>
          <p>Each camera can be enabled or muted independently. Event types shown here already respect Watchtower's participating-camera and allowed-event-type configuration.</p>
          ${settings.cameras.map(camera => {
            const usesDefaultServices = !camera.notify_services || camera.notify_services.length === 0;
            const supportsDoorbell = !!camera.rules?.DOORBELL;
            const cameraRules = Object.entries(camera.rules || {}).map(([eventType, rule]) => `
              <div class="event-rule">
                <div class="rule-row">
                  <label class="inline-check">
                    <input type="checkbox" data-rule-enabled="${camera.channel}:${eventType}" ${rule.enabled ? 'checked' : ''}>
                    <span><strong>${escapeHtml(eventTypeLabels[eventType] || eventType)}</strong></span>
                  </label>
                  <div class="field">
                    <label>Cooldown (s)</label>
                    <input type="number" min="0" step="1" data-rule-cooldown="${camera.channel}:${eventType}" value="${escapeHtml(rule.cooldown_seconds)}">
                  </div>
                </div>
                <div class="field">
                  <label>Title template</label>
                  <input type="text" data-rule-title="${camera.channel}:${eventType}" value="${escapeHtml(rule.title_template || '')}" placeholder="Leave blank to use the event title">
                </div>
                <div class="field">
                  <label>Message template</label>
                  <textarea data-rule-message="${camera.channel}:${eventType}" placeholder="Leave blank to use the event message">${escapeHtml(rule.message_template || '')}</textarea>
                </div>
              </div>
            `).join('');
            return `
              <div class="camera-card ${camera.enabled ? '' : 'disabled'}">
                <div class="camera-header">
                  <div class="camera-title">
                    <h4>${escapeHtml(camera.camera_name || activeChannelName(camera.channel))}</h4>
                    <small>Watchtower channel ${camera.channel}</small>
                  </div>
                  <label class="inline-check">
                    <input type="checkbox" data-camera-enabled="${camera.channel}" ${camera.enabled ? 'checked' : ''}>
                    <span>Enabled</span>
                  </label>
                </div>

                <label class="inline-check">
                  <input type="checkbox" data-camera-inherit="${camera.channel}" ${usesDefaultServices ? 'checked' : ''}>
                  <span>Use default notify services</span>
                </label>

                <div class="field" ${usesDefaultServices ? 'hidden' : ''} data-camera-services-wrap="${camera.channel}">
                  <label>Camera-specific notify services</label>
                  ${serviceCheckboxList(camera.notify_services || [], `data-camera-service="${camera.channel}"`)}
                  <div class="service-note">Leave all camera-specific services unchecked if you want to switch this camera back to the default list.</div>
                </div>

                <div class="event-rule">
                  <h4>Home Assistant Event Sources</h4>
                  <div class="field">
                    <label>Person sensor</label>
                    <input type="text" list="haBinarySensorOptions" data-ha-person="${camera.channel}" value="${escapeHtml(camera.ha_source?.person_entity_id || '')}" placeholder="binary_sensor.front_door_person">
                  </div>
                  <div class="field">
                    <label>Doorbell sensor</label>
                    <input type="text" list="haBinarySensorOptions" data-ha-doorbell="${camera.channel}" value="${escapeHtml(camera.ha_source?.doorbell_entity_id || '')}" placeholder="binary_sensor.front_door_visitor">
                  </div>
                  <div class="field">
                    <label>Animal sensor</label>
                    <input type="text" list="haBinarySensorOptions" data-ha-animal="${camera.channel}" value="${escapeHtml(camera.ha_source?.animal_entity_id || '')}" placeholder="binary_sensor.backyard_animal">
                  </div>
                  <div class="field">
                    <label>Vehicle sensor</label>
                    <input type="text" list="haBinarySensorOptions" data-ha-vehicle="${camera.channel}" value="${escapeHtml(camera.ha_source?.vehicle_entity_id || '')}" placeholder="binary_sensor.driveway_vehicle">
                  </div>
                  <div class="field">
                    <label>Snapshot camera</label>
                    <input type="text" list="haCameraOptions" data-ha-snapshot-camera="${camera.channel}" value="${escapeHtml(camera.ha_source?.snapshot_camera_entity_id || '')}" placeholder="camera.front_door_fluent">
                  </div>
                  <div class="settings-note">Watchtower listens directly to these Home Assistant entities over the websocket API. Leave any field blank if that event type does not apply to this camera.</div>
                </div>

                <div class="event-rule">
                  <div class="rule-row">
                    <label class="inline-check">
                      <input type="checkbox" data-camera-ai-enabled="${camera.channel}" ${camera.ai?.enabled ? 'checked' : ''}>
                      <span><strong>Use AI descriptions on this camera</strong></span>
                    </label>
                  </div>
                  <div class="field">
                    <label>AI event types</label>
                    <div class="service-grid">
                      ${channelAllowedEventTypes(camera.channel).filter(eventType => ['DOORBELL', 'PERSON', 'ANIMAL', 'VEHICLE'].includes(eventType)).map(eventType => `
                        <label class="service-option">
                          <span class="inline-check">
                            <input type="checkbox" data-camera-ai-event="${camera.channel}" value="${eventType}" ${camera.ai?.event_types?.includes(eventType) ? 'checked' : ''}>
                            <span>${escapeHtml(eventTypeLabels[eventType] || eventType)}</span>
                          </span>
                        </label>
                      `).join('')}
                    </div>
                    <div class="settings-note">AI is never used for this camera unless both this toggle and one of these event types are enabled.</div>
                  </div>
                </div>

                <div class="event-rule-grid">${cameraRules || '<div class="settings-note">No event types are enabled for this camera.</div>'}</div>

                ${supportsDoorbell ? `
                  <div class="event-rule">
                    <div class="rule-row">
                      <label class="inline-check">
                        <input type="checkbox" data-doorbell-action-enabled="${camera.channel}" ${camera.doorbell_action?.enabled ? 'checked' : ''}>
                        <span><strong>Doorbell unlock action</strong></span>
                      </label>
                    </div>
                    <div class="field">
                      <label>Button label</label>
                      <input type="text" data-doorbell-action-title="${camera.channel}" value="${escapeHtml(camera.doorbell_action?.title || 'Unlock Front Door')}" placeholder="Unlock Front Door">
                    </div>
                    <div class="field">
                      <label>Service</label>
                      <input type="text" data-doorbell-action-service="${camera.channel}" value="${escapeHtml(camera.doorbell_action?.service || 'lock.unlock')}" placeholder="lock.unlock">
                    </div>
                    <div class="field">
                      <label>Entity ID</label>
                      <input type="text" data-doorbell-action-entity="${camera.channel}" value="${escapeHtml(camera.doorbell_action?.entity_id || '')}" placeholder="lock.front_door">
                    </div>
                    <div class="settings-note">When used from a notification, this action opens a small Watchtower page and runs the configured Home Assistant service through the Supervisor-backed API.</div>
                  </div>
                ` : ''}
              </div>
            `;
          }).join('')}
        </div>
      `;

      const firstService = settings.preferred_test_service || settings.default_notify_services?.[0] || discoveredServices[0] || '';
      const testServiceSelect = document.getElementById('testService');
      if (testServiceSelect && firstService) {
        testServiceSelect.value = firstService;
      }

      saveSettingsButton.disabled = state.settingsSaving;
      sendTestButton.disabled = !state.haStatus?.enabled || !discoveredServices.length;
    }

    function render() {
      renderEventFilters();
      renderChannelFilters();
      const events = visibleEvents();
      elCount.textContent = `${events.length} event${events.length === 1 ? '' : 's'}`;
      elEvents.innerHTML = events.map(e => `
        <li class="event ${state.selected === e.entry_id ? 'active' : ''}" data-id="${escapeHtml(e.entry_id)}">
          <div class="top">
            <strong>${escapeHtml(e.title || e.event_type)}</strong>
            <span class="${badgeClass(e.event_type)}">${escapeHtml(e.event_type)}</span>
          </div>
          <div class="time">${escapeHtml(formatTime(e.timestamp))}${e.camera_name ? ` - ${escapeHtml(e.camera_name)}` : ''}</div>
        </li>`).join('') || '<li class="empty">No recent events.</li>';

      if ((!state.selected || !events.find(e => e.entry_id === state.selected)) && events.length) {
        selectEvent(events[0].entry_id, false);
        return;
      }
      updateLiveLink(state.events.find(e => e.entry_id === state.selected) || null);
    }

    function setStatus(text) {
      elStatus.textContent = text;
    }

    function setSettingsStatus(text) {
      settingsStatus.textContent = text;
    }

    async function loadChannels() {
      const resp = await fetch(apiUrl('api/camera-config'), { cache: 'no-store' });
      const data = await resp.json();
      state.channels = (data.available_channels || []).filter(info => info.participating);
      state.supportedEventTypes = data.supported_event_types || knownEventTypes;
      state.defaultLiveChannel = data.default_live_channel;
      if (state.channel !== 'ALL' && !state.channels.find(info => info.channel === state.channel)) {
        state.channel = 'ALL';
      }
      syncNotificationSettings();
      renderEventFilters();
      renderChannelFilters();
      updateLiveLink();
      if (!settingsShell.hidden && state.settingsLoaded) {
        renderNotificationSettings();
      }
    }

    async function loadRecent() {
      const resp = await fetch(apiUrl('api/events/recent?limit=50'), { cache: 'no-store' });
      const data = await resp.json();
      state.events = sortNewestFirst(data.events || []);
      if (requestedEventId) {
        const requestedEvent = await loadRequestedEvent();
        if (requestedEvent) {
          state.events = sortNewestFirst([requestedEvent, ...state.events.filter(e => e.entry_id !== requestedEvent.entry_id)]);
        }
      }
      state.selected = requestedEventId && state.events.find(e => e.entry_id === requestedEventId)
        ? requestedEventId
        : (state.events.length ? state.events[0].entry_id : null);
      render();
      if (state.selected) selectEvent(state.selected, false);
    }

    async function loadNotificationConfig() {
      const resp = await fetch(apiUrl('api/notifications/config'), { cache: 'no-store' });
      if (!resp.ok) {
        throw new Error(`Failed to load notification config (${resp.status})`);
      }
      const data = await resp.json();
      state.notifications = data.settings || emptyNotificationSettings();
      state.haStatus = data.home_assistant || { enabled: false, discovered_mobile_notify_services: [] };
      state.settingsLoaded = true;
      syncNotificationSettings();
      renderNotificationSettings();
      setSettingsStatus(state.haStatus.enabled ? 'Ready to configure managed notifications.' : 'Home Assistant API access is unavailable. Settings can still be saved.');
    }

    async function loadHomeAssistantEntities() {
      try {
        const resp = await fetch(apiUrl('api/home-assistant/entities'), { cache: 'no-store' });
        if (!resp.ok) {
          state.haEntities = { binary_sensors: [], cameras: [] };
          return;
        }
        const data = await resp.json();
        state.haEntities = {
          binary_sensors: Array.isArray(data.binary_sensors) ? data.binary_sensors : [],
          cameras: Array.isArray(data.cameras) ? data.cameras : [],
        };
      } catch (err) {
        state.haEntities = { binary_sensors: [], cameras: [] };
      }
    }

    async function loadRequestedEvent() {
      if (!requestedEventId) return null;
      const existing = state.events.find(e => e.entry_id === requestedEventId);
      if (existing) return existing;
      try {
        const resp = await fetch(apiUrl(`api/timeline/${encodeURIComponent(requestedEventId)}`), { cache: 'no-store' });
        if (!resp.ok) return null;
        return await resp.json();
      } catch (err) {
        return null;
      }
    }

    function selectEvent(id, userInitiated = true) {
      const entry = state.events.find(e => e.entry_id === id);
      if (!entry) return;
      state.selected = id;
      render();
      updateLiveLink(entry);
      const clipUrl = entry.clip_url;
      const snapshotUrl = cacheBust(entry.thumbnail_url || entry.metadata?.snapshot_url, entry.entry_id);
      if (clipUrl) {
        snapshot.hidden = true;
        player.hidden = false;
        player.pause();
        player.removeAttribute('src');
        player.load();
        player.poster = snapshotUrl || '';
        player.src = clipUrl;
        player.load();
        if (userInitiated) player.play().catch(() => {});
      } else if (snapshotUrl) {
        player.pause();
        player.removeAttribute('src');
        player.load();
        player.removeAttribute('poster');
        snapshot.src = snapshotUrl;
        snapshot.hidden = false;
        player.hidden = true;
      }
      details.innerHTML = `
        <div class="detail-meta">
          <span class="detail-pill ${entry.event_type === 'DOORBELL' ? 'doorbell' : ''}">${escapeHtml(entry.event_type)}</span>
          <span class="detail-pill">${escapeHtml(formatTime(entry.timestamp))}</span>
          <span class="detail-pill">${escapeHtml(entry.camera_name ? entry.camera_name : activeChannelName(entry.channel))}</span>
          ${entry.clip_status && entry.clip_status !== 'ready' ? `<span class="detail-pill">${escapeHtml(`Clip ${entry.clip_status}`)}</span>` : ''}
        </div>
        ${entry.message && entry.message !== entry.title ? `<div class="detail-note">${escapeHtml(entry.message)}</div>` : ''}
      `;
    }

    function collectNotificationSettings() {
      const next = {
        enabled: !!document.getElementById('notificationsEnabled')?.checked,
        default_notify_services: Array.from(document.querySelectorAll('[data-default-service]'))
          .filter(input => input.checked)
          .map(input => input.value),
        preferred_test_service: (document.getElementById('testService')?.value || '').trim(),
        ai: {
          enabled: !!document.getElementById('aiEnabled')?.checked,
          provider: 'openai',
          api_key: (document.getElementById('aiApiKey')?.value || '').trim(),
          model: (document.getElementById('aiModel')?.value || 'gpt-4.1-mini').trim() || 'gpt-4.1-mini',
          detail: (document.getElementById('aiDetail')?.value || 'low') === 'high' ? 'high' : 'low',
          timeout_seconds: Math.max(Number.parseInt(document.getElementById('aiTimeout')?.value || '8', 10) || 8, 3),
          confidence_threshold: Math.min(Math.max(Number.parseFloat(document.getElementById('aiConfidence')?.value || '0.78') || 0.78, 0), 1),
          daily_event_cap: Math.max(Number.parseInt(document.getElementById('aiDailyCap')?.value || '100', 10) || 0, 0),
          include_fun_summary: !!document.getElementById('aiIncludeFunSummary')?.checked,
          fun_style: ['off', 'mild', 'playful'].includes(document.getElementById('aiFunStyle')?.value || '')
            ? document.getElementById('aiFunStyle').value
            : 'playful',
        },
        known_subjects: [],
        cameras: [],
      };

      const knownSubjectCount = state.notifications?.known_subjects?.length || 0;
      for (let index = 0; index < knownSubjectCount; index += 1) {
        const allChannels = !!document.querySelector(`[data-known-subject-all-channels="${index}"]`)?.checked;
        const allEvents = !!document.querySelector(`[data-known-subject-all-events="${index}"]`)?.checked;
        next.known_subjects.push({
          enabled: !!document.querySelector(`[data-known-subject-enabled="${index}"]`)?.checked,
          name: (document.querySelector(`[data-known-subject-name="${index}"]`)?.value || '').trim(),
          subject_type: (document.querySelector(`[data-known-subject-type="${index}"]`)?.value || 'other').trim() || 'other',
          description: (document.querySelector(`[data-known-subject-description="${index}"]`)?.value || '').trim(),
          channels: allChannels
            ? []
            : Array.from(document.querySelectorAll(`[data-known-subject-channel="${index}"]`))
                .filter(input => input.checked)
                .map(input => Number.parseInt(input.value, 10))
                .filter(value => Number.isFinite(value)),
          event_types: allEvents
            ? []
            : Array.from(document.querySelectorAll(`[data-known-subject-event="${index}"]`))
                .filter(input => input.checked)
                .map(input => input.value)
                .filter(eventType => state.supportedEventTypes.includes(eventType)),
        });
      }

      for (const channelInfo of state.channels) {
        const channel = channelInfo.channel;
        const current = notificationCamera(channel) || {};
        const inheritDefaults = !!document.querySelector(`[data-camera-inherit="${channel}"]`)?.checked;
        const notifyServices = inheritDefaults
          ? []
          : Array.from(document.querySelectorAll(`[data-camera-service="${channel}"]`))
              .filter(input => input.checked)
              .map(input => input.value);
        const rules = {};
        for (const eventType of channelAllowedEventTypes(channel)) {
          rules[eventType] = {
            enabled: !!document.querySelector(`[data-rule-enabled="${channel}:${eventType}"]`)?.checked,
            cooldown_seconds: Math.max(Number.parseInt(document.querySelector(`[data-rule-cooldown="${channel}:${eventType}"]`)?.value || '0', 10) || 0, 0),
            title_template: (document.querySelector(`[data-rule-title="${channel}:${eventType}"]`)?.value || '').trim(),
            message_template: (document.querySelector(`[data-rule-message="${channel}:${eventType}"]`)?.value || '').trim(),
          };
        }
        next.cameras.push({
          channel,
          camera_name: current.camera_name || channelInfo.name || `Channel ${channel}`,
          enabled: !!document.querySelector(`[data-camera-enabled="${channel}"]`)?.checked,
          notify_services: notifyServices,
          doorbell_action: {
            enabled: !!document.querySelector(`[data-doorbell-action-enabled="${channel}"]`)?.checked,
            title: (document.querySelector(`[data-doorbell-action-title="${channel}"]`)?.value || 'Unlock Front Door').trim() || 'Unlock Front Door',
            service: (document.querySelector(`[data-doorbell-action-service="${channel}"]`)?.value || 'lock.unlock').trim() || 'lock.unlock',
            entity_id: (document.querySelector(`[data-doorbell-action-entity="${channel}"]`)?.value || '').trim(),
          },
          ha_source: {
            person_entity_id: (document.querySelector(`[data-ha-person="${channel}"]`)?.value || '').trim(),
            doorbell_entity_id: (document.querySelector(`[data-ha-doorbell="${channel}"]`)?.value || '').trim(),
            animal_entity_id: (document.querySelector(`[data-ha-animal="${channel}"]`)?.value || '').trim(),
            vehicle_entity_id: (document.querySelector(`[data-ha-vehicle="${channel}"]`)?.value || '').trim(),
            snapshot_camera_entity_id: (document.querySelector(`[data-ha-snapshot-camera="${channel}"]`)?.value || '').trim(),
          },
          ai: {
            enabled: !!document.querySelector(`[data-camera-ai-enabled="${channel}"]`)?.checked,
            event_types: Array.from(document.querySelectorAll(`[data-camera-ai-event="${channel}"]`))
              .filter(input => input.checked)
              .map(input => input.value)
              .filter(eventType => channelAllowedEventTypes(channel).includes(eventType)),
          },
          rules,
        });
      }

      state.notifications = next;
      syncNotificationSettings();
      return state.notifications;
    }

    async function saveNotificationSettings() {
      try {
        state.settingsSaving = true;
        saveSettingsButton.disabled = true;
        setSettingsStatus('Saving settings...');
        const payload = collectNotificationSettings();
        const resp = await fetch(apiUrl('api/notifications/config'), {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok) {
          throw new Error(data.detail || `Failed to save settings (${resp.status})`);
        }
        state.notifications = data.settings || payload;
        state.haStatus = data.home_assistant || state.haStatus;
        syncNotificationSettings();
        renderNotificationSettings();
        setSettingsStatus('Settings saved.');
      } catch (err) {
        setSettingsStatus(`Save failed: ${err.message || err}`);
      } finally {
        state.settingsSaving = false;
        saveSettingsButton.disabled = false;
      }
    }

    async function sendTestNotification() {
      const service = document.getElementById('testService')?.value || '';
      if (!service) {
        setSettingsStatus('Choose a notify service first.');
        return;
      }
      try {
        sendTestButton.disabled = true;
        setSettingsStatus(`Sending test notification via ${service}...`);
        const resp = await fetch(apiUrl('api/notifications/test'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ service }),
        });
        const data = await resp.json();
        if (!resp.ok) {
          throw new Error(data.detail || `Test notification failed (${resp.status})`);
        }
        setSettingsStatus(`Test notification sent via ${service}.`);
      } catch (err) {
        setSettingsStatus(`Test failed: ${err.message || err}`);
      } finally {
        sendTestButton.disabled = !state.haStatus?.enabled || !availableNotifyServices().length;
      }
    }

    function addKnownSubject() {
      if (!state.notifications) return;
      state.notifications = collectNotificationSettings();
      state.notifications.known_subjects = [...(state.notifications.known_subjects || []), emptyKnownSubject()];
      syncNotificationSettings();
      renderNotificationSettings();
      setSettingsStatus('Added a known subject. Describe how this subject looks so Watchtower can use that context.');
    }

    function removeKnownSubject(index) {
      if (!state.notifications) return;
      state.notifications = collectNotificationSettings();
      state.notifications.known_subjects = (state.notifications.known_subjects || []).filter((_, itemIndex) => itemIndex !== index);
      syncNotificationSettings();
      renderNotificationSettings();
      setSettingsStatus('Removed known subject.');
    }

    function openSettings() {
      settingsShell.hidden = false;
      document.body.style.overflow = 'hidden';
      renderNotificationSettings();
    }

    function closeSettings() {
      settingsShell.hidden = true;
      document.body.style.overflow = '';
    }

    function connectSocket() {
      const socket = new WebSocket(wsUrl('ws/events'));
      state.socket = socket;
      socket.onopen = () => setStatus('Live');
      socket.onclose = () => {
        setStatus('Reconnecting...');
        setTimeout(connectSocket, 1500);
      };
      socket.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.type === 'hello' && Array.isArray(msg.events)) {
          state.events = sortNewestFirst(msg.events);
          render();
          if (state.selected) selectEvent(state.selected, false);
          return;
        }
        if (msg.type === 'event' && msg.event) {
          state.events = sortNewestFirst([msg.event, ...state.events.filter(e => e.entry_id !== msg.event.entry_id)]);
          state.selected = msg.event.entry_id;
          render();
          selectEvent(msg.event.entry_id, false);
        }
      };
    }

    elEvents.addEventListener('click', (ev) => {
      const li = ev.target.closest('.event');
      if (!li) return;
      selectEvent(li.dataset.id, true);
    });

    elEventFilters.addEventListener('click', (ev) => {
      const btn = ev.target.closest('[data-filter]');
      if (!btn) return;
      state.filter = btn.dataset.filter;
      render();
    });

    elCameraFilters.addEventListener('click', (ev) => {
      const btn = ev.target.closest('[data-channel-filter]');
      if (!btn) return;
      const value = btn.dataset.channelFilter;
      state.channel = value === 'ALL' ? 'ALL' : Number.parseInt(value, 10);
      render();
    });

    settingsBody?.addEventListener('click', (ev) => {
      const addButton = ev.target.closest('[data-add-known-subject]');
      if (addButton) {
        addKnownSubject();
        return;
      }
      const removeButton = ev.target.closest('[data-remove-known-subject]');
      if (removeButton) {
        removeKnownSubject(Number.parseInt(removeButton.dataset.removeKnownSubject, 10));
      }
    });

    settingsBody.addEventListener('change', (ev) => {
      const target = ev.target;
      if (target.matches('[data-camera-inherit]')) {
        const channel = target.getAttribute('data-camera-inherit');
        const wrap = settingsBody.querySelector(`[data-camera-services-wrap="${channel}"]`);
        if (wrap) {
          wrap.hidden = target.checked;
        }
      }
      if (target.matches('[data-camera-enabled]')) {
        const card = target.closest('.camera-card');
        if (card) {
          card.classList.toggle('disabled', !target.checked);
        }
      }
    });

    document.getElementById('refresh').addEventListener('click', loadRecent);
    document.getElementById('openSettings').addEventListener('click', openSettings);
    document.getElementById('closeSettings').addEventListener('click', closeSettings);
    saveSettingsButton.addEventListener('click', saveNotificationSettings);
    sendTestButton.addEventListener('click', sendTestNotification);
    settingsShell.addEventListener('click', (ev) => {
      if (ev.target === settingsShell) {
        closeSettings();
      }
    });
    document.addEventListener('keydown', (ev) => {
      if (ev.key === 'Escape' && !settingsShell.hidden) {
        closeSettings();
      }
    });

    Promise.all([loadChannels(), loadRecent(), loadNotificationConfig(), loadHomeAssistantEntities()]).then(() => {
      render();
      connectSocket();
    }).catch(err => {
      setStatus('Offline');
      details.innerHTML = `<div class="empty">Failed to load recent events: ${err}</div>`;
      setSettingsStatus(`Failed to load settings: ${err.message || err}`);
    });
  </script>
</body>
</html>"""


def _doorbell_action_html(channel: int, event_id: Optional[str] = None) -> str:
    channel_label = html.escape(_channel_name(channel) or f"Channel {channel}")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{LIVE_PAGE_TITLE} Doorbell Action</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0f1115;
      --panel: #171b22;
      --line: #263041;
      --text: #e6edf3;
      --muted: #9aa7b7;
      --accent: #5aa9ff;
      --success: #7ed6a5;
      --danger: #ff8a80;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 20px;
      background: var(--bg);
      color: var(--text);
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    .card {{
      width: min(440px, 100%);
      border: 1px solid var(--line);
      border-radius: 16px;
      background: var(--panel);
      padding: 20px;
      display: grid;
      gap: 14px;
    }}
    .status {{
      font-size: 1.1rem;
      font-weight: 600;
    }}
    .meta {{
      color: var(--muted);
      line-height: 1.45;
    }}
    .ok {{ color: var(--success); }}
    .error {{ color: var(--danger); }}
    .actions {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    a, button {{
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #12161d;
      color: var(--text);
      min-height: 42px;
      padding: 10px 12px;
      text-decoration: none;
      font: inherit;
      cursor: pointer;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="status" id="status">Running unlock action...</div>
    <div class="meta" id="message">Watchtower is executing the configured doorbell action for {channel_label}.</div>
    <div class="actions">
      <a href="/app">Back to Watchtower</a>
      <button id="retry" type="button">Retry</button>
    </div>
  </div>
  <script>
    const payload = {{ channel: {channel}, event_id: {json.dumps(event_id)} }};
    async function runAction() {{
      const status = document.getElementById('status');
      const message = document.getElementById('message');
      status.textContent = 'Running unlock action...';
      status.className = 'status';
      message.textContent = 'Watchtower is executing the configured doorbell action for {channel_label}.';
      try {{
        const resp = await fetch('/api/doorbell-action/unlock', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify(payload),
        }});
        const data = await resp.json();
        if (!resp.ok) {{
          throw new Error(data.detail || `Request failed (${{resp.status}})`);
        }}
        status.textContent = data.title ? `${{data.title}} sent` : 'Unlock action sent';
        status.className = 'status ok';
        message.textContent = `Watchtower called ${{data.service}} for ${{data.entity_id}} on {channel_label}.`;
      }} catch (err) {{
        status.textContent = 'Unlock action failed';
        status.className = 'status error';
        message.textContent = err.message || String(err);
      }}
    }}
    document.getElementById('retry').addEventListener('click', runAction);
    runAction();
  </script>
</body>
</html>"""


@app.get("/app", response_class=HTMLResponse, summary="Open the event dashboard")
@app.get("/app/", response_class=HTMLResponse, include_in_schema=False)
async def app_dashboard(
    request: Request,
    view: Optional[str] = Query(None, description="Optional app view: 'live'"),
    channel: Optional[int] = Query(None, description="Camera channel number"),
    event_type: Optional[str] = Query(None),
):
    if view and view.strip().lower() == "live":
        channel = _resolve_live_channel(channel)
        if channel is None:
            raise HTTPException(status_code=400, detail="Invalid channel")
        return HTMLResponse(_live_dashboard_html(channel=channel, event_type=event_type))
    return HTMLResponse(_dashboard_html_v2())


@app.get("/doorbell-action", response_class=HTMLResponse, summary="Execute a configured doorbell action")
@app.get("/app/doorbell-action", response_class=HTMLResponse, include_in_schema=False)
async def app_doorbell_action(
    channel: int = Query(..., description="Camera channel number"),
    event_id: Optional[str] = Query(None),
):
    if not _channel_is_participating(channel):
        raise HTTPException(status_code=404, detail=f"Channel {channel} is not enabled in Watchtower.")
    if not _camera_doorbell_action(channel):
        raise HTTPException(status_code=404, detail=f"No doorbell action is configured for channel {channel}.")
    return HTMLResponse(_doorbell_action_html(channel=channel, event_id=event_id))


@app.websocket("/ws/events")
@app.websocket("/app/ws/events")
async def ws_events(websocket: WebSocket):
    await websocket.accept()
    ui_clients.append(websocket)
    try:
        await websocket.send_json({
            "type": "hello",
            "events": [
                _timeline_entry_to_recent(entry)
                for entry in timeline_index.get_entries(limit=100)
                if _channel_is_participating(entry.channel)
            ][:20],
        })
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in ui_clients:
            ui_clients.remove(websocket)


@app.get("/api/debug/info", summary="Debug info (requires debug=true in config)")
async def debug_info():
    if not DEBUG:
        raise HTTPException(status_code=403, detail="Debug endpoint disabled. Set debug: true in add-on config.")
    return {
        "nvr_host":      NVR_HOST,
        "nvr_port":      NVR_PORT,
        "nvr_ssl":       NVR_SSL,
        "nvr_connected": nvr_host is not None,
        "nvr_info": {
            "model":        nvr_host.model         if nvr_host else None,
            "sw_version":   nvr_host.sw_version    if nvr_host else None,
            "nvr_name":     nvr_host.nvr_name       if nvr_host else None,
            "num_channels": nvr_host.num_channels    if nvr_host else None,
            "is_nvr":       nvr_host.is_nvr         if nvr_host else None,
            "mac_address":  nvr_host.mac_address    if nvr_host else None,
        },
        "camera_config": {
            "participating_channels": _sorted_channels(participating_channels),
            "buffered_channels": _sorted_channels(buffered_channels),
            "default_live_channel": default_live_channel,
            "camera_event_types": {
                str(channel): _sorted_event_types(_channel_allowed_event_types(channel))
                for channel in _sorted_channels(participating_channels)
            },
        },
        "home_assistant": {
            "api_enabled": ha_client.enabled,
            "websocket_listener_running": bool(ha_ws_listener_task and not ha_ws_listener_task.done()),
            "managed_notifications_enabled": watchtower_settings.notifications.enabled,
            "default_notify_services": watchtower_settings.notifications.default_notify_services,
            "ai_enrichment_enabled": watchtower_settings.notifications.ai.enabled,
            "known_subject_count": len(watchtower_settings.notifications.known_subjects),
        },
        "rolling_buffers": {str(channel): buffer.get_stats() for channel, buffer in rolling_buffers.items()},
        "supported_event_types": _supported_event_types(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=API_HOST, port=API_PORT)
