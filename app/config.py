"""
config.py

Configuration management for the Reolink Enhanced API.
Loads settings from environment variables and configuration files.
"""

import os
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class NVRConfig:
    """NVR connection configuration."""
    host: str
    port: int
    username: str
    password: str
    use_https: bool


@dataclass
class VideoBufferConfig:
    """Video buffer configuration."""
    enabled: bool
    buffer_size_seconds: int
    clip_duration_before: int
    clip_duration_after: int
    clip_quality: str  # "low", "medium", "high"
    watch_channels: str
    buffer_channels: str
    default_live_channel: Optional[int]
    camera_event_types: str


@dataclass
class StorageConfig:
    """Storage and retention configuration."""
    clips_directory: str
    index_file: str
    settings_file: str
    retention_days: int
    max_storage_mb: int
    external_storage_path: Optional[str]


@dataclass
class APIConfig:
    """API server configuration."""
    host: str
    port: int
    debug: bool
    allow_cors: bool


@dataclass
class AppConfig:
    """Complete application configuration."""
    nvr: NVRConfig
    video_buffer: VideoBufferConfig
    storage: StorageConfig
    api: APIConfig


def get_config() -> AppConfig:
    """
    Load configuration from environment variables and defaults.
    """

    # ─── NVR Configuration ───
    nvr_host = os.getenv("NVR_HOST", "192.168.1.100")
    nvr_port = int(os.getenv("NVR_PORT", "80"))
    nvr_username = os.getenv("NVR_USERNAME", "admin")
    nvr_password = os.getenv("NVR_PASSWORD", "password")
    nvr_ssl = os.getenv("NVR_SSL", "false").lower() == "true"

    nvr_config = NVRConfig(
        host=nvr_host,
        port=nvr_port,
        username=nvr_username,
        password=nvr_password,
        use_https=nvr_ssl,
    )

    # ─── Video Buffer Configuration ───
    buffer_enabled = os.getenv("BUFFER_ENABLED", "true").lower() == "true"
    buffer_size_seconds = int(os.getenv("BUFFER_SIZE_SECONDS", "60"))
    clip_duration_before = int(os.getenv("CLIP_DURATION_BEFORE", "1"))
    clip_duration_after = int(os.getenv("CLIP_DURATION_AFTER", "15"))
    clip_quality = os.getenv("CLIP_QUALITY", "medium")
    watch_channels = os.getenv("WATCH_CHANNELS", "all")
    buffer_channels = os.getenv("BUFFER_CHANNELS", "")
    default_live_channel_raw = os.getenv("DEFAULT_LIVE_CHANNEL", "").strip()
    default_live_channel = int(default_live_channel_raw) if default_live_channel_raw else None
    if default_live_channel is not None and default_live_channel < 0:
        default_live_channel = None
    camera_event_types = os.getenv("CAMERA_EVENT_TYPES", "")

    video_buffer_config = VideoBufferConfig(
        enabled=buffer_enabled,
        buffer_size_seconds=buffer_size_seconds,
        clip_duration_before=clip_duration_before,
        clip_duration_after=clip_duration_after,
        clip_quality=clip_quality,
        watch_channels=watch_channels,
        buffer_channels=buffer_channels,
        default_live_channel=default_live_channel,
        camera_event_types=camera_event_types,
    )

    # ─── Storage Configuration ───
    # If HOME_ASSISTANT_DATA_DIR is set (when running in HA add-on),
    # use that; otherwise use /tmp or local directory.
    # External storage, when configured, becomes the base directory.
    ha_data_dir = os.getenv("HOME_ASSISTANT_DATA_DIR", "/tmp")
    external_storage = os.getenv("EXTERNAL_STORAGE_PATH", None) or None
    base_dir = external_storage or ha_data_dir
    clips_dir = os.path.join(base_dir, "reolink_clips")
    index_file = os.path.join(base_dir, "timeline.json")
    settings_file = os.path.join(base_dir, "watchtower_settings.json")

    retention_days = int(os.getenv("RETENTION_DAYS", "7"))
    max_storage_mb = int(os.getenv("MAX_STORAGE_MB", "5000"))

    storage_config = StorageConfig(
        clips_directory=clips_dir,
        index_file=index_file,
        settings_file=settings_file,
        retention_days=retention_days,
        max_storage_mb=max_storage_mb,
        external_storage_path=external_storage,
    )

    # ─── API Configuration ───
    api_host = os.getenv("API_HOST", "0.0.0.0")
    api_port = int(os.getenv("API_PORT", "5000"))
    debug = os.getenv("DEBUG", "false").lower() == "true"
    allow_cors = os.getenv("ALLOW_CORS", "false").lower() == "true"

    api_config = APIConfig(
        host=api_host,
        port=api_port,
        debug=debug,
        allow_cors=allow_cors,
    )

    # ─── Create directories ───
    Path(clips_dir).mkdir(parents=True, exist_ok=True)
    Path(index_file).parent.mkdir(parents=True, exist_ok=True)
    Path(settings_file).parent.mkdir(parents=True, exist_ok=True)

    config = AppConfig(
        nvr=nvr_config,
        video_buffer=video_buffer_config,
        storage=storage_config,
        api=api_config,
    )

    _log_config(config)
    return config


def _log_config(config: AppConfig):
    """Log current configuration (hiding sensitive info)."""
    logger.info("=== Reolink Enhanced API Configuration ===")
    logger.info("NVR: %s:%d (SSL: %s)", config.nvr.host, config.nvr.port, config.nvr.use_https)
    logger.info("Username: %s", config.nvr.username)
    logger.info(
        "Video Buffer: enabled=%s, size=%ds, clip_quality=%s, watch_channels=%s, buffer_channels=%s, default_live_channel=%s, camera_event_types=%s",
        config.video_buffer.enabled,
        config.video_buffer.buffer_size_seconds,
        config.video_buffer.clip_quality,
        config.video_buffer.watch_channels,
        config.video_buffer.buffer_channels or "watch_channels",
        config.video_buffer.default_live_channel if config.video_buffer.default_live_channel is not None else "auto",
        config.video_buffer.camera_event_types or "all",
    )
    logger.info("Clips Directory: %s", config.storage.clips_directory)
    logger.info(
        "Retention: %d days, max %d MB, external_storage=%s",
        config.storage.retention_days,
        config.storage.max_storage_mb,
        config.storage.external_storage_path or "none",
    )
    logger.info(
        "API: %s:%d (debug=%s, allow_cors=%s)",
        config.api.host,
        config.api.port,
        config.api.debug,
        config.api.allow_cors,
    )
