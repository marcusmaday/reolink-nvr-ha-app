# API Reference

Watchtower exposes a small HTTP API for the event dashboard, clip playback, live view, and timeline lookup.

Base URL examples:

```text
http://localhost:5000
```

When the app is opened through Home Assistant ingress, the same routes are also available under:

```text
/app/15e0e6e5_watchtower
```

## Core Status

### `GET /api/health`

Returns whether the app is connected to the NVR.

### `GET /api/device/info`

Returns the connected NVR model, firmware, MAC address, and channel count.

### `GET /api/channels`

Returns the list of available channels and their names, plus whether each one is participating in Watchtower, buffered for pre-roll clips, and the default live camera.

### `GET /api/camera-config`

Returns the resolved Watchtower camera selection: available channels, participating channels, buffered channels, each camera's allowed event types, the supported event-type list, and the default live camera.

### `GET /api/home-assistant/status`

Returns whether Watchtower can reach Home Assistant Core through the Supervisor proxy, which `notify.mobile_app_*` services it discovers, and whether the direct websocket listener is running.

### `GET /api/home-assistant/entities`

Returns the discovered Home Assistant `binary_sensor` and `camera` entities that can be mapped to Watchtower cameras in the settings UI.

### `GET /api/notifications/config`

Returns the persisted Watchtower-managed notification settings together with the discovered Home Assistant mobile app notify services. This now includes optional AI enrichment settings and any configured known-subject descriptors.

### `PUT /api/notifications/config`

Updates the persisted Watchtower-managed notification settings, including AI enrichment preferences, per-camera AI routing, and known-subject context.

### `POST /api/notifications/test`

Sends a simple test notification through one configured Home Assistant mobile app notify service.

### `GET /api/debug/info`

Returns runtime configuration and rolling-buffer status. This endpoint is only available when debug mode is enabled.

## Recordings And Timeline

### `GET /api/search`

Search recordings on the NVR.

Parameters:

- `channel` required, zero-based channel number
- `start_date` required, format `YYYY-MM-DD`
- `end_date` optional, defaults to `start_date`
- `event_type` optional, one of `DOORBELL`, `PERSON`, `MOTION`, `ANIMAL`, `VEHICLE`
- `stream` optional, `sub` or `main`

Examples:

```bash
curl "http://localhost:5000/api/search?channel=8&start_date=2026-06-11&end_date=2026-06-12&event_type=PERSON&stream=main"
```

```bash
curl "http://localhost:5000/api/search?channel=8&start_date=2026-06-11"
```

### `GET /api/timeline`

Returns events from the local timeline store.

Parameters:

- `hours` optional, how far back to look
- `channel` optional, filter to one channel
- `event_type` optional, filter to one event type
- `limit` optional, maximum number of entries

Example:

```bash
curl "http://localhost:5000/api/timeline?hours=48&channel=8&event_type=PERSON"
```

### `GET /api/timeline/{entry_id}`

Returns one timeline entry by ID.

### `GET /api/events/recent`

Returns the event list used by the app dashboard.

Parameters:

- `limit` optional, maximum number of entries
- `channel` optional, filter to one channel
- `event_type` optional, filter to one event type

## Event Ingest And Playback

### `POST /api/events/ingest`

Creates a timeline entry from Home Assistant or another source.

This is the optional relay endpoint used by the fallback Home Assistant blueprint path.

Notes:

- `channel` uses the Watchtower zero-based channel index
- `camera_name` is strongly recommended for Home Assistant relays so Watchtower can correct channel-numbering mismatches automatically
- events are rejected with `403` if that event type is not enabled for the resolved camera in Watchtower

### `GET /api/events/{entry_id}/clip`

Returns the playable clip for a timeline event.

### `GET /api/events/{entry_id}/live`

Returns the live-view target for a timeline event.

### `POST /api/webhook/reolink`

Accepts a Reolink ONVIF webhook payload.

## Live Views

### `GET /api/live/{channel}/mjpeg`

Returns a browser-friendly MJPEG stream for the requested channel.

### `GET /app`

Opens the event dashboard.

### `GET /app/live`

Opens the live camera page.

### `GET /live`

Opens the live camera page outside the add-on ingress prefix.

### `GET /app/ws/events`

WebSocket feed for live dashboard updates.

## Response Notes

Search responses include:

- `channel`
- `start_date`
- `end_date`
- `event_type`
- `total_clips`
- `clips`

Timeline entries and recent events include the clip URL, live URL, thumbnail URL, clip status, and camera metadata needed by the dashboard.

## Practical Notes

- `PERSON` and `DOORBELL` are the main event types for the mobile notification flow.
- `channel=0` means the first camera channel.
- If you are testing through Home Assistant ingress, use the app route under `/app/...` instead of a raw LAN URL.
- The recommended setup is now direct Home Assistant websocket listening configured inside Watchtower, not the relay blueprint.
