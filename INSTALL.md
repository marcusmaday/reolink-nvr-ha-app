# Installation

This page covers the full Home Assistant installation and notification setup.

## 1. Add The Repository In Home Assistant

In Home Assistant:

1. Go to `Settings` -> `Add-ons` -> `Add-on Store`
2. Open the three-dot menu and choose `Repositories`
3. Add this repository:

```text
https://github.com/marcusmaday/reolink-nvr-watchtower
```

4. Install **Watchtower**

## 2. Configure The Add-On

Open the add-on configuration and set your NVR details.

Example starting values:

```yaml
api_port: 5000
api_host: 0.0.0.0
nvr_host: 192.168.50.42
nvr_port: 80
nvr_username: admin
nvr_password: your_password
nvr_ssl: false
buffer_enabled: true
buffer_size_seconds: 60
clip_duration_before: 1
clip_duration_after: 15
clip_quality: medium
watch_channels: "all"
buffer_channels: ""
default_live_channel: -1
camera_event_types: ""
retention_days: 7
max_storage_mb: 5000
external_storage_path: ""
allow_cors: false
debug: false
```

Notes:

- `api_port` is the port the app listens on inside the add-on. Leave it at `5000` unless you know you need to change it.
- `clip_quality` controls which RTSP stream the app uses for generated clips.
- `buffer_size_seconds` controls how much rolling clip data is kept available for pre-roll.
- `watch_channels` controls which NVR channels participate in Watchtower. Use `all` or a comma-separated list like `0,1,8`.
- `buffer_channels` lets you keep pre-roll buffers on only a subset of participating cameras. Leave it blank to reuse `watch_channels`.
- `default_live_channel` chooses the fallback camera for live view. Set `-1` to auto-select the first participating camera.
- `camera_event_types` lets you restrict which event types are accepted per camera. Leave it blank for all supported event types on every participating camera, or use a mapping like `all:PERSON,DOORBELL;0:PERSON,VEHICLE;1:PERSON,ANIMAL;8:PERSON,DOORBELL`.

Restart the add-on after saving the configuration.

## 3. Confirm The App Works

After the add-on starts, verify:

- the app loads in the Home Assistant add-on page
- `/api/health` reports `ok`
- `/api/device/info` shows your NVR
- the app dashboard opens from Home Assistant

## 4. Configure Home Assistant Event Sources In Watchtower

Open Watchtower and configure the Home Assistant side directly inside the app:

- map each Watchtower camera to its Home Assistant entities:
  - person sensor
  - doorbell sensor
  - animal sensor
  - vehicle sensor
  - optional snapshot camera
- choose your Home Assistant mobile app notify services
- set per-camera event rules and cooldowns
- optionally enable doorbell unlock actions

Watchtower listens to those entities over the Home Assistant websocket API and
uses the optional snapshot camera to capture notification thumbnails with
`camera.snapshot`.

If you previously used the blueprint + `rest_command` relay, disable those
automations once the direct listener is working to avoid duplicate events or
notifications.

## 5. Optional Fallback: Blueprint Relay

If you prefer the older Home Assistant automation path, Watchtower still ships
with a fallback blueprint:

```text
https://raw.githubusercontent.com/marcusmaday/reolink-nvr-watchtower/main/blueprints/automation/watchtower_notification.yaml
```

That path also requires the `rest_command.reolink_ingest_event` block shown
below:

```yaml
rest_command:
  reolink_ingest_event:
    url: "http://HA_GATEWAY_IP:APP_PORT/api/events/ingest"
    method: POST
    content_type: "application/json"
    payload: >-
      {
        "event_type": "{{ event_type }}",
        "event_id": "{{ event_id }}",
        "channel": {{ channel }},
        "timestamp": "{{ timestamp }}",
        "camera_name": "{{ camera_name }}",
        "title": "{{ title }}",
        "message": "{{ message }}",
        "source": "home_assistant"
      }
```

Find `HA_GATEWAY_IP` from the add-on shell:

```bash
ip route | awk '/default/ {print $3}'
```

`APP_PORT` defaults to `5000`.

## 6. Open The App On Mobile

For the most reliable mobile experience:

- open the app from the Home Assistant dashboard button
- or tap the notification action

The app is designed for the Home Assistant companion app and remote access through Home Assistant, not for being framed inside an iframe.

## Common Issues

- If the app opens but shows no events, check the Home Assistant entity mapping inside Watchtower first.
- If clips are too late, make sure the add-on is updated and the buffer settings are not still at their smallest values.
- If notifications are missing or misrouted, check the Watchtower in-app notification settings instead of the Home Assistant blueprint.
