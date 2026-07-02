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

## 4. Import The Notification Blueprint

Import this blueprint into Home Assistant:

```text
https://raw.githubusercontent.com/marcusmaday/reolink-nvr-watchtower/main/blueprints/automation/watchtower_notification.yaml
```

Then create an automation from the blueprint and choose:

1. Optional doorbell sensors
2. Your person sensor
3. Optional animal sensors
4. Optional vehicle sensors
5. Your snapshot camera
6. Optional unlock action for the doorbell camera
7. Your two mobile app notify services, such as `notify.mobile_app_pixel_8_pro`
8. Your app navigation path or URL

Recommended value:

```text
/app/15e0e6e5_watchtower
```

The blueprint converts that to a `homeassistant://navigate/...` mobile deep link so it works from the Home Assistant companion app even when you are away from your local Wi-Fi.

The blueprint handles:

- snapshot thumbnails
- relay into the Watchtower timeline
- `PERSON`, `DOORBELL`, optional `ANIMAL`, and optional `VEHICLE` notifications for that camera
- tap-to-open event clips
- tap-to-open live view
- optional unlock actions for doorbell notifications
- direct mobile app notifications with images, deep links, and notification actions

## 5. Add The Relay Command To Home Assistant

Home Assistant needs one `rest_command` block in `configuration.yaml` so it can relay events into the app timeline.

Use the internal Home Assistant gateway address and the add-on port:

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
        "snapshot_url": "{{ snapshot_url }}",
        "live_url": "{{ live_url }}",
        "title": "{{ title }}",
        "message": "{{ message }}",
        "source": "home_assistant"
      }
```

Find `HA_GATEWAY_IP` from the add-on shell:

```bash
ip route | awk '/default/ {print $3}'
```

`APP_PORT` is the add-on port you configured. It defaults to `5000`.

Watchtower stores NVR channels internally as zero-based indexes. Reolink commonly labels those same channels as `1-12` in the UI while Watchtower addresses them as `0-11`, so the value used in notification automations should usually be `labeled channel - 1`.

Keep sending `camera_name` in the payload as well so Watchtower can reconcile the event to the correct camera automatically if a channel mapping is ever off.

After adding the block, restart Home Assistant Core or reload the automation if you already had the command in place.

## 6. Open The App On Mobile

For the most reliable mobile experience:

- open the app from the Home Assistant dashboard button
- or tap the notification action

The app is designed for the Home Assistant companion app and remote access through Home Assistant, not for being framed inside an iframe.

## Common Issues

- If the app opens but shows no events, check that `rest_command.reolink_ingest_event` is present and that Home Assistant was restarted.
- If clips are too late, make sure the add-on is updated and the buffer settings are not still at their smallest values.
- If notifications open a browser login page, use the `homeassistant://navigate/...` link style from the blueprint.
