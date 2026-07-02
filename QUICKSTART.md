# Quick Start

Use this if you want the shortest path to a working setup.

## 1. Add The Repository

In Home Assistant:

1. Go to `Settings` -> `Add-ons` -> `Add-on Store`
2. Open the three-dot menu and choose `Repositories`
3. Add:

```text
https://github.com/marcusmaday/reolink-nvr-watchtower
```

## 2. Install The Add-On

Install **Watchtower**, then open its configuration and set:

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

Restart the add-on after saving.

Camera selection notes:

- `watch_channels` controls which NVR channels appear in Watchtower. Use `all` or a comma-separated list like `0,1,8`.
- `buffer_channels` controls which of those cameras keep rolling pre-roll buffers for local clip generation. Leave it blank to match `watch_channels`.
- `default_live_channel` controls which camera opens when the app needs a fallback live view. Use `-1` to pick the first participating camera automatically.
- `camera_event_types` lets you limit each camera to the event types you care about. Example: `all:PERSON;0:PERSON,VEHICLE;1:PERSON,ANIMAL;8:PERSON,DOORBELL`.

## 3. Import The Blueprint

Import this blueprint into Home Assistant:

```text
https://raw.githubusercontent.com/marcusmaday/reolink-nvr-watchtower/main/blueprints/automation/watchtower_notification.yaml
```

Create the automation from the blueprint and choose:

- optional doorbell sensors
- person sensor
- optional animal sensors
- optional vehicle sensors
- snapshot camera
- optional unlock action for the doorbell camera
- two mobile app notify services such as `notify.mobile_app_pixel_8_pro`
- app navigation path or URL

Recommended value:

```text
/app/15e0e6e5_watchtower
```

The blueprint converts that to a `homeassistant://navigate/...` mobile deep link so it works away from your local Wi-Fi too.

## 4. Add The Relay Command

Add this to `configuration.yaml`:

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

Find `HA_GATEWAY_IP` with:

```bash
ip route | awk '/default/ {print $3}'
```

Watchtower stores NVR channels internally as zero-based indexes. Reolink commonly labels channels as `1-12` while Watchtower uses `0-11`, so the channel used in the notification automation should usually be `labeled channel - 1`.

Include `camera_name` in the payload too so Watchtower can reconcile the event to the matching participating camera automatically if the numeric mapping is off.

`APP_PORT` defaults to `5000`.

Restart Home Assistant Core after adding the command.

## 5. Check It

Open the app from the Home Assistant dashboard button and trigger a test doorbell or person event.

What you should see:

- a notification with a snapshot thumbnail
- a tap action that opens the app
- an event clip in the app timeline
- live view for person notifications
- unlock for doorbell notifications

## 6. If Something Is Off

- No events in the app: check the `rest_command` and restart Home Assistant Core.
- Clips start too late: make sure you are on the latest add-on version and use the current buffer defaults.
- Mobile opens a login page: use the notification links from the blueprint, not a raw browser URL.
