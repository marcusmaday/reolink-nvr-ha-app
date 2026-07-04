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

## 3. Configure Home Assistant Event Sources In Watchtower

Open Watchtower and use `Notification Settings` inside the app to configure:

- the Home Assistant entities for each camera:
  - person sensor
  - doorbell sensor
  - animal sensor
  - vehicle sensor
  - optional snapshot camera
- the phones that should receive notifications
- per-camera event filters and cooldowns
- doorbell unlock actions

Note:

- if you keep the old blueprint/rest-command relay active at the same time, you can create duplicate events or notifications

## 4. Check It

Open the app from the Home Assistant dashboard button and trigger a test doorbell or person event.

What you should see:

- an event clip in the app timeline
- a Watchtower-managed notification
- a snapshot thumbnail if you configured a snapshot camera
- unlock for doorbell notifications if you enabled it in Watchtower

## 5. If Something Is Off

- No events in the app: confirm the Home Assistant entity mapping in Watchtower and wait for the listener status to show connected.
- Clips start too late: make sure you are on the latest add-on version and use the current buffer defaults.
- No notification arrives: check the Watchtower notification settings instead of the Home Assistant blueprint.
- If you prefer the older relay path, use the optional blueprint and `rest_command` flow from [INSTALL.md](INSTALL.md).
