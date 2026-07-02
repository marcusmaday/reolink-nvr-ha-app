# Watchtower

<img src="docs/images/watchtower-logo.png" alt="Watchtower logo" width="180">

Watchtower is a Home Assistant add-on and companion app for a Reolink NVR. It gives you:

- a mobile-friendly event dashboard for recent camera activity
- clip playback for event clips with pre-roll buffering
- live view from the selected camera
- camera-aware filtering across any participating NVR channels
- per-camera event-type selection so each camera can focus on only the detections you care about
- Home Assistant notifications with snapshot thumbnails and app links
- a searchable recording timeline for the NVR
- an in-progress path toward Watchtower-managed notifications from inside the app

## App Preview

![Watchtower app preview](docs/images/reolink-app-example.png)

## Start Here

If you want the fastest path to a working setup, read:

- [Quick Start](QUICKSTART.md)
- [Installation](INSTALL.md)

For the API surface, see [API Reference](API.md).
For developer-oriented notes, see [Developer Instructions](DEVELOPMENT.md).

## What You Need

- A Home Assistant instance
- A Reolink NVR already added to your network
- The NVR IP address, username, and password
- The Home Assistant mobile app if you want phone notifications and tap-to-open actions

## What It Does

The app connects to your NVR and builds a live event experience around:

- `PERSON`
- `DOORBELL`
- `MOTION`
- `ANIMAL`
- `VEHICLE`

For supported cameras, it can:

- show a snapshot thumbnail in the notification
- open the event clip in the app
- open live view in the app
- unlock the front door from a doorbell notification

## Home Assistant Setup

The recommended setup is:

1. Add the repository to the Home Assistant add-on store.
2. Install the add-on.
3. Set your NVR connection in the add-on options.
4. Import the provided notification blueprint.
5. Add the one `rest_command` block from the install docs so Home Assistant can relay events into the app timeline.
6. Open the app from the Home Assistant dashboard or from a notification tap.

## Dashboard Entry Point

The repository includes a simple Home Assistant dashboard button that opens the app directly. Use that instead of an iframe if you want a reliable mobile entry point.

## Questions

If you are setting this up, the docs you probably want are:

- [QUICKSTART.md](QUICKSTART.md) for the shortest path
- [INSTALL.md](INSTALL.md) for the full setup flow
- [API.md](API.md) for endpoint details
- [DEVELOPMENT.md](DEVELOPMENT.md) for contributor notes
