# Watchtower

<img src="docs/images/watchtower-logo.png" alt="Watchtower logo" width="180">

Watchtower is a Home Assistant add-on and companion app for a Reolink NVR. It gives you:

- a mobile-friendly event dashboard for recent camera activity
- clip playback for event clips with pre-roll buffering
- live view from the selected camera
- camera-aware filtering across any participating NVR channels
- per-camera event-type selection so each camera can focus on only the detections you care about
- Watchtower-managed Home Assistant notifications with app links, per-camera routing, and optional doorbell unlock actions
- optional AI-enriched notification descriptions with user-defined known subjects like pets or recurring visitors
- a searchable recording timeline for the NVR
- direct Home Assistant websocket event listening with per-camera entity mapping
- an optional lightweight Home Assistant event-forwarding blueprint fallback

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

- open the event clip in the app
- open live view in the app
- unlock the front door from a doorbell notification

## Home Assistant Setup

The recommended setup is:

1. Add the repository to the Home Assistant add-on store.
2. Install the add-on.
3. Set your NVR connection in the add-on options.
4. Configure Home Assistant event sources and notifications inside Watchtower.
   Optionally add AI enrichment and known-subject context there too.
5. Open the app from the Home Assistant dashboard or from a notification tap.

The blueprint and `rest_command` relay are still available as a fallback, but
the preferred path is now direct Home Assistant websocket listening from inside
Watchtower.

## Dashboard Entry Point

The repository includes a simple Home Assistant dashboard button that opens the app directly. Use that instead of an iframe if you want a reliable mobile entry point.

## Questions

If you are setting this up, the docs you probably want are:

- [QUICKSTART.md](QUICKSTART.md) for the shortest path
- [INSTALL.md](INSTALL.md) for the full setup flow
- [API.md](API.md) for endpoint details
- [DEVELOPMENT.md](DEVELOPMENT.md) for contributor notes
