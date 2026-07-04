# Changelog

## 0.5.4

- Fix AI snapshot enrichment again by falling back to the authenticated Home Assistant `camera_proxy` endpoint for the configured snapshot camera when static `/local/...` files are not readable from the add-on.
- Keep the detailed AI-enrichment diagnostics so it is easier to tell whether a skip came from per-camera AI settings, snapshot access, or the upstream AI request itself.

## 0.5.3

- Fix AI snapshot enrichment in the add-on by loading snapshot bytes through Home Assistant when `/local/...` images are not directly mounted inside the Watchtower container.
- Keep the detailed AI-enrichment diagnostics so it is clear whether an event was skipped because of camera AI settings, missing API credentials, or an unavailable snapshot.

## 0.5.2

- Add clearer AI-enrichment logging so it is obvious whether enrichment was skipped because of missing API credentials, per-camera AI settings, missing snapshot files, or request failures.
- Wait briefly for newly captured snapshot files before deciding AI enrichment cannot run, which makes the enrichment path more reliable right after event-triggered snapshot capture.

## 0.5.1

- Fix the Home Assistant websocket listener so it correctly handles coalesced message batches instead of crashing on `list` payloads and silently breaking event-driven notifications.

## 0.5.0

- Add optional OpenAI-powered notification enrichment so Watchtower can turn a snapshot into a short, fun notification description before delivery.
- Add in-app AI settings for model choice, detail level, confidence threshold, daily cap, and per-camera AI event selection.
- Add user-defined known-subject context with plain-English descriptions and optional camera or event scoping, so Watchtower can try to recognize recurring pets or visitors like named dogs or a mail carrier.

## 0.4.59

- Remove the remaining persisted `app_target` notification setting and hardcode the Watchtower ingress deep-link target so the saved configuration matches the websocket-first app flow.
- Keep notification event deep links pinned to the exact `event_id` with URL-safe encoding for a more reliable mobile clip-opening experience.

## 0.4.58

- Fix Watchtower-managed notification deep links so `event_id` is URL-encoded correctly and "View Event Clip" can reopen the exact event instead of falling back to the latest clip.
- Remove the no-longer-needed app target field from the settings UI and refresh the docs/API notes around the websocket-first Home Assistant flow.

## 0.4.57

- Add direct Home Assistant websocket event listening with per-camera entity mapping in the Watchtower settings UI, so Watchtower can react to Reolink-related Home Assistant sensors without relying on a blueprint relay.
- Add optional Home Assistant snapshot camera selection per Watchtower camera so websocket-driven notifications can still include thumbnail images.

## 0.4.56

- Replace the Home Assistant notification blueprint with a lean event-forwarder-only version so Watchtower fully owns notification delivery, cooldowns, deep links, and doorbell unlock actions.
- Remove the blueprint's snapshot capture and direct notification steps to simplify Home Assistant setup.

## 0.4.55

- Persist the Watchtower settings panel's preferred test notification service so the selected device no longer snaps back to the first discovered or default notify target after saving or reopening settings.

## 0.4.54

- Add per-camera doorbell unlock actions to Watchtower-managed notifications, including in-app settings for the button label, Home Assistant service, and target entity.
- Add a Watchtower doorbell action page and backend execution endpoint so the notification action can run the configured Home Assistant service through the Supervisor-backed API.

## 0.4.53

- Add the first in-app managed-notification settings UI so Watchtower can discover Home Assistant mobile app services, enable app-managed notifications, send a test notification, and edit per-camera event rules without leaving the app.

## 0.4.52

- Begin the move to app-managed notifications by adding persisted Watchtower notification settings, Home Assistant mobile app service discovery, and backend endpoints for in-app notification configuration and testing.
- Add opt-in managed notification delivery from Watchtower for ingested and webhook events using Home Assistant Core service calls through the Supervisor proxy.

## 0.4.51

- Restore direct mobile app notification services in the blueprint because Home Assistant's generic `notify.send_message` action rejects the companion-app-specific `data` payload used for images, deep links, and notification actions.

## 0.4.50

- Make the blueprint unlock action truly optional by defaulting it to an empty action list, so non-doorbell cameras no longer need a placeholder unlock step.

## 0.4.49

- Replace the blueprint's plain-text notification fields with notify-target selectors backed by `notify.send_message`, so phones can be chosen from the UI instead of typing service ids.

## 0.4.48

- Make the notification blueprint less front-door-specific by allowing empty doorbell sensors and replacing the required lock entity with an optional unlock action sequence.

## 0.4.47

- Fix the Home Assistant notification blueprint import by replacing unsupported `service` selectors with text inputs for notify service ids.

## 0.4.46

- Add per-camera event-type configuration so each participating channel can allow only the event types that matter, such as `PERSON,DOORBELL` for the front door and `PERSON,ANIMAL` for the backyard.
- Enforce allowed event types across event ingest, webhook handling, search results, and the Watchtower dashboard.
- Expand the Home Assistant blueprint to optionally relay `ANIMAL` and `VEHICLE` events for a camera.

## 0.4.45

- Resolve ingested Home Assistant events to the correct participating NVR channel by camera name before building timeline entries and buffered clips.
- Fall back to one-based channel correction for ingest payloads when camera names are unavailable, reducing channel-numbering mismatches.
- Log the enabled camera/channel map at startup so channel configuration problems are easier to diagnose.

## 0.4.44

- Continuously prune rolling-buffer transport stream segments while recording so buffer files no longer grow without bound.
- Include rolling-buffer files in storage maintenance and emergency storage-limit cleanup.
- Add a hard five-minute maximum age for rolling-buffer segments so stale pre-roll files are always removed aggressively.
- Normalize notification links to Home Assistant app deep links and include the specific event ID so "View Event Clip" works more reliably away from the local network.

## 0.4.43

- Hide empty NVR channel slots from the Watchtower camera picker so only real cameras appear in the UI.

## 0.4.42

- Add support for multiple participating cameras, including per-camera filtering in the Watchtower app.
- Add add-on configuration for participating cameras, buffered cameras, and the default live-view camera.

## 0.4.39

- Change the Home Assistant sidebar icon to a clearer camera/security glyph for Watchtower.

## 0.4.40

- Switch the Home Assistant sidebar icon to a plain camera glyph so the sidebar can render it reliably.

## 0.4.41

- Refresh the README screenshot to better match the current Watchtower UI.

## 0.4.38

- Remove the remaining live-view affordance from the app and notification flow so users are no longer sent to a nonfunctional stream page.

## 0.4.37

- Remove the live-view affordance from the app and notification actions since the live stream has not been reliable.
- Keep event playback focused on clips only.

## 0.4.36

- Normalize stale live links back to the app root with `?view=live` so existing events stop opening the dead `/live` path.
- Store the live notification target as the app root query URL instead of a path suffix.

## 0.4.35

- Delay buffered clip finalization until the full before/after window has elapsed so partial clips do not get finalized early.
- Reduce rolling-recorder ffmpeg logging to only surface warnings and errors.

## 0.4.33

- Delay buffered clip finalization until the full before/after window has elapsed so partial clips do not get finalized early.

## 0.4.32

- Make the notification live link point at the app's dedicated `/live` route instead of the query-based live view.
- Add a short dedupe window so the webhook and HA relay merge into one timeline event instead of creating duplicates.

## 0.4.31

- Route live links through the app root with `?view=live` so phones stay inside Watchtower without hitting ingress auth edge cases.
- Keep the app's live page header branded as Watchtower only.

## 0.4.30

- Keep live view inside Watchtower by linking event details and notifications to the app's `/app/live` route.
- Remove the standalone live label from the live page header.

## 0.4.29

- Point the live notification action at the Home Assistant camera dashboard instead of the app live route.
- Remove the standalone "Live" label from the live page header.

## 0.4.28

- Add a lightweight rolling-buffer watchdog that restarts the recorder task if it stops.
- Keep the rolling recorder logs readable by suppressing ffmpeg progress spam.

## 0.4.27

- Respect the configured clip window exactly instead of forcing a 10-second pre-roll floor.
- Silence ffmpeg stats spam from the rolling recorder.
- Align the add-on defaults with the current 1 second before / 15 second after setup.

## 0.4.26

- Rebrand the product to `Watchtower` and rename the repo and add-on slug for the larger HA migration.

## 0.4.25

- Bust cached snapshot previews and force the player to reload the newest event cleanly.

## 0.4.24

- Disable browser caching for the dashboard recent-events fetch so the newest event loads reliably.

## 0.4.23

- Sort the dashboard events client-side so the newest event always loads first.

## 0.4.22

- Force the dashboard to open the newest event on load.
- Remove the dead entry-id deep-link path from the web UI.

## 0.4.21

- Rename the visible app branding to `Front Door Watch`.
- Update the add-on panel title to match the app.

## 0.4.20

- Fix rolling-buffer segment discovery so buffered clips are recognized correctly.

## 0.4.19

- Stream ffmpeg output from the rolling recorder into the logs for easier diagnosis.

## 0.4.18

- Add rolling-buffer stats to the debug endpoint.
- Improve clip-builder diagnostics when no buffered segments are available.

## 0.4.17

- Retry buffered clip generation before falling back to direct RTSP.

## 0.4.16

- Re-encode the rolling RTSP recorder into short segments so the buffer produces usable files.

## 0.4.15

- Add more detailed rolling-buffer logging around clip selection and timing.

## 0.4.14

- Fix timestamp normalization in the rolling-buffer clip builder.
- Improve recorder diagnostics.

## 0.4.13

- Stamp relay events from the trigger time.
- Increase the buffered pre-roll window to keep clips aligned with the event.

## 0.4.12

- Improve the mobile layout so the player keeps more vertical room.

## 0.4.11

- Make the event list independently scrollable from the player.
- Compact the event metadata under the player.

## 0.4.10

- Restore the timeline index filename so existing event history loads again.

## 0.4.9

- Refresh the release version for the HA add-on update path.
