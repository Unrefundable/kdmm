# KDMM

KDMM (Kodi Debrid Media Manager) is a Kodi video add-on for playing titles from
TMDb Bingie Helper through cached debrid streams.

## What It Does

- Receives movie and episode play requests from TMDb Bingie Helper.
- Looks up matching torrent hashes through Torrentio (free, no key). With an
  optional Debrid Media Manager sponsor API key it also queries DMM and merges
  the results. DMM's open endpoints refuse tools since November 2026.
- Resolves cached streams through Real-Debrid and/or AllDebrid.
- Sorts candidates by quality preferences, including resolution, HDR format,
  source type, preferred release groups, title match, and TV pack preference.
- Filters AV1 streams with filename/title detection and optional direct metadata
  probing.
- Hands Kodi a direct playback URL and cleans up provider-owned stream state
  after playback ends or fails.
- Caches resolved candidates, resume positions, blocked Real-Debrid hashes, and
  preferred TV pack bindings.
- Lets you block the last played source from the Cache settings when a stream
  is laggy or bad, so the next play picks a different one, and pick individual
  cached streams to clear or blocked sources to unblock.
- Supports local resume, watched progress, and play-from-beginning behavior.
- Adds IntroDB.app and TheIntroDB-backed skip buttons for intro, recap, credits,
  and preview segments.
- Adds manual and automatic next-episode playback for TV episodes.
- Opens the movie's or show's page when playback stops or ends, built
  directly by TMDb Bingie Helper so it is always the title that was playing.
- Installs remote keymaps: Back stops playback, Up/Down shows the OSD, and
  FF/RW add 10 minutes per press then seek once. These replace the
  `service.kodi.lg` add-on, which KDMM disables on start.

## Install

Install the Kodi repository zip from:

```text
https://unrefundable.github.io/kdmm
```

After installing the repository, install or update the KDMM add-on from Kodi's
add-on browser.

## Requirements

- Kodi 21+ with Python 3 add-on support.
- TMDb Bingie Helper configured to use the KDMM player.
- A Real-Debrid and/or AllDebrid account authorized in KDMM settings.
- `script.module.requests`.
- `script.module.qrcode` is optional and used for Real-Debrid device-code
  authorization.

## Current Release

The current published add-on version is `2.7.3`.
