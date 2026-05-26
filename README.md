# KDMM

KDMM (Kodi Debrid Media Manager) is a Kodi video add-on for playing titles from
TMDb Bingie Helper through Debrid Media Manager search results and cached debrid
streams.

## What It Does

- Receives movie and episode play requests from TMDb Bingie Helper.
- Queries Debrid Media Manager for matching torrent hashes.
- Resolves cached streams through Real-Debrid and/or AllDebrid.
- Sorts candidates by quality preferences, including resolution, HDR format,
  source type, preferred release groups, title match, and TV pack preference.
- Filters AV1 streams with filename/title detection and optional direct metadata
  probing.
- Hands Kodi a direct playback URL and keeps fallback candidates for automatic
  retry when a stream fails to start.
- Caches resolved candidates, resume positions, blocked Real-Debrid hashes, and
  preferred TV pack bindings.
- Supports local resume, watched progress, and play-from-beginning behavior.
- Adds IntroDB.app and TheIntroDB-backed skip buttons for intro, recap, credits,
  and preview segments.
- Adds manual and automatic next-episode playback for TV episodes.

## Install

Install the Kodi repository zip from:

```text
https://unrefundable.github.io/kdmm/repository.unrefundable.kdmm-1.0.0.zip
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

The current published add-on version is `2.6.17`.
