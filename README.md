<p align="center">
  <img alt="KDMM" title="KDMM" src="resources/icon.png" width="128">
</p>

<h1 align="center">KDMM</h1>

<p align="center">
  <b>Kodi Debrid Media Manager: cached debrid playback for TMDb Bingie Helper</b>
</p>

<div align="center">

<a href="https://unrefundable.github.io/kdmm">
  <img alt="Latest version" src="https://img.shields.io/badge/dynamic/xml?url=https%3A%2F%2Fraw.githubusercontent.com%2FUnrefundable%2Fkdmm%2Fmain%2Faddon.xml&query=%2Faddon%2F%40version&label=version&prefix=v&color=3B7DDD&style=flat">
</a>
<img alt="Kodi 21 or newer" src="https://img.shields.io/badge/Kodi-21%2B-17B2E7.svg?style=flat&logo=kodi&logoColor=white">
<img alt="Python 3" src="https://img.shields.io/badge/Python-3-3776AB.svg?style=flat&logo=python&logoColor=white">
<a href="https://www.gnu.org/licenses/gpl-3.0">
  <img alt="License: GPL v3" src="https://img.shields.io/badge/license-GPL%20v3-2B6DBE.svg?style=flat">
</a>

</div>

<p align="center">
  <a href="https://unrefundable.github.io/kdmm"><b>Install</b></a> ·
  <a href="#features">Features</a> ·
  <a href="#installation">Installation</a> ·
  <a href="#requirements">Requirements</a> ·
  <a href="#how-it-works">How it works</a> ·
  <a href="#building-from-source">Build from source</a>
</p>

---

**KDMM** (Kodi Debrid Media Manager) is an open-source Kodi video add-on that plays movies and episodes from TMDb Bingie Helper through cached debrid streams. Pick a title, and KDMM finds a matching release, resolves it through Real-Debrid or AllDebrid, and hands Kodi a direct playback URL. There are no source lists to scroll through.

It also covers what happens around playback: resume and watched progress, skip buttons for intros and credits, next-episode playback, and remote control keymaps made for the couch.

> [!NOTE]
> KDMM doesn't host or index any content. It needs your own Real-Debrid or AllDebrid account, and it isn't affiliated with Debrid Media Manager, Torrentio, Real-Debrid, AllDebrid or TMDb Bingie Helper.

## Features

### Finding a stream
- **One-click playback.** KDMM receives movie and episode play requests from TMDb Bingie Helper and picks the best cached stream for you.
- **Torrentio lookups.** Matching torrents are found through Torrentio, which is free and needs no key. With an optional Debrid Media Manager sponsor API key, KDMM also queries DMM and merges the results.
- **Real-Debrid and AllDebrid.** Use either service or both, and set which one is tried first.
- **Quality sorting.** Candidates are ranked by your preferences for resolution, HDR format, source type, release groups and TV packs.
- **Strict title matching.** Optionally prefer no playback over playing the wrong title.
- **AV1 filtering.** AV1 streams are detected from the filename and title, with optional probing of the resolved file's metadata.
- **Preferred audio language.** Favor releases that carry the audio language you choose.

### Playback
- **Automatic fallback.** If a stream fails to start, KDMM moves on to the next candidate.
- **Resume and watched progress.** Positions are saved locally, with resume and play-from-beginning options.
- **Skip buttons.** Skip Intro, Recap, Credits and Preview buttons, using timings from IntroDB.app and TheIntroDB.
- **Next episode.** A Play Next Episode button, plus optional automatic playback when an episode ends. A couple of minutes into an episode KDMM already looks up the next one in the background, so the switch does not wait on a search.
- **No wait for "Play Season X: Episode Y".** KDMM remembers each show's next episode (from what you just watched and from TMDb Bingie Helper's last Trakt answer) and gives it to the skin as soon as the show's page opens, instead of showing "Loading..." for seconds. The Trakt answer still replaces it when it arrives. Needs Deflix 3.3.2 or newer.
- **Back to the title.** When playback stops or ends, KDMM opens the movie's or show's page, built by TMDb Bingie Helper so it is always the title you were watching.
- **Clean up.** Provider-owned stream state is removed after playback ends or fails.

### Cache and source control
- **Stream cache.** Resolved streams and preferred TV pack bindings are cached, so replays and the next episode start faster. You choose how long the cache lasts.
- **Streams ready before you press Play (optional).** Turn on *Find streams for menu titles after 10 idle minutes* and KDMM looks up the best stream for the movies and in-progress shows on the home screen and provider hub rows you have browsed while Kodi sits idle. Saved streams keep working across reboots: KDMM stores which file was chosen and asks the debrid service for a fresh link when the old one has aged. Any remote press stops it. Off by default.
- **Block a bad source.** If a stream is laggy or broken, block the last played source and the next play picks a different one.
- **Manage both lists.** Clear individual cached streams or all of them, and unblock individual sources or all of them.


## Installation

1. In Kodi, go to **Settings › File manager › Add source** and add:

   ```text
   https://unrefundable.github.io/kdmm
   ```

2. Go to **Add-ons › Install from zip file**, open the source you added, and install `repository.unrefundable.kdmm-1.0.0.zip`.
3. Go to **Install from repository › Unrefundable KDMM Repo › Video add-ons** and install **KDMM**.
4. Open KDMM's settings and authorize Real-Debrid, AllDebrid or both under **Debrid Accounts**.

KDMM makes itself TMDb Bingie Helper's default player the first time it runs. To choose a different player later, change it in the helper's settings under **Players**; KDMM's **Make KDMM the default player** setting switches it back.

From then on, Kodi keeps KDMM up to date through the repository.

## Requirements

- **Kodi 21 or newer**, with Python 3 add-on support.
- **TMDb Bingie Helper**. KDMM sets itself as its default player on first run.
- **A Real-Debrid or AllDebrid account**, authorized in KDMM's settings.
- `script.module.requests`, which Kodi installs automatically.
- `script.module.qrcode` is optional. It shows a QR code for Real-Debrid device-code authorization.

## How it works

TMDb Bingie Helper sends KDMM a play request with the title's IDs. KDMM looks up torrent hashes for that title through Torrentio, and through Debrid Media Manager when a sponsor API key is set, then asks your debrid service which of them are already cached. The cached candidates are ranked by your quality preferences, the best one is resolved to a direct URL, and Kodi plays it.

A background service follows playback. It saves resume positions, shows the skip and next-episode buttons at the right moments, and cleans up on the debrid service once you stop. Your API keys and tokens stay in Kodi's add-on settings on your device.

## Building from source

The Kodi repository is served by GitHub Pages from the `docs/` folder. To build the add-on zip and update the repository files:

```bash
scripts/publish_github_pages_docs.sh
```

The version in `addon.xml` must match the `plugin.video.kdmm` entry in `docs/addons.xml`. The script checks this before it builds.

## License

KDMM is released under the [GNU General Public License v3.0](LICENSE). You're free to use, modify and distribute it under the terms of the license.

## Credits

- **TMDb Bingie Helper**, which sends KDMM its play requests and builds the title pages.
- [Debrid Media Manager](https://debridmediamanager.com), the project KDMM is named after and can query with a sponsor API key.
- [Torrentio](https://torrentio.strem.fun) for torrent lookups.
- [IntroDB.app](https://introdb.app) and [TheIntroDB](https://theintrodb.org) for intro, recap, credits and preview timings.
