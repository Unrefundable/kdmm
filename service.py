"""
KDMM – service.py
Background service that runs for the lifetime of Kodi.

Responsibilities
────────────────
1. RESUME-SEEK
   When the bridge plugin resolves a stream it sets two global window
   properties before calling setResolvedUrl():
       kdmm.media_id    – e.g. "tt1234567" or "tt1234567:1:2"
       kdmm.resume_time – float seconds (only when > 5.0)

   BridgePlayer.onAVStarted() reads these properties, clears them, and
   calls seekTime() so the video starts at the correct position.

2. PROGRESS TRACKING
   BridgePlayer.onPlayBackStopped() and .onPlayBackEnded() save the
   current playback time to ProgressCache so KDMM knows where to
   resume on next play.

3. BROKEN STREAM RECOVERY
   BridgePlayer.onPlayBackError() clears the cached stream URL for an
   item so the next play attempt fetches a fresh one from DMM.
"""

import os
import sys

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

# ------------------------------------------------------------------ #
# Bootstrap
# ------------------------------------------------------------------ #
_ADDON = xbmcaddon.Addon()
_ADDON_ID = _ADDON.getAddonInfo("id")
_ADDON_PATH = _ADDON.getAddonInfo("path")
_USERDATA_PATH = xbmcvfs.translatePath(
    f"special://profile/addon_data/{_ADDON_ID}/"
)
sys.path.insert(0, os.path.join(_ADDON_PATH, "lib"))

from cache import StreamCache, ProgressCache   # noqa: E402

# ------------------------------------------------------------------ #
# One-time player JSON installer
# ------------------------------------------------------------------ #

def _install_player_json():
    """
    Copy resources/players/kdmm.json into the TMDb Bingie Helper players
    folder so it appears in settings on every device.
    """
    src = os.path.join(_ADDON_PATH, "resources", "players", "kdmm.json")
    dst_dir = xbmcvfs.translatePath(
        "special://profile/addon_data/plugin.video.tmdb.bingie.helper/players/"
    )
    dst = os.path.join(dst_dir, "kdmm.json")

    if not os.path.isfile(src):
        return

    with open(src, "r", encoding="utf-8") as f:
        src_content = f.read()

    if os.path.isfile(dst):
        with open(dst, "r", encoding="utf-8") as f:
            if f.read() == src_content:
                return

    xbmcvfs.mkdirs(dst_dir)

    with open(dst, "w", encoding="utf-8") as f:
        f.write(src_content)

    xbmc.log("[KDMM Service] Installed player JSON to TMDb Bingie Helper players folder", xbmc.LOGINFO)


# ------------------------------------------------------------------ #
# Window property keys (must match default.py)
# ------------------------------------------------------------------ #
WIN = xbmcgui.Window(10000)
PROP_MEDIA_ID = "kdmm.media_id"
PROP_RESUME_TIME = "kdmm.resume_time"
PROP_CANDIDATES = "kdmm.candidates"

WATCHED_MARGIN_SECONDS = 60
MIN_CONTENT_SECONDS = 60


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Service] {msg}", level)


# ------------------------------------------------------------------ #
# Player monitor
# ------------------------------------------------------------------ #

class BridgePlayer(xbmc.Player):
    def __init__(self):
        super().__init__()
        self._current_media_id = None
        self._current_url = None
        self._last_known_time = 0.0
        self._last_known_total = 0.0
        self._tried_urls = set()
        self._stream_cache = StreamCache(_USERDATA_PATH)
        self._progress_cache = ProgressCache(_USERDATA_PATH)

    def tick(self):
        if self._current_media_id and self.isPlaying():
            try:
                t = self.getTime()
                total = self.getTotalTime()
                if t > 0:
                    self._last_known_time = t
                if total > 0:
                    self._last_known_total = total
            except Exception:
                pass

    def onAVStarted(self):
        media_id = WIN.getProperty(PROP_MEDIA_ID)
        if not media_id:
            return

        self._current_media_id = media_id
        self._last_known_time = 0.0
        self._last_known_total = 0.0
        self._tried_urls = set()
        self._current_url = None
        try:
            self._current_url = self.getPlayingFile().split("|")[0]
        except Exception:
            pass
        WIN.clearProperty(PROP_MEDIA_ID)

        resume_str = WIN.getProperty(PROP_RESUME_TIME)
        WIN.clearProperty(PROP_RESUME_TIME)

        if resume_str:
            try:
                resume_time = float(resume_str)
            except (ValueError, TypeError):
                resume_time = 0.0

            if resume_time > 5.0:
                _log(f"Applying resume seek to {resume_time:.1f}s for {media_id}")
                import threading
                def _seek():
                    xbmc.sleep(1200)
                    try:
                        self.seekTime(resume_time)
                    except Exception as exc:
                        _log(f"seekTime() failed: {exc}", xbmc.LOGWARNING)
                threading.Thread(target=_seek, daemon=True).start()

    def onPlayBackStopped(self):
        self._handle_playback_stop(is_ended=False)

    def onPlayBackEnded(self):
        self._handle_playback_stop(is_ended=True)

    def onPlayBackError(self):
        media_id = self._current_media_id or WIN.getProperty(PROP_MEDIA_ID)
        self._current_media_id = None
        WIN.clearProperty(PROP_MEDIA_ID)
        WIN.clearProperty(PROP_RESUME_TIME)

        if not media_id:
            return

        try:
            failed = self.getPlayingFile().split("|")[0]
            self._tried_urls.add(failed)
        except Exception:
            pass
        if self._current_url:
            self._tried_urls.add(self._current_url)

        _log(f"Playback error for {media_id} – trying next candidate", xbmc.LOGWARNING)
        self._try_next_candidate(media_id)

    def _handle_playback_stop(self, is_ended):
        media_id = self._current_media_id
        if not media_id:
            return

        total_time = self._last_known_total

        if 0 < total_time < MIN_CONTENT_SECONDS:
            _log(
                f"Stream too short ({total_time:.1f}s) for {media_id} "
                "– treating as failed stream, retrying next candidate",
                xbmc.LOGWARNING,
            )
            self._current_media_id = None
            if self._current_url:
                self._tried_urls.add(self._current_url)
            self._try_next_candidate(media_id)
        else:
            self._save_progress(is_ended=is_ended)

    def _try_next_candidate(self, media_id):
        import json, threading
        from urllib.parse import urlencode
        from dmm import is_stream_accessible

        candidates_json = WIN.getProperty(PROP_CANDIDATES)
        candidates = []
        if candidates_json:
            try:
                candidates = json.loads(candidates_json)
            except Exception:
                pass

        next_stream = None
        for c in candidates:
            url = c.get("url", "").split("|")[0]
            if url in self._tried_urls:
                continue
            if not is_stream_accessible(url, c.get("headers") or {}):
                _log(f"Candidate {c.get('name', '?')!r} too small – skipping", xbmc.LOGWARNING)
                self._tried_urls.add(url)
                continue
            next_stream = c
            break

        if not next_stream:
            _log(
                f"All {len(candidates)} candidate(s) failed for {media_id} – clearing cache",
                xbmc.LOGWARNING,
            )
            self._stream_cache.clear(media_id)
            WIN.clearProperty(PROP_CANDIDATES)
            xbmcgui.Dialog().notification(
                "KDMM",
                "All available streams failed – cache cleared, please try again",
                xbmcgui.NOTIFICATION_ERROR,
                6000,
            )
            return

        def _retry():
            url = next_stream["url"]
            headers = next_stream.get("headers") or {}
            self._tried_urls.add(url)

            if headers:
                final_url = f"{url}|{urlencode(headers)}"
            else:
                final_url = url

            li = xbmcgui.ListItem(path=final_url)
            li.setProperty("IsPlayable", "true")
            if headers:
                li.setProperty("inputstream", "inputstream.ffmpegdirect")

            WIN.setProperty(PROP_MEDIA_ID, media_id)
            _log(f"Retrying with next candidate: {next_stream['name']!r}")
            xbmc.sleep(800)
            self.play(final_url, li)

        threading.Thread(target=_retry, daemon=True).start()

    def _save_progress(self, is_ended):
        media_id = self._current_media_id
        if not media_id:
            return
        self._current_media_id = None

        current_time = self._last_known_time
        total_time = self._last_known_total

        if total_time <= 0 or current_time <= 0:
            _log(f"No valid time data for {media_id} – progress not saved", xbmc.LOGWARNING)
            return

        near_end = (total_time - current_time) < WATCHED_MARGIN_SECONDS
        if is_ended or near_end:
            _log(f"Marking {media_id} as watched")
            self._progress_cache.set_progress(
                media_id, 0.0, total_time=total_time, watched=True
            )
        elif current_time > 5.0:
            _log(f"Saving resume position {current_time:.1f}s / {total_time:.1f}s for {media_id}")
            self._progress_cache.set_progress(
                media_id, current_time, total_time=total_time, watched=False
            )


# ------------------------------------------------------------------ #
# Service entry point
# ------------------------------------------------------------------ #

class BridgeMonitor(xbmc.Monitor):
    def __init__(self):
        super().__init__()
        self.player = BridgePlayer()

    def run(self):
        _log("Service started")
        _install_player_json()
        while not self.abortRequested():
            self.player.tick()
            self.waitForAbort(5)
        _log("Service stopped")


if __name__ == "__main__":
    monitor = BridgeMonitor()
    monitor.run()
