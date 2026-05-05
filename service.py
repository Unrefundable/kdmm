"""KDMM service: resume, retry, and IntroDB-backed skip/next overlays."""

import json
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

from cache import StreamCache, ProgressCache, PackBindingCache   # noqa: E402
from dmm import is_av1_stream  # noqa: E402
from introdb_client import query_all_segments  # noqa: E402
from next_episode import get_next_episode, play_next_episode  # noqa: E402
from playback import apply_playback_metadata, decode_playback_context  # noqa: E402
from segment_overlay import show_skip_overlay  # noqa: E402

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

    if not xbmcvfs.exists(src):
        return

    try:
        f = xbmcvfs.File(src)
        src_content = f.read()
        f.close()
    except Exception as exc:
        xbmc.log(f"[KDMM Service] Could not read player JSON src: {exc}", xbmc.LOGWARNING)
        return

    if xbmcvfs.exists(dst):
        try:
            f = xbmcvfs.File(dst)
            dst_content = f.read()
            f.close()
            if dst_content == src_content:
                return
        except Exception:
            pass

    xbmcvfs.mkdirs(dst_dir)

    try:
        f = xbmcvfs.File(dst, "w")
        f.write(src_content)
        f.close()
        xbmc.log("[KDMM Service] Installed player JSON to TMDb Bingie Helper players folder", xbmc.LOGINFO)
    except Exception as exc:
        xbmc.log(f"[KDMM Service] Could not write player JSON: {exc}", xbmc.LOGWARNING)


# ------------------------------------------------------------------ #
# Window property keys (must match default.py)
# ------------------------------------------------------------------ #
WIN = xbmcgui.Window(10000)
PROP_MEDIA_ID = "kdmm.media_id"
PROP_RESUME_TIME = "kdmm.resume_time"
PROP_CANDIDATES = "kdmm.candidates"
PROP_PLAYBACK_CONTEXT = "kdmm.playback_context"

WATCHED_MARGIN_SECONDS = 60
MIN_CONTENT_SECONDS = 60
SEGMENT_END_MARGIN_SECONDS = 0.25
NEXT_EPISODE_FALLBACK_SECONDS = 45
POST_CREDITS_SCENE_MIN_SECONDS = 20


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Service] {msg}", level)


def _setting_bool(key, default=False):
    try:
        value = xbmcaddon.Addon().getSetting(key)
    except Exception:
        value = "true" if default else "false"
    if value == "":
        return default
    return value.lower() == "true"


def _setting_int(key, default):
    try:
        value = xbmcaddon.Addon().getSetting(key)
        return int(value) if value not in (None, "") else default
    except Exception:
        return default


def _should_show_segment_button(processed_segments, segment_key, current_time,
                                segment_start, segment_end, margin=SEGMENT_END_MARGIN_SECONDS):
    state = processed_segments.setdefault(segment_key, {
        "inside": False,
        "shown_for_entry": False,
        "last_time": None,
    })

    inside_segment = segment_start <= current_time < (segment_end - margin)
    previous_time = state.get("last_time")

    if not inside_segment:
        state["inside"] = False
        state["shown_for_entry"] = False
        state["last_time"] = current_time
        return False

    reentered = not state["inside"]
    if previous_time is not None and current_time + margin < previous_time:
        reentered = True

    if reentered:
        state["shown_for_entry"] = False

    state["inside"] = True
    state["last_time"] = current_time

    if state["shown_for_entry"]:
        return False

    state["shown_for_entry"] = True
    return True


class SegmentController:
    def __init__(self):
        self.reset()

    def reset(self):
        self._current_file = None
        self._segments = None
        self._processed = {}
        self._next_episode = None
        self._next_episode_checked = False
        self._next_overlay_shown = False

    def tick(self, player, monitor):
        if not player.isPlayingVideo() or not player._playback_context:
            self.reset()
            return

        filename = player.getPlayingFile() if player.isPlaying() else None
        if not filename:
            return
        filename = filename.split("|")[0]

        if filename != self._current_file:
            self.reset()
            self._current_file = filename
            _log(f"Segment tracking reset for file: {filename}")

        context = player._playback_context or {}
        current_time = player._last_known_time if player._last_known_time > 0 else self._safe_get_time(player)
        total_time = player._last_known_total if player._last_known_total > 0 else self._safe_get_total(player)
        if total_time <= 0:
            return

        if self._segments is None and _setting_bool("segment_lookups_enabled", True):
            self._segments = query_all_segments(
                tmdb_id=context.get("tmdb_id"),
                imdb_id=context.get("imdb_id"),
                season=context.get("season"),
                episode=context.get("episode"),
                is_movie=context.get("is_movie", False),
            )

        all_segments = self._build_enabled_segments(self._segments or {})

        for idx, segment in enumerate(all_segments):
            segment_type = segment["type"]
            api_start = segment.get("start")
            api_end = segment.get("end")

            if api_start is None and api_end is None:
                continue
            if api_start is None:
                api_start = 0.0
            effective_end = total_time if api_end is None else api_end
            if effective_end <= api_start:
                continue

            segment_key = f"{segment_type}_{idx}"
            if not _should_show_segment_button(self._processed, segment_key, current_time, api_start, effective_end):
                continue

            action_type = segment_type
            next_episode = None
            if _setting_bool("enable_next_episode_button", True) and not context.get("is_movie", False):
                next_episode = self._get_next_episode(context)
                if next_episode and self._should_use_next_overlay(all_segments, idx, segment_type, api_end, total_time):
                    action_type = "next_episode"
                    self._next_overlay_shown = True

            if action_type == "next_episode" and next_episode:
                pressed = show_skip_overlay(
                    segment_end=effective_end,
                    player=player,
                    monitor=monitor,
                    segment_type="next_episode",
                )
                if pressed:
                    _log(f"Opening next episode S{next_episode['season']}E{next_episode['episode']}")
                    play_next_episode(next_episode)
                continue

            pressed = show_skip_overlay(
                segment_end=effective_end,
                player=player,
                monitor=monitor,
                segment_type=segment_type,
            )
            if pressed:
                self._seek_past_segment(player, effective_end, segment_type, total_time)

        if _setting_bool("enable_next_episode_button", True) and not context.get("is_movie", False):
            self._show_fallback_next_episode(player, monitor, context, current_time, total_time)

    def _build_enabled_segments(self, segments):
        enabled = []
        mapping = {
            "intro": "enable_intro_button",
            "recap": "enable_recap_button",
            "credits": "enable_credits_button",
            "preview": "enable_preview_button",
        }
        for segment_type in ("intro", "recap", "credits", "preview"):
            if not _setting_bool(mapping[segment_type], True):
                continue
            for segment in segments.get(segment_type, []):
                item = dict(segment)
                item["type"] = segment_type
                enabled.append(item)
        enabled.sort(key=lambda item: item.get("start") if item.get("start") is not None else 0.0)
        return enabled

    def _get_next_episode(self, context):
        if not self._next_episode_checked:
            self._next_episode = get_next_episode(context)
            self._next_episode_checked = True
        return self._next_episode

    def _should_use_next_overlay(self, all_segments, idx, segment_type, api_end, total_time):
        if segment_type == "credits":
            return True
        if segment_type != "preview":
            return False
        later_segments = [
            seg for seg in all_segments[idx + 1:]
            if (seg.get("start") or 0) > (all_segments[idx].get("start") or 0)
        ]
        if later_segments:
            return False
        if api_end is None:
            return True
        trailing_content = max(0.0, total_time - api_end)
        return trailing_content <= POST_CREDITS_SCENE_MIN_SECONDS

    def _seek_past_segment(self, player, segment_end, segment_type, total_time):
        offset = _setting_int("skip_offset_seconds", 2)
        target = segment_end + offset
        if target >= total_time:
            target = max(0.0, total_time - 10.0)
        try:
            _log(f"Skipping {segment_type}: target {target:.1f}s")
            player.seekTime(target)
        except Exception as exc:
            _log(f"seekTime failed for {segment_type}: {exc}", xbmc.LOGWARNING)

    def _show_fallback_next_episode(self, player, monitor, context, current_time, total_time):
        if self._next_overlay_shown or total_time <= 0:
            return
        next_episode = self._get_next_episode(context)
        if not next_episode:
            return
        start = max(0.0, total_time - NEXT_EPISODE_FALLBACK_SECONDS)
        if not _should_show_segment_button(self._processed, "next_episode_fallback", current_time, start, total_time):
            return
        self._next_overlay_shown = True
        pressed = show_skip_overlay(
            segment_end=total_time,
            player=player,
            monitor=monitor,
            segment_type="next_episode",
        )
        if pressed:
            _log(f"Opening fallback next episode S{next_episode['season']}E{next_episode['episode']}")
            play_next_episode(next_episode)

    @staticmethod
    def _safe_get_time(player):
        try:
            return player.getTime()
        except Exception:
            return 0.0

    @staticmethod
    def _safe_get_total(player):
        try:
            return player.getTotalTime()
        except Exception:
            return 0.0


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
        self._playback_context = {}

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
        self._playback_context = decode_playback_context(
            WIN.getProperty(PROP_PLAYBACK_CONTEXT)
        )
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

        playback_context = dict(self._playback_context or {})
        total_time = self._last_known_total
        next_episode = None
        should_auto_next = (
            is_ended
            and _setting_bool("auto_play_next_episode", True)
            and not playback_context.get("is_movie", False)
        )
        if should_auto_next:
            next_episode = get_next_episode(playback_context)

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
            if next_episode:
                self._open_next_episode(next_episode)

    def _open_next_episode(self, next_episode):
        import threading

        def _open():
            xbmc.sleep(800)
            _log(
                f"Auto-playing next episode S{next_episode['season']}E{next_episode['episode']}"
            )
            if not play_next_episode(next_episode):
                _log("Automatic next-episode playback failed", xbmc.LOGWARNING)

        threading.Thread(target=_open, daemon=True).start()

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
            if is_av1_stream(c):
                _log(f"Candidate {c.get('name', '?')!r} is AV1 – skipping", xbmc.LOGWARNING)
                continue
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
            if media_id and media_id.count(":") >= 2:
                imdb_id, season, _episode = media_id.split(":", 2)
                PackBindingCache(_USERDATA_PATH).clear(imdb_id, season)
            WIN.clearProperty(PROP_CANDIDATES)
            WIN.clearProperty(PROP_PLAYBACK_CONTEXT)
            self._playback_context = {}
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
            apply_playback_metadata(
                li,
                self._playback_context or decode_playback_context(WIN.getProperty(PROP_PLAYBACK_CONTEXT)),
            )
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
        WIN.clearProperty(PROP_PLAYBACK_CONTEXT)
        self._playback_context = {}

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
        self.segment_controller = SegmentController()

    def run(self):
        _log("Service started")
        try:
            _install_player_json()
        except Exception as exc:
            _log(f"_install_player_json failed: {exc}", xbmc.LOGWARNING)
        while not self.abortRequested():
            self.player.tick()
            self.segment_controller.tick(self.player, self)
            self.waitForAbort(0.5)
        _log("Service stopped")


if __name__ == "__main__":
    monitor = BridgeMonitor()
    monitor.run()
