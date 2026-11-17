"""KDMM service: resume, IntroDB-backed skip/next overlays, return to the
title page after playback, and the remote keymaps."""

import json
import os
import sys
import threading
import time
from urllib.parse import unquote, urlsplit

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

from cache import ProgressCache   # noqa: E402
from choices import migrate_choice_settings  # noqa: E402
from default_player import claim_default_player_once  # noqa: E402
from details_page import open_details, prepare_details  # noqa: E402
from device_tuning import apply_device_tuning  # noqa: E402
from dmm import cleanup_stream_candidate  # noqa: E402
from introdb_client import query_all_segments  # noqa: E402
from next_episode import get_next_episode, next_episode_from_tmdb, play_next_episode  # noqa: E402
from next_warm import NextEpisodeWarmer  # noqa: E402
from playback import decode_playback_context  # noqa: E402
from prewarm import PrewarmController  # noqa: E402
import transition  # noqa: E402
from remote import SeekAccumulator, disable_legacy_addon, install_keymaps  # noqa: E402
from segment_overlay import show_skip_overlay  # noqa: E402
from settings_persistence import get_stream_cache_ttl_hours  # noqa: E402
from upnext import UpNextMemory  # noqa: E402
from user_messages import describe_playback_failure  # noqa: E402

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
PROP_PENDING_PLAYBACK = "kdmm.pending_playback"
PROP_PREPARE_DETAILS = "kdmm.prepare_details"

WATCHED_MARGIN_SECONDS = 60
MIN_CONTENT_SECONDS = 60
PLAYBACK_START_TIMEOUT_SECONDS = 30
SEGMENT_END_MARGIN_SECONDS = 0.25
NEXT_EPISODE_FALLBACK_SECONDS = 45
POST_CREDITS_SCENE_MIN_SECONDS = 20
RETURN_TO_DETAILS_WAIT_SECONDS = 5
# A list of the show's episodes or seasons: the user came from the title page
# to here, so opening the page again would stack it on top of its own list.
EPISODE_LIST_VISIBLE = (
    "Window.IsActive(videos) + [Container.Content(episodes) | Container.Content(seasons)]"
)
STOP_BEFORE_NEXT_TIMEOUT_SECONDS = 8
CLEANUP_JOIN_TIMEOUT_SECONDS = 5
FAILED_START_CHECK_SECONDS = 2.0
SEGMENT_LOOKUP_RETRY_SECONDS = 60
SEGMENT_LOOKUP_MAX_ATTEMPTS = 3
# Kodi also reports "ended" when a stream drops part-way. Only an end this
# close to the full length counts as having watched the item.
ENDED_MARGIN_SECONDS = 120
# A skipped segment's button stays away unless the user seeks back to before it.
SEGMENT_REARM_MARGIN_SECONDS = 2.0


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Service] {msg}", level)


def _safe_url(url):
    """Host and file name only: a debrid link's path is a credential."""
    try:
        parts = urlsplit(url or "")
        name = unquote(parts.path.rsplit("/", 1)[-1])
        return f"{parts.netloc}/…/{name}" if parts.netloc else name
    except Exception:
        return "stream"


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
        self._skipped = set()
        self._segment_attempts = 0
        self._segment_retry_at = 0.0

    def tick(self, player, monitor):
        if not player.isPlayingVideo() or not player._playback_context:
            self.reset()
            return

        try:
            filename = player.getPlayingFile() if player.isPlaying() else None
        except RuntimeError:
            # Playback stopped between the two calls.
            filename = None
        if not filename:
            return
        filename = filename.split("|")[0]

        if filename != self._current_file:
            self.reset()
            self._current_file = filename
            transition.clear()
            _log(f"Segment tracking reset for {_safe_url(filename)}")

        context = player._playback_context or {}
        if transition.active():
            return
        current_time = player._last_known_time if player._last_known_time > 0 else self._safe_get_time(player)
        total_time = player._last_known_total if player._last_known_total > 0 else self._safe_get_total(player)
        if total_time <= 0:
            return

        if (
            self._segments is None
            and time.time() >= self._segment_retry_at
            and _setting_bool("segment_lookups_enabled", True)
        ):
            self._segments = query_all_segments(
                tmdb_id=context.get("tmdb_id"),
                imdb_id=context.get("imdb_id"),
                season=context.get("season"),
                episode=context.get("episode"),
                is_movie=context.get("is_movie", False),
            )
            if self._segments is None:
                # No timing service answered. Ask again shortly instead of
                # going without skip buttons for the whole episode.
                self._segment_attempts += 1
                if self._segment_attempts >= SEGMENT_LOOKUP_MAX_ATTEMPTS:
                    self._segments = {}
                else:
                    self._segment_retry_at = time.time() + SEGMENT_LOOKUP_RETRY_SECONDS

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
            if segment_key in self._skipped:
                # Right after a skip Kodi can still report the old position
                # for a moment; that must not bring the button back.
                if current_time >= api_start - SEGMENT_REARM_MARGIN_SECONDS:
                    continue
                self._skipped.discard(segment_key)
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
                    player.switch_to_episode(next_episode, monitor)
                # The overlay blocked this loop, so the position read above
                # is stale. Look again on the next tick.
                return

            pressed = show_skip_overlay(
                segment_end=effective_end,
                player=player,
                monitor=monitor,
                segment_type=segment_type,
            )
            if pressed:
                self._skipped.add(segment_key)
                # Any other segment of this type that covers the same stretch
                # has been dealt with by this press too.
                for other_idx, other in enumerate(all_segments):
                    if other["type"] != segment_type or other_idx == idx:
                        continue
                    other_start = other.get("start") or 0.0
                    other_end = total_time if other.get("end") is None else other["end"]
                    if other_start < effective_end and api_start < other_end:
                        self._skipped.add(f"{segment_type}_{other_idx}")
                self._seek_past_segment(player, effective_end, segment_type, total_time)
            return

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
            player.switch_to_episode(next_episode, monitor)

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
        self._current_candidate = None
        self._last_known_time = 0.0
        self._last_known_total = 0.0
        self._tried_urls = set()
        self._progress_cache = ProgressCache(_USERDATA_PATH)
        self._playback_context = {}
        self._was_fullscreen = False
        self._cleanup_threads = []
        self.upnext = None
        self._failed_start_checking = None

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
            self._was_fullscreen = xbmc.getCondVisibility("Window.IsActive(fullscreenvideo)")
        self._prepare_requested_details()
        self._check_pending_playback_start()

    def _prepare_requested_details(self):
        """Build the title page as soon as a play request names its title.

        The request is written when Play is pressed, so the page is built
        while KDMM is still looking for a stream and nothing is decoding,
        not during playback.
        """
        raw = WIN.getProperty(PROP_PREPARE_DETAILS)
        if not raw:
            return
        WIN.clearProperty(PROP_PREPARE_DETAILS)
        if _setting_bool("return_to_details", True):
            prepare_details(decode_playback_context(raw))

    def _check_pending_playback_start(self):
        pending_json = WIN.getProperty(PROP_PENDING_PLAYBACK)
        if not pending_json or self.isPlayingVideo():
            return
        try:
            pending = json.loads(pending_json)
        except Exception:
            WIN.clearProperty(PROP_PENDING_PLAYBACK)
            return
        media_id = pending.get("media_id")
        started_at = float(pending.get("timestamp") or 0)
        if not media_id or time.time() - started_at < PLAYBACK_START_TIMEOUT_SECONDS:
            return
        url = (pending.get("url") or "").split("|")[0]
        if url:
            self._tried_urls.add(url)
        failed_candidate = self._candidate_for_url(url)
        WIN.clearProperty(PROP_PENDING_PLAYBACK)
        _log(
            f"Playback did not start for {media_id}; keeping cached source for URL refresh",
            xbmc.LOGWARNING,
        )
        self._clear_failed_stream(
            media_id,
            "Stream did not start. KDMM kept the cached source; retry playback to refresh the link.",
            failed_candidate=failed_candidate,
            failed_url=url,
        )

    def onAVStarted(self):
        media_id = WIN.getProperty(PROP_MEDIA_ID)
        if not media_id:
            return

        self._current_media_id = media_id
        self._last_known_time = 0.0
        self._last_known_total = 0.0
        self._tried_urls = set()
        self._current_url = None
        self._current_candidate = None
        self._playback_context = decode_playback_context(
            WIN.getProperty(PROP_PLAYBACK_CONTEXT)
        )
        try:
            self._current_url = self.getPlayingFile().split("|")[0]
        except Exception:
            pass
        self._current_candidate = self._candidate_for_url(self._current_url)
        WIN.clearProperty(PROP_MEDIA_ID)
        WIN.clearProperty(PROP_PENDING_PLAYBACK)
        transition.clear()

        resume_str = WIN.getProperty(PROP_RESUME_TIME)
        WIN.clearProperty(PROP_RESUME_TIME)

        if resume_str:
            try:
                resume_time = float(resume_str)
            except (ValueError, TypeError):
                resume_time = 0.0

            if resume_time > 5.0:
                _log(f"Applying resume seek to {resume_time:.1f}s for {media_id}")
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
        WIN.clearProperty(PROP_PENDING_PLAYBACK)

        if not media_id:
            return

        try:
            failed = self.getPlayingFile().split("|")[0]
            self._tried_urls.add(failed)
        except Exception:
            pass
        if self._current_url:
            self._tried_urls.add(self._current_url)

        _log(
            f"Playback error for {media_id}; keeping cached source for URL refresh",
            xbmc.LOGWARNING,
        )
        self._clear_failed_stream(
            media_id,
            "Kodi could not open this stream. KDMM kept the cached source; retry playback to refresh the link.",
            failed_candidate=self._current_candidate,
            failed_url=self._current_url,
        )

    def _handle_playback_stop(self, is_ended):
        media_id = self._current_media_id
        if not media_id:
            self._check_failed_start_soon()
            return

        playback_context = dict(self._playback_context or {})
        total_time = self._last_known_total
        was_fullscreen = self._was_fullscreen
        self._was_fullscreen = False
        next_episode = None
        if is_ended and total_time > 0 and total_time - self._last_known_time > ENDED_MARGIN_SECONDS:
            _log(
                f"Stream ended at {self._last_known_time:.0f}s of {total_time:.0f}s for {media_id}; "
                "treating it as stopped, not watched",
                xbmc.LOGWARNING,
            )
            is_ended = False
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
                "– treating as failed stream",
                xbmc.LOGWARNING,
            )
            self._current_media_id = None
            if self._current_url:
                self._tried_urls.add(self._current_url)
            self._clear_failed_stream(
                media_id,
                "Stream opened but ended immediately. KDMM kept the cached source; retry playback to refresh the link.",
                failed_candidate=self._current_candidate,
                failed_url=self._current_url,
            )
        else:
            # A play request that resolved while this item was still playing
            # has already written its own window properties; they belong to
            # the new item and must survive this (late) stop callback.
            new_playback_pending = bool(WIN.getProperty(PROP_PENDING_PLAYBACK))
            self._cleanup_current_candidate(include_window=not new_playback_pending)
            outcome = self._save_progress(is_ended=is_ended, clear_window=not new_playback_pending)
            if self.upnext and outcome:
                known_next = next_episode
                self.upnext.on_progress_saved(
                    playback_context, outcome,
                    lambda context: known_next or next_episode_from_tmdb(context),
                )
            if next_episode:
                self._open_next_episode(next_episode)
            elif was_fullscreen and _setting_bool("return_to_details", True):
                self._return_to_details(playback_context)

    def _check_failed_start_soon(self):
        """Kodi stopped without ever starting the stream KDMM resolved.

        When Kodi cannot open a resolved URL it reports a plain stop, not an
        error. Without this the user waits out the 30-second start timeout
        before KDMM says anything.
        """
        pending_json = WIN.getProperty(PROP_PENDING_PLAYBACK)
        if not pending_json or pending_json == self._failed_start_checking:
            return
        self._failed_start_checking = pending_json

        def _check():
            try:
                _check_once()
            finally:
                self._failed_start_checking = None

        def _check_once():
            if xbmc.Monitor().waitForAbort(FAILED_START_CHECK_SECONDS):
                return
            if self.isPlaying() or self._current_media_id:
                return
            # Unchanged means no new play request replaced it meanwhile.
            if WIN.getProperty(PROP_PENDING_PLAYBACK) != pending_json:
                return
            try:
                pending = json.loads(pending_json)
            except Exception:
                WIN.clearProperty(PROP_PENDING_PLAYBACK)
                return
            media_id = pending.get("media_id")
            url = (pending.get("url") or "").split("|")[0]
            if not media_id:
                return
            if url:
                self._tried_urls.add(url)
            transition.clear()
            _log(f"Kodi could not open the stream for {media_id}", xbmc.LOGWARNING)
            self._clear_failed_stream(
                media_id,
                "Kodi could not open this stream. KDMM kept the cached source; retry playback to refresh the link.",
                failed_candidate=self._candidate_for_url(url),
                failed_url=url,
            )

        threading.Thread(target=_check, daemon=True).start()

    def switch_to_episode(self, next_episode, monitor):
        """Stop the current stream completely, then open the next episode.

        The lookup for the next episode must never run under live playback:
        the old stream's stop callback would land after the new item's state
        was written, and on Amlogic boxes two streams would overlap.
        """
        transition.begin()
        if self.isPlaying():
            self._stop_and_wait(monitor)
        self._join_cleanups(CLEANUP_JOIN_TIMEOUT_SECONDS)
        if monitor.abortRequested():
            transition.clear()
            return False
        if play_next_episode(next_episode):
            return True
        transition.clear()
        _log("Next-episode playback could not be opened", xbmc.LOGWARNING)
        return False

    def _stop_and_wait(self, monitor):
        _log("Stopping playback before looking up the next episode")
        try:
            self.stop()
        except Exception as exc:
            _log(f"stop() failed: {exc}", xbmc.LOGWARNING)
        deadline = time.time() + STOP_BEFORE_NEXT_TIMEOUT_SECONDS
        while time.time() < deadline:
            # waitForAbort also lets Kodi deliver the stop callback, which
            # saves progress and clears _current_media_id.
            if monitor.waitForAbort(0.1):
                return
            if not self.isPlaying() and not self._current_media_id:
                return
        if self.isPlaying():
            _log("Playback is still running after stop(); continuing anyway", xbmc.LOGWARNING)
        elif self._current_media_id:
            _log("Stop callback did not arrive; saving progress directly", xbmc.LOGWARNING)
            self._handle_playback_stop(is_ended=False)

    def _join_cleanups(self, timeout):
        deadline = time.time() + timeout
        for thread in self._cleanup_threads:
            thread.join(max(0.0, deadline - time.time()))
        self._cleanup_threads = [t for t in self._cleanup_threads if t.is_alive()]

    def _return_to_details(self, context):
        def _open():
            if not self._wait_for_menu():
                return
            if not context.get("is_movie") and xbmc.getCondVisibility(EPISODE_LIST_VISIBLE):
                _log("Back on the episode list; not opening the title page over it")
                return
            open_details(context)

        threading.Thread(target=_open, daemon=True).start()

    def _wait_for_menu(self):
        """Wait for Kodi to leave fullscreen video.

        Returns False when something else started playing or a KDMM play
        request is in flight, so the title page never covers new playback.
        """
        monitor = xbmc.Monitor()
        deadline = time.time() + RETURN_TO_DETAILS_WAIT_SECONDS
        while time.time() < deadline:
            if (
                self.isPlayingVideo()
                or transition.active()
                or WIN.getProperty(PROP_PENDING_PLAYBACK)
                or WIN.getProperty(PROP_MEDIA_ID)
            ):
                return False
            if not xbmc.getCondVisibility(
                "Window.IsActive(fullscreenvideo) | Window.IsActive(busydialog) "
                "| Window.IsActive(busydialognocancel)"
            ):
                return True
            if monitor.waitForAbort(0.1):
                return False
        _log("Still in fullscreen video; not opening the title page", xbmc.LOGWARNING)
        return False

    def _open_next_episode(self, next_episode):
        def _open():
            xbmc.sleep(800)
            self._join_cleanups(CLEANUP_JOIN_TIMEOUT_SECONDS)
            _log(
                f"Auto-playing next episode S{next_episode['season']}E{next_episode['episode']}"
            )
            transition.begin()
            if not play_next_episode(next_episode):
                transition.clear()
                _log("Automatic next-episode playback failed", xbmc.LOGWARNING)

        threading.Thread(target=_open, daemon=True).start()

    def _load_candidates(self):
        candidates_json = WIN.getProperty(PROP_CANDIDATES)
        if not candidates_json:
            return []
        try:
            candidates = json.loads(candidates_json)
        except Exception:
            return []
        return candidates if isinstance(candidates, list) else []

    def _candidate_for_url(self, url):
        url = (url or "").split("|")[0]
        if not url:
            return None
        for candidate in self._load_candidates():
            candidate_url = (candidate.get("url") or "").split("|")[0]
            if candidate_url == url:
                return candidate
        return None

    def _candidate_has_provider_state(self, candidate):
        if not isinstance(candidate, dict):
            return False
        return (
            candidate.get("provider") == "alldebrid"
            and bool(candidate.get("provider_item_id") or candidate.get("ad_magnet_id"))
        )

    def _cleanup_candidate(self, candidate):
        if not self._candidate_has_provider_state(candidate):
            return

        def _cleanup():
            cleanup_stream_candidate(candidate)

        thread = threading.Thread(target=_cleanup, daemon=True)
        thread.start()
        self._cleanup_threads = [t for t in self._cleanup_threads if t.is_alive()]
        self._cleanup_threads.append(thread)

    def _cleanup_candidates(self, candidates):
        seen = set()
        for candidate in candidates or []:
            if not isinstance(candidate, dict):
                continue
            key = (
                candidate.get("provider"),
                str(candidate.get("provider_item_id") or candidate.get("ad_magnet_id") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            self._cleanup_candidate(candidate)

    def _cleanup_current_candidate(self, include_window=True):
        candidates = []
        if self._current_candidate:
            candidates.append(self._current_candidate)
        if include_window:
            candidates.extend(self._load_candidates())
        else:
            # The window list now describes the next item. Keep any provider
            # state it shares with the item that just stopped (a season pack
            # backs both episodes with the same AllDebrid magnet).
            keep = {self._provider_state_key(c) for c in self._load_candidates()}
            candidates = [c for c in candidates if self._provider_state_key(c) not in keep]
        self._current_candidate = None
        self._cleanup_candidates(candidates)
        if include_window:
            WIN.clearProperty(PROP_CANDIDATES)

    @staticmethod
    def _provider_state_key(candidate):
        if not isinstance(candidate, dict):
            return None
        return (
            candidate.get("provider"),
            str(candidate.get("provider_item_id") or candidate.get("ad_magnet_id") or ""),
        )

    def _clear_failed_stream(self, media_id, message="Playback failed",
                             failed_candidate=None, failed_url=""):
        self._cleanup_candidate(failed_candidate)
        WIN.clearProperty(PROP_CANDIDATES)
        WIN.clearProperty(PROP_PLAYBACK_CONTEXT)
        WIN.clearProperty(PROP_PENDING_PLAYBACK)
        WIN.clearProperty(PROP_MEDIA_ID)
        WIN.clearProperty(PROP_RESUME_TIME)
        self._playback_context = {}
        self._current_candidate = None
        xbmcgui.Dialog().notification(
            "KDMM",
            describe_playback_failure(message),
            xbmcgui.NOTIFICATION_ERROR,
            8000,
        )

    def _save_progress(self, is_ended, clear_window=True):
        media_id = self._current_media_id
        if not media_id:
            return None
        self._current_media_id = None
        if clear_window:
            WIN.clearProperty(PROP_PENDING_PLAYBACK)
            WIN.clearProperty(PROP_PLAYBACK_CONTEXT)
            WIN.clearProperty(PROP_CANDIDATES)
        self._playback_context = {}

        current_time = self._last_known_time
        total_time = self._last_known_total

        if total_time <= 0 or current_time <= 0:
            _log(f"No valid time data for {media_id} – progress not saved", xbmc.LOGWARNING)
            return None

        near_end = (total_time - current_time) < WATCHED_MARGIN_SECONDS
        if is_ended or near_end:
            _log(f"Marking {media_id} as watched")
            self._progress_cache.set_progress(
                media_id, 0.0, total_time=total_time, watched=True
            )
            return "watched"
        if current_time > 5.0:
            _log(f"Saving resume position {current_time:.1f}s / {total_time:.1f}s for {media_id}")
            self._progress_cache.set_progress(
                media_id, current_time, total_time=total_time, watched=False
            )
            return "resume"
        return None


# ------------------------------------------------------------------ #
# Service entry point
# ------------------------------------------------------------------ #

class BridgeMonitor(xbmc.Monitor):
    def __init__(self):
        super().__init__()
        self.player = BridgePlayer()
        self.segment_controller = SegmentController()
        self.seek = SeekAccumulator()
        self.prewarm = PrewarmController(_USERDATA_PATH)
        self.next_warm = NextEpisodeWarmer(_USERDATA_PATH)
        self.upnext = UpNextMemory(_USERDATA_PATH)
        self._step_failures = {}
        self.player.upnext = self.upnext

    def onNotification(self, sender, method, data):  # noqa: N802
        # Sent by NotifyAll(plugin.video.kdmm,<action>) in kdmm_seek.xml.
        # upnext_<tmdb id> comes from the skin when a show's page opens.
        if sender == _ADDON_ID and method.startswith("Other."):
            action = method[len("Other."):]
            if not self.upnext.handle_notification(action):
                self.seek.press(action)

    def onSettingsChanged(self):  # noqa: N802
        self._install_keymaps()
        self.prewarm.refresh_settings()

    def _log_step_failure(self, name, exc):
        # A step that fails every half second would flood the log.
        now = time.time()
        if now - self._step_failures.get(name, 0) >= 60:
            self._step_failures[name] = now
            _log(f"Service step '{name}' failed: {exc!r}", xbmc.LOGERROR)

    def _install_keymaps(self):
        install_keymaps(
            _ADDON_PATH,
            remote_enabled=_setting_bool("remote_keymap", True),
            seek_enabled=_setting_bool("seek_on_release", True),
        )

    def run(self):
        _log("Service started")
        try:
            _install_player_json()
        except Exception as exc:
            _log(f"_install_player_json failed: {exc}", xbmc.LOGWARNING)
        try:
            claim_default_player_once(_USERDATA_PATH)
        except Exception as exc:
            _log(f"claim_default_player_once failed: {exc}", xbmc.LOGWARNING)
        startup = [
            ("cache duration", lambda: get_stream_cache_ttl_hours(_USERDATA_PATH)),
            ("legacy add-on", disable_legacy_addon),
            ("settings migration", migrate_choice_settings),
            ("keymaps", self._install_keymaps),
        ]
        if _setting_bool("tune_bingie_helper", True):
            startup.append(("device tuning", apply_device_tuning))
        for name, step in startup:
            try:
                step()
            except Exception as exc:
                _log(f"Startup step '{name}' failed: {exc!r}", xbmc.LOGWARNING)
        steps = (
            ("player", self.player.tick),
            ("segments", lambda: self.segment_controller.tick(self.player, self)),
            ("seek", lambda: self.seek.tick(self.player)),
            ("prewarm", lambda: self.prewarm.tick(self.player)),
            ("next warm", lambda: self.next_warm.tick(self.player)),
        )
        while not self.abortRequested():
            for name, step in steps:
                # One failing step must not end the service for the session.
                try:
                    step()
                except Exception as exc:
                    self._log_step_failure(name, exc)
            self.waitForAbort(0.5)
        self.prewarm.stop()
        self.next_warm.stop()
        self.upnext.stop()
        _log("Service stopped")


if __name__ == "__main__":
    monitor = BridgeMonitor()
    monitor.run()
