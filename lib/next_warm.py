"""Find the next episode's stream while the current episode plays.

Pressing "Play Next Episode" otherwise starts a full lookup: on a low-power
box that is 20-30 seconds of spinner. A couple of minutes into an episode
the service resolves the following one in a background thread and stores it
in the normal stream cache, so the switch is a cache hit.
"""

import threading

import xbmc
import xbmcaddon
import xbmcgui

from dmm import cleanup_stream_candidate, fetch_all_cached_streams
from next_episode import next_episode_from_tmdb
from prewarm import DEFAULT_STREAM_CACHE_TTL_HOURS, _cached_is_fresh, _episode_target, _playable
from cache import StreamCache

SETTING_ID = "warm_next_episode"
PROP_PENDING_PLAYBACK = "kdmm.pending_playback"
WARM_AFTER_SECONDS = 120

_WIN = xbmcgui.Window(10000)


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Next Warm] {msg}", level)


def _enabled():
    try:
        value = xbmcaddon.Addon().getSetting(SETTING_ID)
    except Exception:
        return True
    return value.lower() != "false"


def provider_state_key(candidate):
    if not isinstance(candidate, dict):
        return None
    item_id = candidate.get("provider_item_id") or candidate.get("ad_magnet_id")
    if not candidate.get("provider") or not item_id:
        return None
    return candidate.get("provider"), str(item_id)


class NextEpisodeWarmer:
    def __init__(self, userdata_path):
        self._userdata_path = userdata_path
        self._started_for = None
        self._worker = None
        self._cancel = threading.Event()

    def stop(self):
        self._cancel.set()
        worker = self._worker
        if worker and worker.is_alive():
            worker.join(2.0)

    def tick(self, player):
        context = getattr(player, "_playback_context", None) or {}
        media_id = context.get("media_id")
        playing = bool(media_id) and self._is_playing(player)
        if not playing or _WIN.getProperty(PROP_PENDING_PLAYBACK):
            # Playback ended or another title is starting: a lookup still in
            # flight must not compete with, or clean up under, the play path.
            self._cancel.set()
            if not playing:
                # Watching the same episode again later may look ahead again.
                self._started_for = None
            return
        if context.get("is_movie") or media_id == self._started_for:
            return
        if getattr(player, "_last_known_time", 0) < WARM_AFTER_SECONDS:
            return
        worker = self._worker
        if worker and worker.is_alive():
            return
        self._started_for = media_id
        if not _enabled():
            return
        self._cancel = threading.Event()
        self._worker = threading.Thread(
            target=self._warm,
            args=(dict(context), getattr(player, "_current_candidate", None), self._cancel),
            daemon=True,
        )
        self._worker.start()

    @staticmethod
    def _is_playing(player):
        try:
            return bool(player.isPlayingVideo())
        except Exception:
            return False

    def _stream_cache(self):
        try:
            hours = int(xbmcaddon.Addon().getSetting("stream_cache_ttl_hours") or "0")
        except Exception:
            hours = 0
        if hours <= 0:
            hours = DEFAULT_STREAM_CACHE_TTL_HOURS
        return StreamCache(self._userdata_path, ttl=hours * 3600)

    def _warm(self, context, playing_candidate, cancel):
        try:
            self._warm_next(context, playing_candidate, cancel)
        except PermissionError as exc:
            _log(f"Debrid authorization problem ({exc}); next episode not looked up", xbmc.LOGWARNING)
        except Exception as exc:
            _log(f"Next-episode lookup failed: {exc}", xbmc.LOGWARNING)

    def _warm_next(self, context, playing_candidate, cancel):
        imdb = str(context.get("imdb_id") or "").strip()
        if not imdb:
            return False
        upcoming = next_episode_from_tmdb(context)
        if not upcoming or cancel.is_set():
            return False
        target = _episode_target(
            imdb, int(upcoming["season"]), int(upcoming["episode"]),
            context.get("showtitle") or "", upcoming.get("title") or "",
        )
        media_id = target["media_id"]
        if _cached_is_fresh(self._stream_cache(), media_id):
            return False

        _log(f"Looking up {target['label'] or media_id} while the current episode plays")
        candidates = fetch_all_cached_streams(
            "series", media_id, cancel_event=cancel,
            query_title=target["query_title"], year=None,
            userdata_path=self._userdata_path,
            episode_title=target["episode_title"], quiet=True,
        )
        candidates = [c for c in candidates or [] if isinstance(c, dict)]
        if cancel.is_set():
            # A play request took over. It may be using the same AllDebrid
            # magnet, so leave the provider state alone and save nothing.
            return False

        # Never release the magnet behind the episode that is playing: a
        # season pack backs this episode and the next with the same one.
        keep = provider_state_key(playing_candidate)
        if keep is None and (playing_candidate is None or playing_candidate.get("provider") == "alldebrid"):
            # Which magnet backs the playing episode is unknown, so none can
            # be released safely. The stop handler and the next play clean up.
            _log("Playing source unknown; leaving provider state in place")
        else:
            for candidate in candidates:
                if provider_state_key(candidate) != keep:
                    cleanup_stream_candidate(candidate)

        playable = _playable(candidates, target)
        if not playable:
            _log(f"No playable stream found ahead of time for {media_id}")
            return False
        self._stream_cache().set(media_id, [playable[0]], label=target["label"], prewarmed=True)
        _log(f"Saved {playable[0].get('name', 'stream')!r} for {media_id}")
        return True
