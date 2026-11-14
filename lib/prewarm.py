"""Find streams ahead of time for the titles on the skin's widgets.

While the user browses, the service remembers which movies, shows and
episodes the skin's widget rows are showing (home screen and provider hubs
alike: whichever window the user rests on). Once Kodi has been idle for ten
minutes and nothing is playing, a background thread looks up the best stream
for each of them and stores it in the normal stream cache, so pressing Play
later is a cache hit. Any remote press or playback start cancels the run.

Off unless the "prewarm_widgets" setting is on.
"""

import os
import re
import threading
import time

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

from cache import ProgressCache, StreamCache
from dmm import (
    candidate_matches_audio_preference,
    candidate_matches_episode_identity,
    cleanup_stream_candidate,
    fetch_all_cached_streams,
    is_av1_stream,
)
from next_episode import next_episode_from_tmdb

SETTING_ID = "prewarm_widgets"
PROP_PENDING_PLAYBACK = "kdmm.pending_playback"

IDLE_SECONDS = 600
SNAPSHOT_MIN_IDLE_SECONDS = 10
SNAPSHOT_INTERVAL_SECONDS = 30
CHECK_INTERVAL_SECONDS = 5

MAX_ITEMS_PER_SESSION = 400
MAX_ITEMS_PER_WIDGET = 150
MAX_REMEMBERED_ITEMS = 1000
MAX_WIDGET_IDS = 250
PAUSE_BETWEEN_ITEMS_SECONDS = 3
# Only for Real-Debrid entries saved without a hoster link (KDMM 2.7.6):
# look them up again once they are this old, before the play path stops
# trusting them (cache.PREWARM_MAX_AGE_SECONDS). Newer entries never age out.
REWARM_AFTER_SECONDS = 20 * 3600
FAILURE_RETRY_SECONDS = 24 * 3600
DEFAULT_STREAM_CACHE_TTL_HOURS = 336

_WIDGET_ID_PATTERN = re.compile(r'name="widgetid"\s+value="(\d+)"', re.IGNORECASE)
# Deflix's home rows and spotlight, used when the skin's files can't be read.
_FALLBACK_WIDGET_IDS = tuple(range(23003, 23009)) + (1508,)
_MAX_SKIN_XML_BYTES = 2 * 1024 * 1024

_WIN = xbmcgui.Window(10000)
_widget_ids_by_skin = {}


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Prewarm] {msg}", level)


def _to_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError, AttributeError):
        return None


def _is_imdb_id(value):
    return bool(value) and value.startswith("tt") and value[2:].isdigit()


# ------------------------------------------------------------------ #
# Reading the skin's widgets
# ------------------------------------------------------------------ #

def widget_ids():
    """Container ids of the current skin's widgets, read from its XML once."""
    skin_path = xbmcvfs.translatePath("special://skin/")
    if skin_path in _widget_ids_by_skin:
        return _widget_ids_by_skin[skin_path]

    found = set()
    try:
        for folder in sorted(os.listdir(skin_path)):
            folder_path = os.path.join(skin_path, folder)
            if not os.path.isfile(os.path.join(folder_path, "Home.xml")):
                continue
            for name in os.listdir(folder_path):
                if not name.lower().endswith(".xml"):
                    continue
                file_path = os.path.join(folder_path, name)
                if os.path.getsize(file_path) > _MAX_SKIN_XML_BYTES:
                    continue
                with open(file_path, "r", encoding="utf-8", errors="ignore") as fh:
                    found.update(int(m) for m in _WIDGET_ID_PATTERN.findall(fh.read()))
    except Exception as exc:
        _log(f"Could not read widget ids from the skin: {exc}", xbmc.LOGWARNING)

    ids = tuple(sorted(found)[:MAX_WIDGET_IDS]) or _FALLBACK_WIDGET_IDS
    _widget_ids_by_skin[skin_path] = ids
    _log(f"Watching {len(ids)} widget container(s)")
    return ids


def _widget_counts(ids):
    counts = []
    for container_id in ids:
        count = _to_int(xbmc.getInfoLabel(f"Container({container_id}).NumItems")) or 0
        if count > 0:
            counts.append((container_id, count))
    return tuple(counts)


def _read_item(container_id, position):
    base = f"Container({container_id}).ListItemAbsolute({position})."

    def label(name):
        return (xbmc.getInfoLabel(base + name) or "").strip()

    db_type = label("DBType").lower()
    tmdb_type = label("Property(tmdb_type)").lower()
    title = label("Title") or label("Label")

    if db_type == "movie" or (not db_type and tmdb_type == "movie"):
        imdb = label("Property(imdb_id)")
        if not _is_imdb_id(imdb):
            imdb = label("IMDBNumber")
        if not _is_imdb_id(imdb):
            return None
        return {
            "key": imdb, "kind": "movie", "imdb": imdb,
            "title": title, "year": label("Year"), "position": position,
        }

    if db_type == "episode":
        # KDMM plays episodes by the show's IMDb id, never the episode's own.
        imdb = label("Property(tvshow.imdb_id)")
        season = _to_int(label("Season"))
        episode = _to_int(label("Episode"))
        if not _is_imdb_id(imdb) or not season or not episode:
            return None
        return {
            "key": f"{imdb}:{season}:{episode}", "kind": "episode", "imdb": imdb,
            "season": season, "episode": episode, "title": title,
            "showtitle": label("TVShowTitle"), "position": position,
        }

    if db_type == "tvshow" or tmdb_type == "tv":
        imdb = label("Property(imdb_id)")
        if not _is_imdb_id(imdb):
            return None
        return {
            "key": f"show:{imdb}", "kind": "show", "imdb": imdb,
            "tmdb": label("Property(tmdb_id)"), "showtitle": title,
            "position": position,
        }

    return None


def scan_widgets(counts):
    items = []
    for container_id, count in counts:
        for position in range(min(count, MAX_ITEMS_PER_WIDGET)):
            item = _read_item(container_id, position)
            if item:
                items.append(item)
    return items


# ------------------------------------------------------------------ #
# Turning a widget item into something to look up
# ------------------------------------------------------------------ #

def _episode_label(showtitle, season, episode):
    return f"{showtitle} S{season:02d}E{episode:02d}" if showtitle else ""


def _episode_target(imdb, season, episode, showtitle, title):
    media_id = f"{imdb}:{season}:{episode}"
    return {
        "catalog_type": "series", "media_id": media_id,
        "query_title": showtitle, "year": None,
        "season": str(season), "episode": str(episode),
        "episode_title": title or None,
        "label": _episode_label(showtitle, season, episode),
    }


def _show_target(item, progress, next_cache):
    """The episode the user would play next, for a show started in KDMM."""
    episodes = progress.episodes_for(item["imdb"])
    if not episodes:
        return None
    season, episode, entry = max(episodes, key=lambda found: found[2].get("updated", 0))

    if not entry.get("watched"):
        if float(entry.get("resume_time") or 0) <= 5.0:
            return None
        return _episode_target(item["imdb"], season, episode, item["showtitle"], "")

    lookup_key = (item["imdb"], season, episode)
    if lookup_key not in next_cache:
        next_cache[lookup_key] = next_episode_from_tmdb({
            "imdb_id": item["imdb"], "tmdb_id": item.get("tmdb"),
            "season": season, "episode": episode, "showtitle": item["showtitle"],
        })
    upcoming = next_cache[lookup_key]
    if not upcoming:
        return None
    return _episode_target(
        item["imdb"], upcoming["season"], upcoming["episode"],
        item["showtitle"], upcoming.get("title") or "",
    )


def build_target(item, progress, next_cache):
    if item["kind"] == "movie":
        if progress.is_watched(item["imdb"]):
            return None
        title, year = item.get("title") or "", item.get("year") or ""
        return {
            "catalog_type": "movie", "media_id": item["imdb"],
            "query_title": title, "year": year or None,
            "season": None, "episode": None, "episode_title": None,
            "label": f"{title} ({year})" if title and year else title,
        }
    if item["kind"] == "episode":
        target = _episode_target(
            item["imdb"], item["season"], item["episode"],
            item.get("showtitle") or "", item.get("title") or "",
        )
        return None if progress.is_watched(target["media_id"]) else target
    return _show_target(item, progress, next_cache)


def _playable(candidates, target):
    return [
        candidate for candidate in candidates or []
        if isinstance(candidate, dict)
        and candidate.get("url")
        and not is_av1_stream(candidate)
        and candidate_matches_audio_preference(candidate)
        and candidate_matches_episode_identity(
            candidate,
            season=target["season"],
            episode=target["episode"],
            episode_title=target["episode_title"],
        )
    ]


def _cached_is_fresh(stream_cache, media_id):
    cached = stream_cache.get(media_id)
    if not cached:
        return False
    age = stream_cache.prewarm_age(media_id)
    if age is None or age < REWARM_AFTER_SECONDS:
        return True
    # A link that can be rebuilt at play time never goes stale: AllDebrid by
    # torrent hash, Real-Debrid by its stored hoster link.
    first = cached[0] if isinstance(cached, list) and cached else cached
    return isinstance(first, dict) and (
        first.get("provider") == "alldebrid" or bool(first.get("rd_link"))
    )


# ------------------------------------------------------------------ #
# Controller, driven by the service loop
# ------------------------------------------------------------------ #

class PrewarmController:
    def __init__(self, userdata_path):
        self._userdata_path = userdata_path
        self._enabled = False
        self._items = {}
        self._last_signature = None
        self._last_check = 0.0
        self._last_snapshot = 0.0
        self._session_done = False
        self._worker = None
        self._cancel = threading.Event()
        self._failures = {}
        self._next_cache = {}
        self.refresh_settings()

    def refresh_settings(self):
        try:
            enabled = xbmcaddon.Addon().getSetting(SETTING_ID).lower() == "true"
        except Exception:
            enabled = False
        if enabled != self._enabled:
            _log("Idle stream lookup " + ("enabled" if enabled else "disabled"))
        self._enabled = enabled
        if not enabled:
            self._cancel.set()
            self._items.clear()
            self._last_signature = None

    def stop(self):
        self._cancel.set()
        worker = self._worker
        if worker and worker.is_alive():
            worker.join(2.0)

    def tick(self, player):
        worker = self._worker
        if worker and worker.is_alive():
            if self._user_is_back(player):
                self._cancel.set()
                self._session_done = False
            return
        if not self._enabled:
            return

        now = time.time()
        if now - self._last_check < CHECK_INTERVAL_SECONDS:
            return
        self._last_check = now

        idle = xbmc.getGlobalIdleTime()
        if idle < SNAPSHOT_MIN_IDLE_SECONDS:
            self._session_done = False
            return
        if self._busy(player):
            return
        if idle < IDLE_SECONDS:
            if now - self._last_snapshot >= SNAPSHOT_INTERVAL_SECONDS:
                self._last_snapshot = now
                self._snapshot()
            return
        if self._session_done:
            return

        self._session_done = True
        self._snapshot()
        items = sorted(self._items.values(), key=lambda item: item["position"])
        if not items:
            _log("Kodi is idle, but no widget titles have been seen yet")
            return
        self._cancel = threading.Event()
        self._worker = threading.Thread(
            target=self._run, args=(items, self._cancel), daemon=True,
        )
        self._worker.start()

    @staticmethod
    def _busy(player):
        try:
            if player.isPlaying():
                return True
        except Exception:
            pass
        return bool(_WIN.getProperty(PROP_PENDING_PLAYBACK))

    def _user_is_back(self, player):
        return (
            not self._enabled
            or self._busy(player)
            or xbmc.getGlobalIdleTime() < IDLE_SECONDS
        )

    def _snapshot(self):
        """Remember what the widgets on the current window are showing."""
        try:
            counts = _widget_counts(widget_ids())
            if not counts:
                return
            signature = (xbmcgui.getCurrentWindowId(), counts)
            if signature == self._last_signature:
                return
            self._last_signature = signature
            for item in scan_widgets(counts):
                self._items.pop(item["key"], None)
                self._items[item["key"]] = item
            while len(self._items) > MAX_REMEMBERED_ITEMS:
                self._items.pop(next(iter(self._items)))
        except Exception as exc:
            _log(f"Widget scan failed: {exc}", xbmc.LOGWARNING)

    def _stream_cache(self):
        try:
            hours = int(xbmcaddon.Addon().getSetting("stream_cache_ttl_hours") or "0")
        except Exception:
            hours = 0
        if hours <= 0:
            hours = DEFAULT_STREAM_CACHE_TTL_HOURS
        return StreamCache(self._userdata_path, ttl=hours * 3600)

    def _run(self, items, cancel):
        _log(f"Kodi is idle; checking {len(items)} widget title(s)")
        found = failed = 0
        try:
            progress = ProgressCache(self._userdata_path)
            for item in items:
                if cancel.is_set() or found + failed >= MAX_ITEMS_PER_SESSION:
                    break
                outcome = self._prewarm_item(item, progress, cancel)
                if outcome is None:
                    continue
                if outcome == "stop":
                    break
                if outcome:
                    found += 1
                else:
                    failed += 1
                if cancel.wait(PAUSE_BETWEEN_ITEMS_SECONDS):
                    break
        except Exception as exc:
            _log(f"Idle stream lookup stopped on an error: {exc}", xbmc.LOGWARNING)
        state = "interrupted" if cancel.is_set() else "finished"
        _log(f"Idle stream lookup {state}: {found} stream(s) saved, {failed} not found")

    def _prewarm_item(self, item, progress, cancel):
        """True: saved. False: nothing playable. None: skipped. "stop": end the run."""
        target = build_target(item, progress, self._next_cache)
        if not target:
            return None
        media_id = target["media_id"]
        if time.time() - self._failures.get(media_id, 0) < FAILURE_RETRY_SECONDS:
            return None
        if _cached_is_fresh(self._stream_cache(), media_id):
            return None

        _log(f"Looking up {target['label'] or media_id}")
        try:
            candidates = fetch_all_cached_streams(
                target["catalog_type"], media_id, cancel_event=cancel,
                query_title=target["query_title"], year=target["year"],
                userdata_path=self._userdata_path,
                episode_title=target["episode_title"], quiet=True,
            )
        except PermissionError as exc:
            _log(f"Debrid authorization problem ({exc}); stopping", xbmc.LOGWARNING)
            return "stop"
        except Exception as exc:
            _log(f"Lookup failed for {media_id}: {exc}", xbmc.LOGWARNING)
            self._failures[media_id] = time.time()
            return False

        candidates = [c for c in candidates or [] if isinstance(c, dict)]
        # Resolving leaves an AllDebrid magnet behind for each stream. Nothing
        # is about to play, so release them all; the play path uploads again.
        for candidate in candidates:
            cleanup_stream_candidate(candidate)
        if cancel.is_set():
            return "stop"

        playable = _playable(candidates, target)
        if not playable:
            self._failures[media_id] = time.time()
            return False
        self._stream_cache().set(
            media_id, [playable[0]], label=target["label"], prewarmed=True,
        )
        _log(f"Saved {playable[0].get('name', 'stream')!r} for {media_id}")
        return True
