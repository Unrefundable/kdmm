"""Show a title page's "Play Season X: Episode Y" label without the wait.

The skin fills that button from TMDb Bingie Helper's Trakt up-next lookup,
which runs a Python plugin each time a show's page opens: 3-5 seconds of
"Loading..." on a low-power box. KDMM remembers the answer per show and
hands it to the skin through window properties the moment the page opens.
The helper's list still loads behind it and replaces the remembered answer
when it arrives, so Trakt stays the authority.

What is remembered comes from two places: the helper's own answer the last
time the page was open, and KDMM's playback (an episode left half-way is the
one to resume; a finished one makes the following episode next).

The skin asks by sending NotifyAll(plugin.video.kdmm,upnext_<tmdb id>) from
the page's onload.
"""

import json
import os
import threading
import time

import xbmc
import xbmcgui

NOTIFY_PREFIX = "upnext_"
PROP_TMDB = "kdmm.upnext.tmdb"
PROP_SEASON = "kdmm.upnext.season"
PROP_EPISODE = "kdmm.upnext.episode"
PROP_RESUMABLE = "kdmm.upnext.resumable"
PROP_PATH = "kdmm.upnext.path"
_PROPS = (PROP_TMDB, PROP_SEASON, PROP_EPISODE, PROP_RESUMABLE, PROP_PATH)

MAX_SHOWS = 500
LEARN_TIMEOUT_SECONDS = 20
LEARN_POLL_SECONDS = 0.25
HELPER_LIST_ID = 1234

_WIN = xbmcgui.Window(10000)


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM UpNext] {msg}", level)


def _to_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def play_path(tmdb_id, season, episode):
    """The helper's play route, as its own up-next list builds it."""
    return (
        "plugin://plugin.video.tmdb.bingie.helper/?info=play&tmdb_type=tv"
        f"&tmdb_id={tmdb_id}&season={season}&episode={episode}"
    )


class UpNextMemory:
    def __init__(self, userdata_path):
        self._path = os.path.join(userdata_path, "upnext.json")
        self._lock = threading.Lock()
        self._shows = self._load()
        self._requested = None
        self._learner_cancel = threading.Event()

    # ---------------------------------------------------------------- #
    # Storage
    # ---------------------------------------------------------------- #
    def _load(self):
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save(self):
        """Caller holds the lock."""
        try:
            os.makedirs(os.path.dirname(self._path), exist_ok=True)
            tmp = self._path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self._shows, fh)
            os.replace(tmp, self._path)
        except Exception as exc:
            _log(f"Could not save up-next memory: {exc}", xbmc.LOGWARNING)

    def get(self, tmdb_id):
        with self._lock:
            entry = self._shows.get(str(tmdb_id))
            return dict(entry) if entry else None

    def remember(self, tmdb_id, season, episode, resumable=False, path=""):
        tmdb_id = str(tmdb_id or "").strip()
        season, episode = _to_int(season), _to_int(episode)
        if not tmdb_id.isdigit() or season is None or not episode:
            return False
        entry = {
            "season": season,
            "episode": episode,
            "resumable": bool(resumable),
            "path": path or play_path(tmdb_id, season, episode),
        }
        with self._lock:
            previous = self._shows.get(tmdb_id) or {}
            if all(previous.get(key) == value for key, value in entry.items()):
                return False
            entry["updated"] = int(time.time())
            self._shows.pop(tmdb_id, None)
            self._shows[tmdb_id] = entry
            while len(self._shows) > MAX_SHOWS:
                oldest = min(self._shows, key=lambda key: self._shows[key].get("updated", 0))
                del self._shows[oldest]
            self._save()
        self._republish(tmdb_id)
        return True

    def forget(self, tmdb_id):
        tmdb_id = str(tmdb_id or "").strip()
        with self._lock:
            if self._shows.pop(tmdb_id, None) is None:
                return False
            self._save()
        self._republish(tmdb_id)
        return True

    # ---------------------------------------------------------------- #
    # Window properties read by the skin
    # ---------------------------------------------------------------- #
    def publish(self, tmdb_id):
        entry = self.get(tmdb_id)
        if not entry:
            for prop in _PROPS:
                _WIN.clearProperty(prop)
            return False
        # The id goes last: the skin only trusts the other values once the
        # id matches the page it is showing.
        _WIN.setProperty(PROP_SEASON, str(entry["season"]))
        _WIN.setProperty(PROP_EPISODE, str(entry["episode"]))
        _WIN.setProperty(PROP_RESUMABLE, "1" if entry.get("resumable") else "")
        _WIN.setProperty(PROP_PATH, entry.get("path") or "")
        _WIN.setProperty(PROP_TMDB, str(tmdb_id))
        return True

    def _republish(self, tmdb_id):
        if self._requested == tmdb_id:
            self.publish(tmdb_id)

    # ---------------------------------------------------------------- #
    # Title page opened
    # ---------------------------------------------------------------- #
    def handle_notification(self, action):
        """True when the notification was an up-next request."""
        if not action.startswith(NOTIFY_PREFIX):
            return False
        tmdb_id = action[len(NOTIFY_PREFIX):].strip()
        if not tmdb_id.isdigit():
            return True
        self._requested = tmdb_id
        self.publish(tmdb_id)
        self._learner_cancel.set()
        self._learner_cancel = cancel = threading.Event()
        threading.Thread(target=self._learn, args=(tmdb_id, cancel), daemon=True).start()
        return True

    def stop(self):
        self._learner_cancel.set()

    def _learn(self, tmdb_id, cancel):
        """Wait for the helper's list on the open page and remember its answer."""
        try:
            deadline = time.time() + LEARN_TIMEOUT_SECONDS
            monitor = xbmc.Monitor()
            while time.time() < deadline and not cancel.is_set():
                if monitor.waitForAbort(LEARN_POLL_SECONDS):
                    return
                state = read_helper_list(tmdb_id)
                if state == "closed":
                    return
                if isinstance(state, dict):
                    self.remember(tmdb_id, **state)
                    return
            if not cancel.is_set() and read_helper_list(tmdb_id) == "empty":
                # The helper finished and has nothing next for this show.
                self.forget(tmdb_id)
        except Exception as exc:
            _log(f"Could not read the helper's up-next list: {exc}", xbmc.LOGWARNING)

    # ---------------------------------------------------------------- #
    # Playback
    # ---------------------------------------------------------------- #
    def on_progress_saved(self, context, outcome, find_next):
        """Update the memory from an episode KDMM just stopped playing.

        outcome is "resume" or "watched". find_next(context) returns the
        following episode ({"season", "episode"}) or None and may be slow, so
        it runs on its own thread.
        """
        context = context or {}
        tmdb_id = str(context.get("tmdb_id") or "").strip()
        if context.get("is_movie") or not tmdb_id.isdigit():
            return None
        if outcome == "resume":
            self.remember(tmdb_id, context.get("season"), context.get("episode"), resumable=True)
            return None
        if outcome != "watched":
            return None
        # Until the next episode is known, show nothing rather than the
        # episode that was just finished.
        self.forget(tmdb_id)

        def _find():
            try:
                upcoming = find_next(context)
            except Exception as exc:
                _log(f"Next-episode lookup failed: {exc}", xbmc.LOGWARNING)
                return
            if upcoming:
                self.remember(tmdb_id, upcoming.get("season"), upcoming.get("episode"))

        thread = threading.Thread(target=_find, daemon=True)
        thread.start()
        return thread


def read_helper_list(tmdb_id):
    """State of the helper's hidden up-next list on the open title page.

    Returns "closed" when the page for this show is no longer open, "loading"
    or "empty", or a dict of what the list says is next.
    """
    if not xbmc.getCondVisibility("Window.IsActive(movieinformation)"):
        return "closed"
    if xbmc.getInfoLabel("ListItem.UniqueID(tmdb)") != str(tmdb_id):
        return "closed"
    prefix = f"Container({HELPER_LIST_ID})"
    if xbmc.getCondVisibility(f"{prefix}.IsUpdating"):
        return "loading"
    if not _to_int(xbmc.getInfoLabel(f"{prefix}.NumItems")):
        return "empty"
    if xbmc.getInfoLabel(f"{prefix}.ListItem.UniqueID(tvshow.tmdb)") != str(tmdb_id):
        return "loading"
    season = _to_int(xbmc.getInfoLabel(f"{prefix}.ListItem.Season"))
    episode = _to_int(xbmc.getInfoLabel(f"{prefix}.ListItem.Episode"))
    if season is None or not episode:
        return "loading"
    return {
        "season": season,
        "episode": episode,
        "resumable": bool(xbmc.getCondVisibility(f"{prefix}.ListItem.IsResumable")),
        "path": xbmc.getInfoLabel(f"{prefix}.ListItem.FolderPath"),
    }
