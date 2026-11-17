"""Open the TMDb Bingie Helper details page for a KDMM playback context.

The ListItem is built by the helper's own router and handed straight to
Kodi's video-info dialog, so the page shown is always the title that was
playing. Nothing depends on which item has focus.
"""

import threading
import time
from urllib.parse import urlencode

import xbmc
import xbmcgui

from bingie_helper import BINGIE_HELPER_ID, add_bingie_helper_paths

_get_listitem = None

# Longest wait for a build that is still running when playback stops.
PREPARE_WAIT_SECONDS = 20


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Details] {msg}", level)


def details_path(context):
    """Return the helper's details route for a movie or the episode's show."""
    context = context or {}
    params = {
        "info": "details",
        "tmdb_type": "movie" if context.get("is_movie") else "tv",
        "nextpage": "false",
    }
    tmdb_id = str(context.get("tmdb_id") or "").strip()
    imdb_id = str(context.get("imdb_id") or "").strip()
    if tmdb_id:
        params["tmdb_id"] = tmdb_id
    elif imdb_id:
        params["imdb_id"] = imdb_id
    else:
        return ""
    return f"plugin://{BINGIE_HELPER_ID}/?" + urlencode(params)


def _import_get_listitem():
    global _get_listitem
    if _get_listitem:
        return _get_listitem
    if not add_bingie_helper_paths():
        _log("TMDb Bingie Helper is not installed", xbmc.LOGWARNING)
        return None
    try:
        from tmdbbingiehelper.lib.window.manager import get_listitem
    except Exception as exc:
        _log(f"Could not import Bingie helper listitem builder: {exc}", xbmc.LOGWARNING)
        return None
    _get_listitem = get_listitem
    return _get_listitem


class _Prepared:
    """One details ListItem built ahead of time, for the title now playing."""

    def __init__(self, path):
        self.path = path
        self.listitem = None
        self.done = threading.Event()


_prepared = None
_prepared_lock = threading.Lock()


def _build(path):
    get_listitem = _import_get_listitem()
    if not get_listitem:
        return None
    try:
        return get_listitem(path)
    except Exception as exc:
        _log(f"Building details listitem failed for {path}: {exc}", xbmc.LOGWARNING)
        return None


def prepare_details(context):
    """Build the details page for context in the background.

    Called while the title plays, so stopping can show the page at once
    instead of waiting several seconds for the helper on a slow box.
    Returns True when a build was started or is already held for context.
    """
    global _prepared
    path = details_path(context)
    if not path:
        return False
    with _prepared_lock:
        if _prepared and _prepared.path == path:
            return True
        entry = _prepared = _Prepared(path)

    def _run():
        started = time.time()
        try:
            entry.listitem = _build(path)
        finally:
            entry.done.set()
        _log(f"Prepared details page in {time.time() - started:.1f}s")

    threading.Thread(target=_run, daemon=True).start()
    return True


def _take_prepared(path):
    """Hand over the prepared ListItem for path, waiting if it is mid-build."""
    global _prepared
    with _prepared_lock:
        entry, _prepared = _prepared, None
    if not entry or entry.path != path:
        return None
    if not entry.done.is_set():
        xbmc.executebuiltin("ActivateWindow(busydialognocancel)")
        try:
            entry.done.wait(PREPARE_WAIT_SECONDS)
        finally:
            xbmc.executebuiltin("Dialog.Close(busydialognocancel,true)")
    return entry.listitem


def open_details(context):
    """Show the details page for context. Blocks until the dialog closes.

    Returns False when the page could not be built; the user is then left on
    whatever window Kodi returned to.
    """
    path = details_path(context)
    if not path:
        _log("Playback context has no TMDb or IMDb id; not opening details")
        return False

    listitem = _take_prepared(path)
    if not listitem:
        xbmc.executebuiltin("ActivateWindow(busydialognocancel)")
        try:
            listitem = _build(path)
        finally:
            xbmc.executebuiltin("Dialog.Close(busydialognocancel,true)")

    if not listitem:
        _log(f"No details listitem for {path}", xbmc.LOGWARNING)
        return False

    _log(f"Opening details page {path}")
    xbmc.executebuiltin("Dialog.Close(movieinformation,true)")
    xbmcgui.Dialog().info(listitem)
    return True
