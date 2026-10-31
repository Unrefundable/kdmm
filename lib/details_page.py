"""Open the TMDb Bingie Helper details page for a KDMM playback context.

The ListItem is built by the helper's own router and handed straight to
Kodi's video-info dialog, so the page shown is always the title that was
playing. Nothing depends on which item has focus.
"""

from urllib.parse import urlencode

import xbmc
import xbmcgui

from bingie_helper import BINGIE_HELPER_ID, add_bingie_helper_paths

_get_listitem = None


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


def open_details(context):
    """Show the details page for context. Blocks until the dialog closes.

    Returns False when the page could not be built; the user is then left on
    whatever window Kodi returned to.
    """
    path = details_path(context)
    if not path:
        _log("Playback context has no TMDb or IMDb id; not opening details")
        return False
    get_listitem = _import_get_listitem()
    if not get_listitem:
        return False

    xbmc.executebuiltin("ActivateWindow(busydialognocancel)")
    try:
        listitem = get_listitem(path)
    except Exception as exc:
        listitem = None
        _log(f"Building details listitem failed for {path}: {exc}", xbmc.LOGWARNING)
    finally:
        xbmc.executebuiltin("Dialog.Close(busydialognocancel,true)")

    if not listitem:
        _log(f"No details listitem for {path}", xbmc.LOGWARNING)
        return False

    _log(f"Opening details page {path}")
    xbmc.executebuiltin("Dialog.Close(movieinformation,true)")
    xbmcgui.Dialog().info(listitem)
    return True
