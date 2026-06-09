"""
KDMM – default.py
Plugin entry point.

Called by TMDb Bingie Helper when the user presses Play or Resume.
Runs as a standard Kodi resolver plugin: always exits via
xbmcplugin.setResolvedUrl() so Kodi manages playback internally and
all Player-subclass callbacks (onAVStarted, onPlayBackStopped, …) fire
correctly in service.py.

Flow:
  1. Parse media ID from URL params (IMDB ID + optional season/episode).
  2. Look up a cached stream URL.  On cache miss, query DMM's torrent
     database, check RD availability, resolve best, and save to cache.
  3. Read locally-stored resume position (populated by service.py).
  4. Set window properties so the background service can apply the resume seek
     after A/V playback has actually started (onAVStarted).
  5. Resolve to Kodi via xbmcplugin.setResolvedUrl().
"""

import json
import os
import sys
import threading
import time
from urllib.parse import parse_qsl, urlencode

import xbmc
import xbmcaddon
import xbmcgui
import xbmcplugin
import xbmcvfs

# ------------------------------------------------------------------ #
# Bootstrap: add lib/ to sys.path before importing local modules.
# ------------------------------------------------------------------ #
_ADDON = xbmcaddon.Addon()
_ADDON_ID = _ADDON.getAddonInfo("id")
_ADDON_PATH = _ADDON.getAddonInfo("path")
_USERDATA_PATH = xbmcvfs.translatePath(
    f"special://profile/addon_data/{_ADDON_ID}/"
)
# Plugin handle – valid when Kodi invoked us as a resolver.
ADDON_HANDLE = int(sys.argv[1]) if len(sys.argv) > 1 else -1
# Kodi 18+ passes "resume:false" as sys.argv[3] when PlayMedia(..., noresume)
# is used (i.e. "Play from Beginning").  Any other value (or absent) means
# the normal resume-from-saved-position behaviour applies.
NO_RESUME = len(sys.argv) > 3 and "resume:false" in sys.argv[3].lower()
sys.path.insert(0, os.path.join(_ADDON_PATH, "lib"))

from cache import StreamCache, ProgressCache, PackBindingCache        # noqa: E402 (after sys.path)
from dmm import (    # noqa: E402
    candidate_matches_audio_preference,
    fetch_all_cached_streams,
    is_av1_stream,
    is_stream_accessible,
)
from playback import apply_playback_metadata, build_playback_context, encode_playback_context  # noqa: E402
from ad_auth import authorize as ad_authorize, revoke as ad_revoke  # noqa: E402
from rd_auth import authorize as rd_authorize, revoke as rd_revoke  # noqa: E402
from settings_persistence import get_stream_cache_ttl_hours  # noqa: E402

# ------------------------------------------------------------------ #
# Constants – window property keys shared with service.py
# ------------------------------------------------------------------ #
WIN = xbmcgui.Window(10000)
PROP_MEDIA_ID = "kdmm.media_id"
PROP_RESUME_TIME = "kdmm.resume_time"
PROP_CANDIDATES = "kdmm.candidates"   # JSON list of all cached stream candidates
PROP_PLAYBACK_CONTEXT = "kdmm.playback_context"
PROP_PENDING_PLAYBACK = "kdmm.pending_playback"


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM] {msg}", level)


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _build_final_url(url, headers_dict):
    """
    Produce the final URL string for xbmc.Player().play().

    If the stream has proxy headers (e.g. an Authorization bearer token),
    we append them after a pipe in the format that Kodi's HTTP handler
    understands:
        https://example.com/stream.mp4|Authorization=Bearer+abc
    """
    if not headers_dict:
        return url, False

    formatted = urlencode(headers_dict)
    if not formatted:
        return url, False
    return f"{url}|{formatted}", True


def _play_stream(media_id, url, headers_dict, imdb, tmdb, title, showtitle,
                 season, episode, year=None, no_resume=False):
    """
    Build the ListItem and resolve it to Kodi via setResolvedUrl.
    """
    final_url, has_headers = _build_final_url(url, headers_dict)

    li = xbmcgui.ListItem(path=final_url)
    li.setProperty("IsPlayable", "true")

    playback_context = build_playback_context(
        media_id=media_id,
        imdb=imdb,
        tmdb=tmdb,
        title=title,
        showtitle=showtitle,
        season=season,
        episode=episode,
        year=year,
    )
    apply_playback_metadata(li, playback_context)

    # Only force inputstream.ffmpegdirect for adaptive / container formats that
    # Kodi's native HTTP player can't handle.
    _url_path = url.split("?")[0].lower()
    _ADAPTIVE_EXTS = (".ts", ".mpd", ".m3u8")
    _needs_ffmpegdirect = any(_url_path.endswith(ext) for ext in _ADAPTIVE_EXTS)

    if has_headers and _needs_ffmpegdirect:
        li.setProperty("inputstream", "inputstream.ffmpegdirect")
        if _url_path.endswith(".ts"):
            li.setMimeType("video/mp2t")
        elif _url_path.endswith(".mpd"):
            li.setMimeType("application/dash+xml")
        elif _url_path.endswith(".m3u8"):
            li.setMimeType("application/vnd.apple.mpegurl")

    # Tag the item with its IMDB number for Trakt scrobbling.
    if imdb:
        WIN.setProperty("script.trakt.ids", json.dumps({"imdb": imdb}))

    # Tell the service which item is playing so it can save progress later.
    WIN.setProperty(PROP_MEDIA_ID, media_id)
    WIN.setProperty(PROP_PLAYBACK_CONTEXT, encode_playback_context(playback_context))
    WIN.setProperty(PROP_PENDING_PLAYBACK, json.dumps({
        "media_id": media_id,
        "url": url,
        "timestamp": time.time(),
    }))

    # Queue a resume seek via window property; service.py reads it in onAVStarted.
    progress_cache = ProgressCache(_USERDATA_PATH)
    resume_time = progress_cache.get_resume_time(media_id)
    if no_resume:
        _log(f"Play from beginning requested for {media_id} – skipping resume seek")
        WIN.clearProperty(PROP_RESUME_TIME)
    elif resume_time > 5.0:
        _log(f"Queuing resume seek to {resume_time:.1f}s for {media_id}")
        WIN.setProperty(PROP_RESUME_TIME, str(resume_time))
    else:
        WIN.clearProperty(PROP_RESUME_TIME)

    _log(f"Resolving stream to Kodi: {final_url[:80]}…")
    xbmcplugin.setResolvedUrl(ADDON_HANDLE, True, li)


def _filter_playable_candidates(candidates):
    if isinstance(candidates, dict):
        candidates = [candidates]
    original = list(candidates or [])
    filtered = [
        candidate for candidate in original
        if not is_av1_stream(candidate)
        and candidate_matches_audio_preference(candidate)
    ]
    av1_removed = sum(1 for candidate in original if is_av1_stream(candidate))
    audio_removed = len(original) - av1_removed - len(filtered)
    if av1_removed:
        _log(f"Filtered {av1_removed} cached/resolved AV1 candidate(s)", xbmc.LOGWARNING)
    if audio_removed:
        _log(
            f"Filtered {audio_removed} cached/resolved candidate(s) missing "
            "the current audio language probe",
            xbmc.LOGWARNING,
        )
    return filtered


def _candidate_has_probe_consumed_url(candidate):
    return (
        candidate.get("av1_probe") in ("not_av1", "unknown")
        and not candidate.get("url_refreshed_after_probe")
    )


def _candidate_needs_accessibility_check(candidate):
    # The AV1 probe already touched the first debrid URL. When the resolver
    # refreshed it afterward, leave that fresh URL untouched for Kodi.
    return not bool(candidate.get("url_refreshed_after_probe"))


def _cache_needs_refresh(cached):
    candidates = cached if isinstance(cached, list) else [cached]
    return any(_candidate_has_probe_consumed_url(c) for c in candidates if isinstance(c, dict))


def _episode_cache_needs_identity_refresh(cached):
    candidates = cached if isinstance(cached, list) else [cached]
    return any(
        isinstance(candidate, dict)
        and candidate.get("episode_identity_probe") not in ("matched", "unknown")
        for candidate in candidates
    )


def _authorize_debrid_account():
    choice = xbmcgui.Dialog().select(
        "KDMM - Authorization Required",
        ["Authorize Real-Debrid", "Authorize AllDebrid", "Cancel"],
    )
    if choice == 0:
        return rd_authorize()
    if choice == 1:
        return ad_authorize()
    return False


def _auth_error_notice(error_code):
    code = (error_code or "").strip()
    if code == "no_debrid_token":
        return (
            "No valid Real-Debrid or AllDebrid authorization is configured.",
            "Authorize a debrid account now to continue watching?",
        )
    if "alldebrid" in code and "realdebrid" not in code:
        return (
            "AllDebrid was previously authorized, but the stored authorization is now rejected.",
            "Re-authorize AllDebrid now to continue watching?",
        )
    if "realdebrid" in code and "alldebrid" not in code:
        return (
            "Real-Debrid was previously authorized, but the stored authorization is now rejected.",
            "Re-authorize Real-Debrid now to continue watching?",
        )
    return (
        "All configured debrid authorizations are now rejected.",
        "Re-authorize Real-Debrid or AllDebrid now to continue watching?",
    )


def _wait_for_fetch(thread, cancel_event):
    xbmc.executebuiltin("ActivateWindow(busydialog)")
    xbmc.sleep(100)
    started = False
    try:
        while thread.is_alive():
            visible = (
                xbmc.getCondVisibility("Window.IsActive(busydialog)")
                or xbmc.getCondVisibility("Window.IsVisible(busydialog)")
            )
            if visible:
                started = True
            elif started:
                cancel_event.set()
                return False
            xbmc.sleep(200)
        return True
    finally:
        xbmc.executebuiltin("Dialog.Close(busydialog,true)")


# ------------------------------------------------------------------ #
# Actions
# ------------------------------------------------------------------ #

def action_play(params):
    """
    Core handler for play_movie / play_episode.
    Resolves or re-uses a cached stream URL and starts playback.
    """
    action = params.get("action", "play_movie")
    imdb = params.get("imdb", "").strip()
    tmdb = params.get("tmdb", "").strip()
    season = params.get("season", "").strip()
    episode = params.get("episode", "").strip()
    title = params.get("title", "").strip()
    showtitle = params.get("showtitle", "").strip()
    year = params.get("year", "").strip()
    query_title = showtitle or title
    force_refresh = params.get("refresh", "0") == "1"

    # Determine catalog type and video_id.
    if action == "play_episode" and season and episode:
        catalog_type = "series"
        video_id = f"{imdb}:{season}:{episode}"
        media_id = video_id
    else:
        catalog_type = "movie"
        video_id = imdb
        media_id = imdb

    if not media_id:
        xbmcgui.Dialog().notification(
            "KDMM", "No media ID – check player JSON config",
            xbmcgui.NOTIFICATION_ERROR,
        )
        return

    ttl_hours = get_stream_cache_ttl_hours(_USERDATA_PATH)
    stream_cache = StreamCache(_USERDATA_PATH, ttl=ttl_hours * 3600)
    pack_cache = PackBindingCache(_USERDATA_PATH)
    bound_pack = pack_cache.get(imdb, season) if catalog_type == "series" else None

    # ---- 1. Try stream cache ---------------------------------------- #
    candidates = None
    if not force_refresh:
        cached = stream_cache.get(media_id)
        if cached:
            if _cache_needs_refresh(cached):
                _log(f"Refreshing {media_id}; cached URL may have been consumed by codec probe")
                stream_cache.clear(media_id)
                cached = None
            elif catalog_type == "series" and _episode_cache_needs_identity_refresh(cached):
                _log(f"Refreshing {media_id}; cached episode stream predates identity checks")
                stream_cache.clear(media_id)
                cached = None
            if cached:
                cached_hash = ""
                if isinstance(cached, list) and cached:
                    cached_hash = (cached[0].get("hash") or "").lower()
                if bound_pack and cached_hash != (bound_pack.get("hash") or "").lower():
                    _log(f"Bypassing stale episode cache for {media_id}; bound pack is {bound_pack.get('hash')}")
                else:
                    _log(f"Cache hit for {media_id}")
                    candidates = _filter_playable_candidates(cached)
                    if not candidates:
                        _log(f"Cached candidates for {media_id} were AV1 only; refreshing")
                        stream_cache.clear(media_id)
                        if catalog_type == "series":
                            pack_cache.clear(imdb, season)
                        candidates = None
                        bound_pack = None
                        cached = None
                    if cached and candidates and _ADDON.getSetting("notify_cache_hit").lower() == "true":
                        xbmcgui.Dialog().notification(
                            "KDMM", "Using cached stream",
                            xbmcgui.NOTIFICATION_INFO, 2000,
                        )

    # ---- 2. Fetch fresh from DMM + debrid if needed ---------------- #
    if candidates is None:
        _log(f"Cache miss – querying DMM + debrid for {media_id}")
        if force_refresh and catalog_type == "series":
            pack_cache.clear(imdb, season)

        fetch_result = {}
        cancel_event = threading.Event()

        def _fetch():
            try:
                fetch_result["candidates"] = fetch_all_cached_streams(
                    catalog_type, video_id, cancel_event=cancel_event,
                    query_title=query_title, year=year,
                    userdata_path=_USERDATA_PATH,
                    ignore_pack_binding=force_refresh,
                    episode_title=title if catalog_type == "series" else None)
            except PermissionError as exc:
                fetch_result["needs_auth"] = True
                fetch_result["auth_error"] = str(exc)
            except Exception as exc:
                fetch_result["error"] = exc

        t = threading.Thread(target=_fetch, daemon=True)
        t.start()

        if not _wait_for_fetch(t, cancel_event):
            xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
            return

        # Auth token missing or expired — prompt user to authorize a provider (main thread)
        if fetch_result.get("needs_auth"):
            auth_error = fetch_result.get("auth_error") or "no_debrid_token"
            notice, question = _auth_error_notice(auth_error)
            _log(f"Debrid authorization problem ({auth_error}) – prompting user to authorize",
                 xbmc.LOGWARNING)
            xbmcgui.Dialog().notification(
                "KDMM authorization",
                notice,
                xbmcgui.NOTIFICATION_ERROR,
                10000,
            )
            if xbmcgui.Dialog().yesno(
                "KDMM – Authorization Required",
                f"{notice}[CR]{question}",
                yeslabel="Authorize",
                nolabel="Cancel",
            ):
                if _authorize_debrid_account():
                    # Re-run the fetch now that we have a fresh token
                    fetch_result.clear()
                    cancel_event.clear()
                    t2 = threading.Thread(target=_fetch, daemon=True)
                    t2.start()
                    if not _wait_for_fetch(t2, cancel_event):
                        xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
                        return
                else:
                    xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
                    return
            else:
                xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
                return
            if fetch_result.get("needs_auth"):
                auth_error = fetch_result.get("auth_error") or "no_debrid_token"
                notice, _question = _auth_error_notice(auth_error)
                xbmcgui.Dialog().notification(
                    "KDMM", f"Authorization still rejected. {notice}",
                    xbmcgui.NOTIFICATION_ERROR, 8000)
                xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
                return

        if "error" in fetch_result:
            _log(f"fetch_all_cached_streams raised: {fetch_result['error']}", xbmc.LOGERROR)
            xbmcgui.Dialog().notification(
                "KDMM", f"Error: {fetch_result['error']}",
                xbmcgui.NOTIFICATION_ERROR, 8000)
            xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
            return

        candidates = _filter_playable_candidates(fetch_result.get("candidates") or [])

        if not candidates:
            xbmcgui.Dialog().notification(
                "KDMM",
                "No non-AV1 cached streams found",
                xbmcgui.NOTIFICATION_ERROR, 8000)
            xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
            return

        stream_cache.set(media_id, candidates)
        _log(f"Stored {len(candidates)} candidate(s) for {media_id}: {candidates[0]['name']!r}")

    # ---- 3. Ensure candidates is a list ----------------------------- #
    candidates = _filter_playable_candidates(candidates)

    # ---- 4. Store full candidate list for service.py retry ----------- #
    chosen_idx = 0
    for i, c in enumerate(candidates):
        if (not _candidate_needs_accessibility_check(c)
                or is_stream_accessible(c["url"], c.get("headers") or {})):
            if i > 0:
                _log(f"Skipped {i} inaccessible candidate(s); using: {c['name']!r}")
            chosen_idx = i
            break
        _log(f"Candidate {i} ({c['name']!r}) is too small – skipping", xbmc.LOGWARNING)
    else:
        _log("All candidates failed size check – falling back to first", xbmc.LOGWARNING)
        chosen_idx = 0

    remaining = candidates[chosen_idx:]
    WIN.setProperty(PROP_CANDIDATES, json.dumps(remaining))

    # ---- 5. Play first remaining candidate -------------------------- #
    stream = remaining[0]
    _play_stream(
        media_id=media_id,
        url=stream["url"],
        headers_dict=stream.get("headers") or {},
        imdb=imdb,
        tmdb=tmdb,
        title=title,
        showtitle=showtitle,
        season=season,
        episode=episode,
        year=year,
        no_resume=NO_RESUME,
    )


def action_clear_cache(params):
    """Clear the stream cache for one item (or all items)."""
    imdb = params.get("imdb", "").strip()
    season = params.get("season", "").strip()
    episode = params.get("episode", "").strip()

    if imdb and season and episode:
        media_id = f"{imdb}:{season}:{episode}"
    elif imdb:
        media_id = imdb
    else:
        media_id = None

    StreamCache(_USERDATA_PATH).clear(media_id)
    if imdb and season:
        PackBindingCache(_USERDATA_PATH).clear(imdb, season)
    elif imdb:
        PackBindingCache(_USERDATA_PATH).clear(imdb)
    else:
        PackBindingCache(_USERDATA_PATH).clear()
    label = media_id if media_id else "all entries"
    xbmcgui.Dialog().notification(
        "KDMM",
        f"Stream cache cleared ({label})",
        xbmcgui.NOTIFICATION_INFO,
    )


def action_clear_progress(params):
    """Reset the locally-stored resume position for one item."""
    imdb = params.get("imdb", "").strip()
    season = params.get("season", "").strip()
    episode = params.get("episode", "").strip()

    if imdb and season and episode:
        media_id = f"{imdb}:{season}:{episode}"
    elif imdb:
        media_id = imdb
    else:
        xbmcgui.Dialog().notification(
            "KDMM", "Provide imdb param to clear progress",
            xbmcgui.NOTIFICATION_WARNING,
        )
        return

    pc = ProgressCache(_USERDATA_PATH)
    pc.set_progress(media_id, 0.0, watched=False)
    xbmcgui.Dialog().notification(
        "KDMM",
        f"Resume position cleared for {media_id}",
        xbmcgui.NOTIFICATION_INFO,
    )


# ------------------------------------------------------------------ #
# Router
# ------------------------------------------------------------------ #

def action_main_menu():
    """
    Show a simple main menu when the addon is launched directly.
    """
    items = [
        ("Clear Stream Cache",
         "Force re-fetch stream URLs on next play",
         "plugin://plugin.video.kdmm/?action=clear_cache"),
    ]

    listing = []
    for label, label2, url in items:
        li = xbmcgui.ListItem(label=label, label2=label2)
        li.setProperty("IsPlayable", "false")
        listing.append((url, li, False))

    xbmcplugin.setContent(ADDON_HANDLE, "files")
    xbmcplugin.addDirectoryItems(ADDON_HANDLE, listing, len(listing))
    xbmcplugin.endOfDirectory(ADDON_HANDLE)


def addon_router():
    params = dict(parse_qsl(sys.argv[2][1:])) if len(sys.argv) > 2 else {}
    action = params.get("action", "")

    if action in ("play_movie", "play_episode", "play"):
        action_play(params)
    elif action == "authorize_rd":
        rd_authorize()
    elif action == "revoke_rd":
        rd_revoke()
        xbmcgui.Dialog().notification("KDMM", "Real-Debrid authorization revoked",
                                       xbmcgui.NOTIFICATION_INFO)
    elif action == "authorize_ad":
        ad_authorize()
    elif action == "revoke_ad":
        ad_revoke()
        xbmcgui.Dialog().notification("KDMM", "AllDebrid authorization revoked",
                                      xbmcgui.NOTIFICATION_INFO)
    elif action == "clear_cache":
        action_clear_cache(params)
    elif action == "clear_progress":
        action_clear_progress(params)
    else:
        action_main_menu()


if __name__ == "__main__":
    addon_router()
