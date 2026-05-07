import json
import os
import sys
from urllib.parse import urlencode

import xbmc
import xbmcaddon


def _log(message, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Next] {message}", level)


def _extract_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError, AttributeError):
        return None


def _import_bingie_helper_get_next_episodes():
    try:
        addon_path = xbmcaddon.Addon("plugin.video.tmdb.bingie.helper").getAddonInfo("path")
    except Exception:
        return None

    modules_path = os.path.join(addon_path, "resources", "modules")
    candidate_paths = (
        modules_path,
        os.path.join(addon_path, "resources", "lib"),
        addon_path,
    )
    for path in reversed(candidate_paths):
        if path and path not in sys.path:
            sys.path.insert(0, path)

    try:
        import tmdbbingiehelper_lib  # noqa: F401
        from tmdbbingiehelper.lib.player.details import get_next_episodes
        return get_next_episodes
    except Exception as exc:
        _log(f"Could not import Bingie helper next-episode API: {exc}", xbmc.LOGWARNING)
        return None


def _import_bingie_helper_tmdb_api():
    try:
        addon_path = xbmcaddon.Addon("plugin.video.tmdb.bingie.helper").getAddonInfo("path")
    except Exception:
        return None

    modules_path = os.path.join(addon_path, "resources", "modules")
    candidate_paths = (
        modules_path,
        os.path.join(addon_path, "resources", "lib"),
        addon_path,
    )
    for path in reversed(candidate_paths):
        if path and path not in sys.path:
            sys.path.insert(0, path)

    try:
        import tmdbbingiehelper_lib  # noqa: F401
        from tmdbbingiehelper.lib.api.tmdb.api import TMDb
        return TMDb
    except Exception as exc:
        _log(f"Could not import Bingie helper TMDb API: {exc}", xbmc.LOGWARNING)
        return None


def _item_episode_key(item):
    infolabels = {}
    if isinstance(item, dict):
        infolabels = item.get("infolabels") or {}
    else:
        infolabels = getattr(item, "infolabels", {}) or {}

    season = _extract_int(infolabels.get("season"))
    episode = _extract_int(infolabels.get("episode"))
    if season and episode:
        return season, episode

    if isinstance(item, dict):
        season = _extract_int(item.get("season") or item.get("season_number"))
        episode = _extract_int(item.get("episode") or item.get("episode_number"))
        if season and episode:
            return season, episode

    return None


def _item_title(item):
    infolabels = {}
    if isinstance(item, dict):
        infolabels = item.get("infolabels") or {}
        return infolabels.get("title") or item.get("title") or item.get("name") or ""

    infolabels = getattr(item, "infolabels", {}) or {}
    return infolabels.get("title") or ""


def _build_kdmm_episode_url(context, season, episode, title=""):
    imdb_id = str(context.get("imdb_id") or "").strip()
    tmdb_id = str(context.get("tmdb_id") or "").strip()
    if not imdb_id or not tmdb_id:
        return None

    params = {
        "action": "play_episode",
        "imdb": imdb_id,
        "tmdb": tmdb_id,
        "season": season,
        "episode": episode,
        "title": title or "",
        "showtitle": context.get("showtitle") or "",
    }

    return {
        "play_url": f"plugin://plugin.video.kdmm/?{urlencode(params)}",
        "season": season,
        "episode": episode,
        "title": title or "",
    }


def _fallback_next_episode(context, season, episode):
    TMDb = _import_bingie_helper_tmdb_api()
    if not TMDb:
        _log("No catalog API available for next-episode fallback", xbmc.LOGWARNING)
        return None

    tmdb_id = _extract_int(context.get("tmdb_id"))
    if not tmdb_id:
        return None

    try:
        items = TMDb().get_flatseasons_list(tmdb_id) or []
    except Exception as exc:
        _log(f"Catalog next-episode fallback failed: {exc}", xbmc.LOGWARNING)
        return None

    current_key = (season, episode)
    best_item = None
    best_key = None
    for item in items:
        candidate_key = _item_episode_key(item)
        if not candidate_key or candidate_key <= current_key:
            continue
        if best_key is None or candidate_key < best_key:
            best_item = item
            best_key = candidate_key

    if not best_key:
        _log(f"No real next episode found after S{season}E{episode}")
        return None

    next_episode = _build_kdmm_episode_url(
        context,
        best_key[0],
        best_key[1],
        title=_item_title(best_item),
    )
    if next_episode:
        _log(f"Using catalog fallback next episode URL for S{best_key[0]}E{best_key[1]}")
    return next_episode


def get_next_episode(context):
    context = context or {}
    if context.get("is_movie"):
        return None

    tmdb_id = _extract_int(context.get("tmdb_id"))
    season = _extract_int(context.get("season"))
    episode = _extract_int(context.get("episode"))
    if not tmdb_id or not season or not episode:
        return None

    get_next_episodes = _import_bingie_helper_get_next_episodes()
    if not get_next_episodes:
        return _fallback_next_episode(context, season, episode)

    try:
        items = get_next_episodes(tmdb_id, season, episode, context.get("player_file") or "kdmm.json") or []
    except Exception as exc:
        _log(f"Next-episode lookup failed: {exc}", xbmc.LOGWARNING)
        return _fallback_next_episode(context, season, episode)

    current_key = (season, episode)
    best_item = None
    best_key = None
    for item in items:
        candidate_key = _item_episode_key(item)
        if not candidate_key or candidate_key <= current_key:
            continue
        if best_key is None or candidate_key < best_key:
            best_item = item
            best_key = candidate_key

    if not best_item or not best_key:
        return _fallback_next_episode(context, season, episode)

    try:
        play_url = best_item.get_url()
    except Exception:
        play_url = None
    if not play_url:
        return _build_kdmm_episode_url(
            context,
            best_key[0],
            best_key[1],
            title=_item_title(best_item),
        ) or _fallback_next_episode(context, season, episode)

    infolabels = getattr(best_item, "infolabels", {}) or {}
    return {
        "play_url": play_url,
        "season": best_key[0],
        "episode": best_key[1],
        "title": infolabels.get("title") or _item_title(best_item),
    }


def play_next_episode(next_episode):
    play_url = (next_episode or {}).get("play_url")
    if not play_url:
        return False

    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "Player.Open",
            "id": 1,
            "params": {"item": {"file": play_url}},
        }
        response = json.loads(xbmc.executeJSONRPC(json.dumps(payload)))
        if response and "error" not in response:
            return True
        _log(f"Player.Open failed: {response}", xbmc.LOGWARNING)
    except Exception as exc:
        _log(f"Could not open next episode: {exc}", xbmc.LOGWARNING)
    return False
