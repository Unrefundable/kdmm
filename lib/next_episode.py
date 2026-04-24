import json
import os
import sys

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
    if modules_path not in sys.path:
        sys.path.insert(0, modules_path)

    try:
        import tmdbbingiehelper_lib  # noqa: F401
        from tmdbbingiehelper.lib.player.details import get_next_episodes
        return get_next_episodes
    except Exception as exc:
        _log(f"Could not import Bingie helper next-episode API: {exc}", xbmc.LOGWARNING)
        return None


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
        return None

    try:
        items = get_next_episodes(tmdb_id, season, episode, context.get("player_file") or "kdmm.json") or []
    except Exception as exc:
        _log(f"Next-episode lookup failed: {exc}", xbmc.LOGWARNING)
        return None

    current_key = (season, episode)
    best_item = None
    best_key = None
    for item in items:
        infolabels = getattr(item, "infolabels", {}) or {}
        candidate_key = (_extract_int(infolabels.get("season")), _extract_int(infolabels.get("episode")))
        if not candidate_key[0] or not candidate_key[1] or candidate_key <= current_key:
            continue
        if best_key is None or candidate_key < best_key:
            best_item = item
            best_key = candidate_key

    if not best_item or not best_key:
        return None

    try:
        play_url = best_item.get_url()
    except Exception:
        play_url = None
    if not play_url:
        return None

    infolabels = getattr(best_item, "infolabels", {}) or {}
    return {
        "play_url": play_url,
        "season": best_key[0],
        "episode": best_key[1],
        "title": infolabels.get("title") or "",
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
