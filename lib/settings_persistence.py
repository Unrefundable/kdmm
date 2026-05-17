import json
import os

import xbmc
import xbmcaddon


STREAM_CACHE_TTL_KEY = "stream_cache_ttl_hours"
DEFAULT_STREAM_CACHE_TTL_HOURS = 336
LEGACY_RESET_TTL_HOURS = 6


def _log(message, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Settings] {message}", level)


def _path(userdata_path):
    return os.path.join(userdata_path, "settings_persistence.json")


def _load(userdata_path):
    path = _path(userdata_path)
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                return data if isinstance(data, dict) else {}
        except Exception:
            pass
    return {}


def _save(userdata_path, data):
    try:
        os.makedirs(userdata_path, exist_ok=True)
        with open(_path(userdata_path), "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
    except Exception as exc:
        _log(f"Could not persist settings: {exc}", xbmc.LOGWARNING)


def get_stream_cache_ttl_hours(userdata_path):
    """
    Kodi can reset number settings to the old addon default after updates.
    Keep the last non-legacy value in addon data and restore it when that
    reset happens.
    """
    addon = xbmcaddon.Addon()
    saved = _load(userdata_path)
    saved_ttl = saved.get(STREAM_CACHE_TTL_KEY)

    try:
        current = int(addon.getSetting(STREAM_CACHE_TTL_KEY) or "0")
    except Exception:
        current = 0

    if current == LEGACY_RESET_TTL_HOURS:
        restored = int(saved_ttl or DEFAULT_STREAM_CACHE_TTL_HOURS)
        if restored != current:
            try:
                addon.setSetting(STREAM_CACHE_TTL_KEY, str(restored))
                _log(f"Restored stream cache TTL to {restored}h")
            except Exception as exc:
                _log(f"Could not restore stream cache TTL: {exc}", xbmc.LOGWARNING)
        saved[STREAM_CACHE_TTL_KEY] = restored
        _save(userdata_path, saved)
        return restored

    if current <= 0:
        current = int(saved_ttl or DEFAULT_STREAM_CACHE_TTL_HOURS)
        try:
            addon.setSetting(STREAM_CACHE_TTL_KEY, str(current))
        except Exception:
            pass

    saved[STREAM_CACHE_TTL_KEY] = current
    _save(userdata_path, saved)
    return current
