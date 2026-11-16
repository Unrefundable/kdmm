"""Size TMDb Bingie Helper for the device, folded in from service.kodi.lg.

Applied once at service start. Each step only writes when a value differs:
low-power boxes (e.g. CoreELEC on Amlogic) stutter when settings files are
rewritten.
"""

import os
import xml.etree.ElementTree as ET

import xbmc
import xbmcaddon
import xbmcvfs

from bingie_helper import BINGIE_HELPER_ID

_BINGIE_THREAD_PATH = "special://home/addons/script.module.bingie/resources/modules/bingie/thread.py"
_BINGIE_THREAD_MAX = 12
_HELPER_DATABASE_PATH = (
    f"special://home/addons/{BINGIE_HELPER_ID}/resources/tmdbbingiehelper/lib/files/database.py"
)
_HELPER_PRAGMA_ANCHOR = 'connection.execute("PRAGMA foreign_keys=ON")'
_HELPER_PRAGMA_MMAP_OFF = 'connection.execute("PRAGMA mmap_size=0")'

# A Mac has the CPU for 260-item Trakt lists; an Amlogic box does not.
# Two Trakt pages (40 items) still fill the skin's 25-item widgets.
_HELPER_SETTINGS_MACOS = {
    "pagemulti_trakt": "13",
    "max_threads": "10",
}
_HELPER_SETTINGS_LOW_POWER = {
    "pagemulti_trakt": "2",
    "max_threads": "4",
    "service_listitem_read_ahead": "false",
}
_MACOS_PAGEMULTI_MAX = "13"
_MACOS_FILE_LIMIT = 4096


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Tuning] {msg}", level)


def _is_macos():
    return bool(xbmc.getCondVisibility("System.Platform.OSX"))


def patch_bingie_thread_limit():
    """Cap Bingie's unlimited ParallelThread fan-out for 250-item widgets."""
    path = xbmcvfs.translatePath(_BINGIE_THREAD_PATH)
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
        if f"thread_max = {_BINGIE_THREAD_MAX}" in raw:
            return
        updated = raw.replace("thread_max = 0", f"thread_max = {_BINGIE_THREAD_MAX}")
        if updated != raw:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(updated)
            _log(f"Bingie ParallelThread limit set to {_BINGIE_THREAD_MAX}")
    except Exception as exc:
        _log(f"Could not patch Bingie thread limit: {exc}", xbmc.LOGWARNING)


def patch_helper_database_mmap():
    """Stop the helper's SQLite connections memory-mapping its databases.

    The helper never closes its connections, and each one maps the whole
    database (about 66 MB for ItemBuilder.db). On a 32-bit Kodi a few hours
    of browsing fills the address space; Kodi then cannot allocate its video
    cache and every stream fails to open until Kodi is restarted.
    """
    path = xbmcvfs.translatePath(_HELPER_DATABASE_PATH)
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
        if _HELPER_PRAGMA_MMAP_OFF in raw:
            return False
        if _HELPER_PRAGMA_ANCHOR not in raw:
            _log("Helper database code has changed; mmap patch not applied", xbmc.LOGWARNING)
            return False
        indent = raw[raw.rfind("\n", 0, raw.index(_HELPER_PRAGMA_ANCHOR)) + 1:raw.index(_HELPER_PRAGMA_ANCHOR)]
        updated = raw.replace(
            _HELPER_PRAGMA_ANCHOR,
            f"{_HELPER_PRAGMA_ANCHOR}\n{indent}{_HELPER_PRAGMA_MMAP_OFF}",
            1,
        )
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(updated)
        _log("Helper databases no longer memory-mapped (takes effect after Kodi restarts)")
        return True
    except Exception as exc:
        _log(f"Could not patch helper database mmap: {exc}", xbmc.LOGWARNING)
        return False


def _patch_helper_pagemulti_maximum():
    """Allow pagemulti_trakt=13 so Kodi accepts 260-item Trakt lists."""
    path = xbmcvfs.translatePath(f"special://home/addons/{BINGIE_HELPER_ID}/resources/settings.xml")
    if not os.path.exists(path):
        return
    try:
        tree = ET.parse(path)
        for setting in tree.getroot().iter("setting"):
            if setting.get("id") != "pagemulti_trakt":
                continue
            constraints = setting.find("constraints")
            if constraints is None:
                constraints = ET.SubElement(setting, "constraints")
            maximum = constraints.find("maximum")
            if maximum is None:
                maximum = ET.SubElement(constraints, "maximum")
            if maximum.text != _MACOS_PAGEMULTI_MAX:
                maximum.text = _MACOS_PAGEMULTI_MAX
                tree.write(path, encoding="utf-8", xml_declaration=True)
                _log(f"Helper pagemulti_trakt maximum set to {_MACOS_PAGEMULTI_MAX}")
            return
    except Exception as exc:
        _log(f"Could not patch helper settings schema: {exc}", xbmc.LOGWARNING)


def tune_bingie_helper():
    """Set the helper's Trakt paging and thread count for this platform."""
    try:
        helper = xbmcaddon.Addon(BINGIE_HELPER_ID)
    except RuntimeError:
        return
    if _is_macos():
        _patch_helper_pagemulti_maximum()
        managed = _HELPER_SETTINGS_MACOS
    else:
        managed = _HELPER_SETTINGS_LOW_POWER
    changed = 0
    for key, value in managed.items():
        try:
            if helper.getSetting(key) != value:
                helper.setSetting(key, value)
                changed += 1
        except Exception as exc:
            _log(f"Could not set helper setting {key}: {exc}", xbmc.LOGWARNING)
    if changed:
        _log(f"Helper settings tuned ({changed} value update(s))")


def raise_file_limit():
    """Give large cloud widgets enough file handles on macOS."""
    if not _is_macos():
        return
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = min(max(soft, _MACOS_FILE_LIMIT), hard if hard > 0 else _MACOS_FILE_LIMIT)
        if soft < target:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
            _log(f"Raised file descriptor limit from {soft} to {target}")
    except Exception as exc:
        _log(f"Could not raise file descriptor limit: {exc}", xbmc.LOGWARNING)


def apply_device_tuning():
    patch_bingie_thread_limit()
    patch_helper_database_mmap()
    tune_bingie_helper()
    raise_file_limit()
