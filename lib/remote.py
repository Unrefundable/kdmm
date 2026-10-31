"""Remote keymaps and FF/RW seek-on-release, folded in from service.kodi.lg."""

import json
import os
import time

import xbmc
import xbmcvfs

LEGACY_ADDON_ID = "service.kodi.lg"
KEYMAP_DIR = "special://userdata/keymaps/"
REMOTE_KEYMAP = "kdmm_remote.xml"
SEEK_KEYMAP = "kdmm_seek.xml"
# Written by service.kodi.lg; they call a script that no longer exists once
# it is removed, and they would fight KDMM's own Back mapping.
LEGACY_KEYMAPS = ("kodi_lg.xml", "kodi_seek.xml")

SEEK_FORWARD = "seek_forward"
SEEK_BACK = "seek_back"
SEEK_COMMIT_DELAY_SECONDS = 1.5
BIG_SEEK_STEP_SECONDS = 600


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Remote] {msg}", level)


def _read(path):
    try:
        f = xbmcvfs.File(path)
        content = f.read()
        f.close()
        return content
    except Exception:
        return None


def _install(src, dst):
    """Copy src to dst when the content differs. Returns True if written."""
    content = _read(src)
    if content is None:
        _log(f"Could not read keymap {src}", xbmc.LOGWARNING)
        return False
    if xbmcvfs.exists(dst) and _read(dst) == content:
        return False
    try:
        f = xbmcvfs.File(dst, "w")
        f.write(content)
        f.close()
        _log(f"Installed keymap {dst}")
        return True
    except Exception as exc:
        _log(f"Could not write keymap {dst}: {exc}", xbmc.LOGWARNING)
        return False


def _remove(path):
    if not xbmcvfs.exists(path):
        return False
    if xbmcvfs.delete(path):
        _log(f"Removed keymap {path}")
        return True
    _log(f"Could not remove keymap {path}", xbmc.LOGWARNING)
    return False


def install_keymaps(addon_path, remote_enabled, seek_enabled):
    """Sync KDMM's keymaps with its settings and drop kodi-lg's leftovers."""
    keymap_dir = xbmcvfs.translatePath(KEYMAP_DIR)
    xbmcvfs.mkdirs(keymap_dir)
    changed = False
    for name, enabled in ((REMOTE_KEYMAP, remote_enabled), (SEEK_KEYMAP, seek_enabled)):
        dst = os.path.join(keymap_dir, name)
        if enabled:
            changed |= _install(os.path.join(addon_path, "resources", "keymaps", name), dst)
        else:
            changed |= _remove(dst)
    for name in LEGACY_KEYMAPS:
        changed |= _remove(os.path.join(keymap_dir, name))
    if changed:
        xbmc.executebuiltin("Action(reloadkeymaps)")


def disable_legacy_addon():
    """Turn off service.kodi.lg so it cannot reinstall its keymaps."""
    if not xbmc.getCondVisibility(f"System.AddonIsEnabled({LEGACY_ADDON_ID})"):
        return
    try:
        response = json.loads(xbmc.executeJSONRPC(json.dumps({
            "jsonrpc": "2.0",
            "method": "Addons.SetAddonEnabled",
            "params": {"addonid": LEGACY_ADDON_ID, "enabled": False},
            "id": 1,
        })))
    except Exception as exc:
        _log(f"Could not disable {LEGACY_ADDON_ID}: {exc}", xbmc.LOGWARNING)
        return
    if "error" in response:
        _log(f"Could not disable {LEGACY_ADDON_ID}: {response['error']}", xbmc.LOGWARNING)
        return
    _log(f"Disabled {LEGACY_ADDON_ID}; KDMM now provides its features")


class SeekAccumulator:
    """Collect FF/RW presses and commit one seek after the last press.

    Each press adds BIG_SEEK_STEP_SECONDS in its direction; reversing
    direction starts the count again. Seeking once avoids Kodi's speed-ramp
    FF/RW, which stalls on debrid streams.
    """

    def __init__(self):
        self._direction = 0
        self._count = 0
        self._last_press = 0.0

    def press(self, action):
        direction = {SEEK_FORWARD: 1, SEEK_BACK: -1}.get(action)
        if not direction:
            return
        if direction != self._direction:
            self._count = 0
        self._direction = direction
        self._count += 1
        self._last_press = time.time()

    def tick(self, player):
        if not self._count or time.time() - self._last_press < SEEK_COMMIT_DELAY_SECONDS:
            return
        offset = self._direction * self._count * BIG_SEEK_STEP_SECONDS
        self._direction = 0
        self._count = 0
        if not player.isPlayingVideo():
            return
        try:
            current = player.getTime()
            total = player.getTotalTime()
            target = max(0.0, min(float(total), current + offset))
            _log(f"Seek {offset:+d}s -> {target:.0f}s")
            player.seekTime(target)
        except Exception as exc:
            _log(f"seekTime failed: {exc}", xbmc.LOGWARNING)
