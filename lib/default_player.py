"""Make KDMM the player TMDb Bingie Helper uses for movies and episodes.

The helper stores its default players as "<player file> <mode>" strings and
asks which player to use while they are empty. KDMM fills them in once, the
first time its service runs with the helper installed, so playback works
straight after install. After that the choice is the user's: clearing or
changing the default in the helper's settings is never undone here.
"""

import xbmc
import xbmcaddon

from bingie_helper import BINGIE_HELPER_ID
from settings_persistence import get_flag, set_flag

PLAYER_FILE = "kdmm.json"
_DEFAULTS = {
    "default_player_movies": f"{PLAYER_FILE} play_movie",
    "default_player_episodes": f"{PLAYER_FILE} play_episode",
}
_CLAIMED_FLAG = "default_player_claimed"


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Player] {msg}", level)


def set_default_player(force=False):
    """Point the helper's default players at KDMM.

    Without force only empty settings are filled in. Returns the number of
    settings changed, or None when the helper is not installed.
    """
    try:
        helper = xbmcaddon.Addon(BINGIE_HELPER_ID)
    except RuntimeError:
        return None
    changed = 0
    for key, value in _DEFAULTS.items():
        try:
            current = helper.getSetting(key)
            if current == value or (current and not force):
                continue
            helper.setSetting(key, value)
            changed += 1
        except Exception as exc:
            _log(f"Could not set helper setting {key}: {exc}", xbmc.LOGWARNING)
    if changed:
        _log(f"KDMM set as the helper's default player ({changed} setting(s))")
    return changed


def claim_default_player_once(userdata_path):
    """Fill in the helper's empty default players the first time only."""
    if get_flag(userdata_path, _CLAIMED_FLAG):
        return
    if set_default_player() is None:
        return  # helper not installed yet: try again on the next start
    set_flag(userdata_path, _CLAIMED_FLAG)
