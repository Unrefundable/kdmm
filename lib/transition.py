"""The "switching to the next episode" flag shared by the service and overlay.

The flag holds the time it was raised, so a next-episode start that never
reaches playback cannot leave it set for the rest of the Kodi session.
"""

import time

import xbmcgui

PROP_NEXT_EPISODE_TRANSITION = "kdmm.next_episode_transition"
TRANSITION_TIMEOUT_SECONDS = 60

_WIN = xbmcgui.Window(10000)


def begin():
    _WIN.setProperty(PROP_NEXT_EPISODE_TRANSITION, str(time.time()))


def clear():
    _WIN.clearProperty(PROP_NEXT_EPISODE_TRANSITION)


def active():
    raw = _WIN.getProperty(PROP_NEXT_EPISODE_TRANSITION)
    if not raw:
        return False
    try:
        started = float(raw)
    except (TypeError, ValueError):
        started = 0.0
    if time.time() - started > TRANSITION_TIMEOUT_SECONDS:
        clear()
        return False
    return True
