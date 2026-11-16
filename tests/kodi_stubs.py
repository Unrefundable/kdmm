"""Stand-ins for Kodi's Python modules so KDMM's logic runs outside Kodi.

Import this before any KDMM module:  import kodi_stubs  # noqa: F401
Run the suite with:  python3 -m unittest discover -s tests
"""

import os
import sys
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "lib"))
sys.path.insert(0, ROOT)


class FakeAddon:
    settings = {}

    def __init__(self, addon_id=None):
        self.addon_id = addon_id or "plugin.video.kdmm"

    def getSetting(self, key):
        return FakeAddon.settings.get(key, "")

    def setSetting(self, key, value):
        FakeAddon.settings[key] = value

    def getAddonInfo(self, key):
        return {"id": self.addon_id, "path": ROOT}.get(key, "")


class FakeWindow:
    props = {}

    def __init__(self, *_args):
        pass

    def getProperty(self, key):
        return FakeWindow.props.get(key, "")

    def setProperty(self, key, value):
        FakeWindow.props[key] = value

    def clearProperty(self, key):
        FakeWindow.props.pop(key, None)


class FakeMonitor:
    def abortRequested(self):
        return False

    def waitForAbort(self, _timeout=0):
        return False


class FakePlayer:
    def __init__(self):
        pass


def _install():
    xbmc = types.ModuleType("xbmc")
    xbmc.LOGDEBUG, xbmc.LOGINFO, xbmc.LOGWARNING, xbmc.LOGERROR = 0, 1, 2, 3
    xbmc.log = lambda *_a, **_k: None
    xbmc.sleep = lambda _ms: None
    xbmc.getCondVisibility = lambda _c: False
    xbmc.getInfoLabel = lambda _l: ""
    xbmc.getGlobalIdleTime = lambda: 0
    xbmc.executebuiltin = lambda *_a, **_k: None
    xbmc.executeJSONRPC = lambda _p: "{}"
    xbmc.Monitor = FakeMonitor
    xbmc.Player = FakePlayer

    xbmcaddon = types.ModuleType("xbmcaddon")
    xbmcaddon.Addon = FakeAddon

    xbmcgui = types.ModuleType("xbmcgui")
    xbmcgui.Window = FakeWindow
    xbmcgui.WindowXMLDialog = type("WindowXMLDialog", (), {})
    xbmcgui.Dialog = type("Dialog", (), {"notification": lambda *_a, **_k: None})
    xbmcgui.ListItem = type("ListItem", (), {})
    xbmcgui.getCurrentWindowId = lambda: 10000
    xbmcgui.NOTIFICATION_ERROR = xbmcgui.NOTIFICATION_INFO = ""

    xbmcvfs = types.ModuleType("xbmcvfs")
    xbmcvfs.translatePath = lambda path: path
    xbmcvfs.exists = os.path.exists
    xbmcvfs.mkdirs = lambda path: os.makedirs(path, exist_ok=True)

    xbmcplugin = types.ModuleType("xbmcplugin")

    for module in (xbmc, xbmcaddon, xbmcgui, xbmcvfs, xbmcplugin):
        sys.modules[module.__name__] = module


_install()


def reset():
    """Forget settings and window properties between tests."""
    FakeAddon.settings = {}
    FakeWindow.props = {}
