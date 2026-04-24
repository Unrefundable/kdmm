import os
import threading
import time

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs


ADDON = xbmcaddon.Addon()
ADDON_PATH = ADDON.getAddonInfo("path")
OVERLAY_RES = "1080i"
BUTTON_ID = 3001
BG_IMAGE_SHADOW = 3003
BG_IMAGE_FILL = 3004
ACTION_SELECT = 7
ACTION_PREVIOUS_MENU = 10
ACTION_BACK = 92
POLL_INTERVAL = 0.5
DISPLAY_DURATION = 5.0
DISPLAY_DURATION_BY_SEGMENT = {
    "next_episode": 25.0,
}


def _texture_path():
    return xbmcvfs.translatePath(os.path.join(
        ADDON_PATH, "resources", "skins", "default", OVERLAY_RES, "rounded_rect.png"
    ))


class SkipOverlay(xbmcgui.WindowXMLDialog):
    def __new__(cls, xml_file, addon_path, skin, res,
                callback=None, segment_end=None, player=None, monitor=None, segment_type="intro"):
        return super(SkipOverlay, cls).__new__(cls, xml_file, addon_path, skin, res)

    def __init__(self, xml_file, addon_path, skin, res,
                 callback=None, segment_end=None, player=None, monitor=None, segment_type="intro"):
        super(SkipOverlay, self).__init__(xml_file, addon_path, skin, res)
        self._callback = callback
        self._segment_end = segment_end
        self._player = player
        self._monitor = monitor
        self._segment_type = segment_type
        self._skip_pressed = False
        self._closed = False
        self._lock = threading.Lock()
        self._display_deadline = None

    @property
    def skip_pressed(self):
        return self._skip_pressed

    def _button_text(self):
        mapping = {
            "intro": "Skip Intro",
            "recap": "Skip Recap",
            "credits": "Skip Credits",
            "preview": "Skip Preview",
            "next_episode": "Play Next Episode",
        }
        return mapping.get(self._segment_type, "Skip")

    def onInit(self):
        monitor = self._monitor if self._monitor is not None else xbmc.Monitor()
        if monitor.abortRequested():
            self._dismiss_main_thread()
            return

        try:
            texture = _texture_path()
            self.getControl(BG_IMAGE_SHADOW).setImage(texture)
            self.getControl(BG_IMAGE_FILL).setImage(texture)
            self.getControl(BUTTON_ID).setLabel(self._button_text())
        except Exception as exc:
            xbmc.log(f"[KDMM Overlay] init failed: {exc}", xbmc.LOGWARNING)

        try:
            self.setFocusId(BUTTON_ID)
        except Exception:
            pass

        if self._segment_end is not None and self._player is not None:
            duration = DISPLAY_DURATION_BY_SEGMENT.get(self._segment_type, DISPLAY_DURATION)
            self._display_deadline = time.time() + duration
            thread = threading.Thread(target=self._poll_loop)
            thread.daemon = True
            thread.start()

    def onClick(self, control_id):
        if control_id == BUTTON_ID:
            self._do_press()

    def onAction(self, action):
        action_id = action.getId()
        if action_id == ACTION_SELECT:
            try:
                if self.getFocusId() == BUTTON_ID:
                    self._do_press()
            except Exception:
                pass
            return
        if action_id in (ACTION_PREVIOUS_MENU, ACTION_BACK):
            self._dismiss_main_thread()

    def _do_press(self):
        with self._lock:
            if self._closed:
                return
            self._skip_pressed = True
            self._closed = True
        if self._callback:
            try:
                self._callback()
            except Exception:
                pass
        self._dismiss_main_thread()

    def _poll_loop(self):
        monitor = self._monitor if self._monitor is not None else xbmc.Monitor()
        while True:
            with self._lock:
                if self._closed:
                    return
            if monitor.abortRequested() or monitor.waitForAbort(POLL_INTERVAL):
                self._close_from_thread()
                return
            try:
                if self._display_deadline is not None and time.time() >= self._display_deadline:
                    self._close_from_thread()
                    return
                player = self._player
                if player and player.isPlaying():
                    if player.getTime() >= self._segment_end:
                        self._close_from_thread()
                        return
                elif player and not player.isPlaying():
                    self._close_from_thread()
                    return
            except Exception:
                pass

    def _close_from_thread(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            self.close()
        except Exception:
            pass

    def _dismiss_main_thread(self):
        with self._lock:
            self._closed = True
        try:
            self.close()
        except Exception:
            pass


def show_skip_overlay(callback=None, segment_end=None, player=None, monitor=None, segment_type="intro"):
    monitor = monitor if monitor is not None else xbmc.Monitor()
    if monitor.abortRequested():
        return False

    try:
        window = SkipOverlay(
            "overlay.xml",
            ADDON_PATH,
            "default",
            OVERLAY_RES,
            callback=callback,
            segment_end=segment_end,
            player=player,
            monitor=monitor,
            segment_type=segment_type,
        )
        window.doModal()
        pressed = window.skip_pressed
        del window
        return pressed
    except Exception as exc:
        xbmc.log(f"[KDMM Overlay] error: {exc}", xbmc.LOGERROR)
        return False
