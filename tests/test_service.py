import json
import threading
import time
import unittest

import kodi_stubs
import service


class FakeBridgePlayer:
    """Just enough of BridgePlayer for SegmentController.tick()."""

    def __init__(self):
        self._playback_context = {"media_id": "tt1:1:1", "is_movie": True}
        self._last_known_time = 0.0
        self._last_known_total = 3000.0
        self.seeks = []

    def isPlayingVideo(self):
        return True

    def isPlaying(self):
        return True

    def getPlayingFile(self):
        return "https://example.invalid/a.mkv|User-Agent=x"

    def seekTime(self, target):
        self.seeks.append(target)


class SegmentControllerSkipTest(unittest.TestCase):
    def setUp(self):
        kodi_stubs.reset()
        self.shown = []
        self.press = True
        self._overlay = service.show_skip_overlay
        self._query = service.query_all_segments

        def fake_overlay(segment_end=None, player=None, monitor=None, segment_type="intro"):
            self.shown.append(segment_type)
            return self.press

        service.show_skip_overlay = fake_overlay
        service.query_all_segments = lambda **_k: {"intro": [{"start": 60.0, "end": 120.0}]}
        self.addCleanup(setattr, service, "show_skip_overlay", self._overlay)
        self.addCleanup(setattr, service, "query_all_segments", self._query)
        self.player = FakeBridgePlayer()
        self.controller = service.SegmentController()

    def tick(self, position):
        self.player._last_known_time = position
        self.controller.tick(self.player, kodi_stubs.FakeMonitor())

    def test_pressing_skip_seeks_past_the_segment(self):
        self.tick(61.0)
        self.assertEqual(self.shown, ["intro"])
        self.assertEqual(self.player.seeks, [122.0])

    def test_button_does_not_return_while_kodi_still_reports_the_old_position(self):
        self.tick(61.0)
        # After the seek Kodi reports the new position, then briefly the old
        # one again: that used to look like re-entering the intro.
        self.tick(122.0)
        self.tick(63.0)
        self.tick(122.5)
        self.assertEqual(self.shown, ["intro"])

    def test_button_returns_after_seeking_back_before_the_segment(self):
        self.tick(61.0)
        self.tick(122.0)
        self.tick(30.0)
        self.tick(61.0)
        self.assertEqual(self.shown, ["intro", "intro"])

    def test_an_overlapping_segment_of_the_same_type_is_skipped_with_it(self):
        service.query_all_segments = lambda **_k: {
            "intro": [{"start": 0.0, "end": 23.0}, {"start": 0.0, "end": 24.0}],
        }
        self.tick(5.0)
        # The seek lands on a keyframe just before the second intro's end.
        self.tick(23.5)
        self.tick(26.0)
        self.assertEqual(self.shown, ["intro"])
        self.assertEqual(self.player.seeks, [25.0])

    def test_ignoring_the_button_does_not_suppress_it_for_a_later_visit(self):
        self.press = False
        self.tick(61.0)
        self.tick(200.0)
        self.tick(61.0)
        self.assertEqual(self.shown, ["intro", "intro"])
        self.assertEqual(self.player.seeks, [])


class FailedStartTest(unittest.TestCase):
    def setUp(self):
        kodi_stubs.reset()
        self._seconds = service.FAILED_START_CHECK_SECONDS
        service.FAILED_START_CHECK_SECONDS = 0
        self.addCleanup(setattr, service, "FAILED_START_CHECK_SECONDS", self._seconds)
        self.player = service.BridgePlayer.__new__(service.BridgePlayer)
        self.player._current_media_id = None
        self.player._tried_urls = set()
        self.player._playback_context = {}
        self.player._current_candidate = None
        self.player._cleanup_threads = []
        self.playing = False
        self.player.isPlaying = lambda: self.playing
        self.failed = []
        self.player._clear_failed_stream = (
            lambda media_id, message="", failed_candidate=None, failed_url="":
            self.failed.append((media_id, failed_url))
        )

    def _settle(self):
        deadline = time.time() + 2
        while time.time() < deadline and threading.active_count() > 1:
            time.sleep(0.01)

    def _pend(self, media_id="tt1:1:2", url="https://example.invalid/b.mkv|h=1"):
        service.WIN.setProperty(service.PROP_PENDING_PLAYBACK, json.dumps({
            "media_id": media_id, "url": url, "timestamp": time.time(),
        }))

    def test_stop_without_a_start_reports_the_failure_quickly(self):
        self._pend()
        self.player._handle_playback_stop(is_ended=False)
        self._settle()
        self.assertEqual(self.failed, [("tt1:1:2", "https://example.invalid/b.mkv")])
        self.assertIn("https://example.invalid/b.mkv", self.player._tried_urls)

    def test_stop_of_other_content_with_nothing_pending_is_ignored(self):
        self.player._handle_playback_stop(is_ended=False)
        self._settle()
        self.assertEqual(self.failed, [])

    def test_no_failure_when_playback_did_start(self):
        self._pend()
        self.playing = True
        self.player._handle_playback_stop(is_ended=False)
        self._settle()
        self.assertEqual(self.failed, [])

    def test_no_failure_when_a_newer_play_request_replaced_the_pending_one(self):
        self._pend()
        original = kodi_stubs.FakeMonitor.waitForAbort

        def swap(_self, _timeout=0):
            self._pend(media_id="tt1:1:3", url="https://example.invalid/c.mkv")
            return False

        kodi_stubs.FakeMonitor.waitForAbort = swap
        self.addCleanup(setattr, kodi_stubs.FakeMonitor, "waitForAbort", original)
        self.player._handle_playback_stop(is_ended=False)
        self._settle()
        self.assertEqual(self.failed, [])


if __name__ == "__main__":
    unittest.main()
