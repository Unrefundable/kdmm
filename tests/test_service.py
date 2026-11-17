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
        self.player._failed_start_checking = None
        self.player.upnext = None
        self.playing = False
        self.player.isPlaying = lambda: self.playing
        self.failed = []
        self.ran = threading.Event()
        original_wait = kodi_stubs.FakeMonitor.waitForAbort

        def wait_and_mark(monitor, timeout=0):
            self.ran.set()
            return original_wait(monitor, timeout)

        kodi_stubs.FakeMonitor.waitForAbort = wait_and_mark
        self.addCleanup(setattr, kodi_stubs.FakeMonitor, "waitForAbort", original_wait)
        def clear_failed(media_id, message="", failed_candidate=None, failed_url=""):
            # Like the real method, this ends the pending request.
            service.WIN.clearProperty(service.PROP_PENDING_PLAYBACK)
            self.failed.append((media_id, failed_url))

        self.player._clear_failed_stream = clear_failed

    def _settle(self):
        # Wait for the check itself, so a test cannot pass because it never ran.
        self.assertTrue(self.ran.wait(2), "failed-start check did not run")
        deadline = time.time() + 2
        while time.time() < deadline and self.player._failed_start_checking is not None:
            time.sleep(0.005)

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
        self.assertFalse(self.ran.wait(0.1))
        self.assertEqual(self.failed, [])

    def test_two_stop_callbacks_report_one_failure(self):
        self._pend()
        self.player._handle_playback_stop(is_ended=False)
        self.player._handle_playback_stop(is_ended=False)
        self._settle()
        time.sleep(0.05)
        self.assertEqual(len(self.failed), 1)

    def test_no_failure_when_playback_did_start(self):
        self._pend()
        self.playing = True
        self.player._handle_playback_stop(is_ended=False)
        self._settle()
        self.assertEqual(self.failed, [])

    def test_no_failure_when_a_newer_play_request_replaced_the_pending_one(self):
        self._pend()
        def swap(_self, _timeout=0):
            self._pend(media_id="tt1:1:3", url="https://example.invalid/c.mkv")
            self.ran.set()
            return False

        kodi_stubs.FakeMonitor.waitForAbort = swap
        self.player._handle_playback_stop(is_ended=False)
        self._settle()
        self.assertEqual(self.failed, [])



class StopHandlingTest(unittest.TestCase):
    def setUp(self):
        kodi_stubs.reset()
        self.player = service.BridgePlayer.__new__(service.BridgePlayer)
        p = self.player
        p._current_media_id = "tt1:1:4"
        p._current_url = None
        p._current_candidate = None
        p._tried_urls = set()
        p._playback_context = {"media_id": "tt1:1:4", "tmdb_id": "9", "season": 1, "episode": 4, "is_movie": False}
        p._was_fullscreen = False
        p._cleanup_threads = []
        p._failed_start_checking = None
        p._last_known_total = 2400.0
        self.saved = []
        p._progress_cache = type("Cache", (), {
            "set_progress": lambda _s, media_id, resume, total_time=0.0, watched=False:
                self.saved.append((media_id, resume, watched)),
        })()
        self.outcomes = []
        p.upnext = type("Memory", (), {
            "on_progress_saved": lambda _s, context, outcome, find_next: self.outcomes.append(outcome),
        })()
        self._next = service.get_next_episode
        service.get_next_episode = lambda _c: self.fail("must not auto-play after a dropped stream")
        self.addCleanup(setattr, service, "get_next_episode", self._next)

    def test_a_stream_that_drops_part_way_is_resumable_not_watched(self):
        self.player._last_known_time = 700.0
        self.player._handle_playback_stop(is_ended=True)
        self.assertEqual(self.saved, [("tt1:1:4", 700.0, False)])
        self.assertEqual(self.outcomes, ["resume"])

    def test_reaching_the_end_is_watched(self):
        service.get_next_episode = lambda _c: None
        self.player._last_known_time = 2395.0
        self.player._handle_playback_stop(is_ended=True)
        self.assertEqual(self.saved, [("tt1:1:4", 0.0, True)])
        self.assertEqual(self.outcomes, ["watched"])


class ReturnToDetailsTest(unittest.TestCase):
    """Which window Kodi returned to decides whether the title page opens."""

    EPISODE = {"tmdb_id": "9", "season": 1, "episode": 4, "is_movie": False}

    def setUp(self):
        kodi_stubs.reset()
        self.player = service.BridgePlayer.__new__(service.BridgePlayer)
        self.player.isPlayingVideo = lambda: False
        self.visible = set()
        self._cond = service.xbmc.getCondVisibility
        service.xbmc.getCondVisibility = lambda cond: cond in self.visible
        self.addCleanup(setattr, service.xbmc, "getCondVisibility", self._cond)
        self.opened = []
        self._open = service.open_details
        service.open_details = self.opened.append
        self.addCleanup(setattr, service, "open_details", self._open)

    def _return(self, context):
        threads = set(threading.enumerate())
        self.player._return_to_details(context)
        for thread in set(threading.enumerate()) - threads:
            thread.join(2)

    def test_the_title_page_opens_over_the_home_screen(self):
        self._return(self.EPISODE)
        self.assertEqual(self.opened, [self.EPISODE])

    def test_the_title_page_stays_closed_over_the_episode_list(self):
        self.visible.add(service.EPISODE_LIST_VISIBLE)
        self._return(self.EPISODE)
        self.assertEqual(self.opened, [])

    def test_a_movie_still_opens_its_page(self):
        self.visible.add(service.EPISODE_LIST_VISIBLE)
        movie = {"tmdb_id": "5", "is_movie": True}
        self._return(movie)
        self.assertEqual(self.opened, [movie])


class PrepareOnPlayRequestTest(unittest.TestCase):
    """A play request has its title page built before the stream starts."""

    REQUEST = {"tmdb_id": "9", "imdb_id": "tt1", "is_movie": False}

    def setUp(self):
        kodi_stubs.reset()
        self.player = service.BridgePlayer.__new__(service.BridgePlayer)
        self.prepared = []
        self._prepare = service.prepare_details
        service.prepare_details = self.prepared.append
        self.addCleanup(setattr, service, "prepare_details", self._prepare)

    def test_the_requested_title_is_prepared_once(self):
        service.WIN.setProperty(service.PROP_PREPARE_DETAILS, json.dumps(self.REQUEST))
        self.player._prepare_requested_details()
        self.player._prepare_requested_details()
        self.assertEqual(self.prepared, [self.REQUEST])

    def test_nothing_is_prepared_without_a_request(self):
        self.player._prepare_requested_details()
        self.assertEqual(self.prepared, [])

    def test_nothing_is_prepared_when_the_page_is_switched_off(self):
        kodi_stubs.FakeAddon.settings["return_to_details"] = "false"
        service.WIN.setProperty(service.PROP_PREPARE_DETAILS, json.dumps(self.REQUEST))
        self.player._prepare_requested_details()
        self.assertEqual(self.prepared, [])
        self.assertEqual(service.WIN.getProperty(service.PROP_PREPARE_DETAILS), "")


class PreparedDetailsTest(unittest.TestCase):
    """The title page is built during playback and shown without a rebuild."""

    SHOW = {"tmdb_id": "9", "is_movie": False}

    def setUp(self):
        kodi_stubs.reset()
        import details_page
        self.details = details_page
        details_page._prepared = None
        self.built = []
        self._import = details_page._import_get_listitem
        details_page._import_get_listitem = lambda: self._get_listitem
        self.addCleanup(setattr, details_page, "_import_get_listitem", self._import)
        self.shown = []
        dialog = type("Dialog", (), {"info": lambda _s, item: self.shown.append(item)})
        self._dialog = details_page.xbmcgui.Dialog
        details_page.xbmcgui.Dialog = dialog
        self.addCleanup(setattr, details_page.xbmcgui, "Dialog", self._dialog)

    def _get_listitem(self, path):
        self.built.append(path)
        return f"item {len(self.built)}"

    def _wait_prepared(self):
        self.assertTrue(self.details._prepared.done.wait(2))

    def test_stopping_shows_the_page_built_during_playback(self):
        self.assertTrue(self.details.prepare_details(self.SHOW))
        self._wait_prepared()
        self.assertTrue(self.details.open_details(self.SHOW))
        self.assertEqual(self.shown, ["item 1"])
        self.assertEqual(len(self.built), 1)

    def test_preparing_twice_builds_once(self):
        self.details.prepare_details(self.SHOW)
        self.details.prepare_details(self.SHOW)
        self._wait_prepared()
        self.assertEqual(len(self.built), 1)

    def test_a_page_prepared_for_another_title_is_not_shown(self):
        self.details.prepare_details({"tmdb_id": "7", "is_movie": True})
        self._wait_prepared()
        self.details.open_details(self.SHOW)
        self.assertEqual(self.shown, ["item 2"])
        self.assertIn("tmdb_id=9", self.built[1])

    def test_without_a_prepared_page_it_is_built_on_stop(self):
        self.assertTrue(self.details.open_details(self.SHOW))
        self.assertEqual(self.shown, ["item 1"])

    def test_a_prepared_page_is_shown_only_once(self):
        self.details.prepare_details(self.SHOW)
        self._wait_prepared()
        self.details.open_details(self.SHOW)
        self.details.open_details(self.SHOW)
        self.assertEqual(self.shown, ["item 1", "item 2"])

    def test_a_context_without_ids_is_not_prepared(self):
        self.assertFalse(self.details.prepare_details({"is_movie": True}))
        self.assertEqual(self.built, [])


class SafeUrlTest(unittest.TestCase):
    def test_the_link_path_stays_out_of_the_log(self):
        text = service._safe_url("https://c5.debrid.it/dl/SECRETTOKEN/House.S01E10.mkv")
        self.assertNotIn("SECRETTOKEN", text)
        self.assertIn("House.S01E10.mkv", text)
        self.assertIn("c5.debrid.it", text)


class SegmentLookupRetryTest(unittest.TestCase):
    def setUp(self):
        kodi_stubs.reset()
        self.answers = []
        self.calls = 0
        self._query = service.query_all_segments
        self._overlay = service.show_skip_overlay
        service.show_skip_overlay = lambda **_k: False

        def fake_query(**_k):
            self.calls += 1
            return self.answers.pop(0)

        service.query_all_segments = fake_query
        self.addCleanup(setattr, service, "query_all_segments", self._query)
        self.addCleanup(setattr, service, "show_skip_overlay", self._overlay)
        self.player = FakeBridgePlayer()
        self.player._last_known_time = 5.0
        self.controller = service.SegmentController()

    def tick(self):
        self.controller.tick(self.player, kodi_stubs.FakeMonitor())

    def test_no_answer_is_asked_again_after_a_pause_not_every_tick(self):
        self.answers = [None, {"intro": [{"start": 0.0, "end": 30.0}]}]
        self.tick()
        self.tick()
        self.assertEqual(self.calls, 1)
        self.controller._segment_retry_at = 0
        self.tick()
        self.assertEqual(self.calls, 2)
        self.assertIn("intro", self.controller._segments)

    def test_gives_up_after_the_last_attempt(self):
        self.answers = [None] * service.SEGMENT_LOOKUP_MAX_ATTEMPTS
        for _ in range(service.SEGMENT_LOOKUP_MAX_ATTEMPTS + 2):
            self.controller._segment_retry_at = 0
            self.tick()
        self.assertEqual(self.calls, service.SEGMENT_LOOKUP_MAX_ATTEMPTS)
        self.assertEqual(self.controller._segments, {})

    def test_a_real_empty_answer_is_final(self):
        self.answers = [{}]
        self.tick()
        self.tick()
        self.assertEqual(self.calls, 1)

    def test_playback_stopping_mid_tick_is_not_an_error(self):
        def gone():
            raise RuntimeError("Kodi is not playing any file")

        self.player.getPlayingFile = gone
        self.tick()
        self.assertEqual(self.calls, 0)


if __name__ == "__main__":
    unittest.main()
