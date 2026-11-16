import tempfile
import unittest

import kodi_stubs
import upnext

PROPS = kodi_stubs.FakeWindow


class UpNextMemoryTest(unittest.TestCase):
    def setUp(self):
        kodi_stubs.reset()
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.memory = upnext.UpNextMemory(self._dir.name)
        self._read = upnext.read_helper_list
        self.addCleanup(setattr, upnext, "read_helper_list", self._read)
        self._poll = upnext.LEARN_POLL_SECONDS
        upnext.LEARN_POLL_SECONDS = 0
        self.addCleanup(setattr, upnext, "LEARN_POLL_SECONDS", self._poll)

    def test_remembered_show_is_published_when_its_page_opens(self):
        upnext.read_helper_list = lambda _tmdb: "closed"
        self.memory.remember("1408", 1, 11)
        self.assertTrue(self.memory.handle_notification("upnext_1408"))
        self.assertEqual(PROPS.props[upnext.PROP_TMDB], "1408")
        self.assertEqual(PROPS.props[upnext.PROP_SEASON], "1")
        self.assertEqual(PROPS.props[upnext.PROP_EPISODE], "11")
        self.assertEqual(PROPS.props[upnext.PROP_RESUMABLE], "")
        self.assertIn("tmdb_id=1408&season=1&episode=11", PROPS.props[upnext.PROP_PATH])

    def test_unknown_show_clears_what_the_last_page_published(self):
        upnext.read_helper_list = lambda _tmdb: "closed"
        self.memory.remember("1408", 1, 11)
        self.memory.handle_notification("upnext_1408")
        self.memory.handle_notification("upnext_73375")
        self.assertNotIn(upnext.PROP_TMDB, PROPS.props)
        self.assertNotIn(upnext.PROP_SEASON, PROPS.props)

    def test_other_notifications_are_left_for_the_seek_handler(self):
        self.assertFalse(self.memory.handle_notification("seek_forward"))

    def test_memory_survives_a_restart(self):
        self.memory.remember("1408", 2, 3, resumable=True)
        again = upnext.UpNextMemory(self._dir.name)
        entry = again.get("1408")
        self.assertEqual((entry["season"], entry["episode"], entry["resumable"]), (2, 3, True))

    def test_bad_values_are_not_remembered(self):
        self.assertFalse(self.memory.remember("", 1, 1))
        self.assertFalse(self.memory.remember("1408", 1, 0))
        self.assertFalse(self.memory.remember("1408", None, 4))
        self.assertIsNone(self.memory.get("1408"))

    def test_the_helpers_answer_is_learned_and_shown_on_the_open_page(self):
        answers = iter(["loading", "loading", {"season": 4, "episode": 5, "resumable": True, "path": "plugin://x"}])
        upnext.read_helper_list = lambda _tmdb: next(answers)
        self.memory.remember("73375", 4, 4)
        self.memory._requested = "73375"
        self.memory.publish("73375")
        self.memory._learn("73375", self.memory._learner_cancel)
        entry = self.memory.get("73375")
        self.assertEqual((entry["season"], entry["episode"], entry["resumable"], entry["path"]), (4, 5, True, "plugin://x"))
        self.assertEqual(PROPS.props[upnext.PROP_EPISODE], "5")
        self.assertEqual(PROPS.props[upnext.PROP_RESUMABLE], "1")

    def test_nothing_next_forgets_the_show(self):
        upnext.read_helper_list = lambda _tmdb: "empty"
        self._timeout = upnext.LEARN_TIMEOUT_SECONDS
        upnext.LEARN_TIMEOUT_SECONDS = 0
        self.addCleanup(setattr, upnext, "LEARN_TIMEOUT_SECONDS", self._timeout)
        self.memory.remember("1408", 8, 22)
        self.memory._requested = "1408"
        self.memory._learn("1408", self.memory._learner_cancel)
        self.assertIsNone(self.memory.get("1408"))
        self.assertNotIn(upnext.PROP_TMDB, PROPS.props)

    def test_stopping_part_way_makes_that_episode_the_one_to_resume(self):
        context = {"tmdb_id": "1408", "season": 1, "episode": 10, "is_movie": False}
        self.memory.on_progress_saved(context, "resume", lambda _c: self.fail("no lookup needed"))
        entry = self.memory.get("1408")
        self.assertEqual((entry["season"], entry["episode"], entry["resumable"]), (1, 10, True))

    def test_finishing_an_episode_makes_the_following_one_next(self):
        context = {"tmdb_id": "1408", "season": 1, "episode": 10, "is_movie": False}
        self.memory.remember("1408", 1, 10, resumable=True)
        thread = self.memory.on_progress_saved(context, "watched", lambda _c: {"season": 1, "episode": 11})
        thread.join(2)
        entry = self.memory.get("1408")
        self.assertEqual((entry["season"], entry["episode"], entry["resumable"]), (1, 11, False))

    def test_finishing_the_last_episode_leaves_nothing_remembered(self):
        context = {"tmdb_id": "1408", "season": 8, "episode": 22, "is_movie": False}
        self.memory.remember("1408", 8, 22, resumable=True)
        self.memory.on_progress_saved(context, "watched", lambda _c: None).join(2)
        self.assertIsNone(self.memory.get("1408"))

    def test_movies_are_ignored(self):
        self.assertIsNone(self.memory.on_progress_saved({"tmdb_id": "5", "is_movie": True}, "resume", None))
        self.assertIsNone(self.memory.get("5"))

    def test_oldest_shows_are_dropped_past_the_limit(self):
        self._max = upnext.MAX_SHOWS
        upnext.MAX_SHOWS = 3
        self.addCleanup(setattr, upnext, "MAX_SHOWS", self._max)
        for index, tmdb in enumerate(("1", "2", "3", "4")):
            self.memory.remember(tmdb, 1, 1)
            self.memory._shows[tmdb]["updated"] = index
        self.memory.remember("5", 1, 1)
        self.assertNotIn("1", self.memory._shows)
        self.assertLessEqual(len(self.memory._shows), 3)


if __name__ == "__main__":
    unittest.main()
