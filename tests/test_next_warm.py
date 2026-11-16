import tempfile
import unittest

import kodi_stubs
import next_warm

CONTEXT = {
    "media_id": "tt9:1:10", "imdb_id": "tt9", "tmdb_id": "1408",
    "season": 1, "episode": 10, "showtitle": "Show", "is_movie": False,
}
PLAYING = {"provider": "alldebrid", "provider_item_id": "777", "hash": "a" * 40}


class Player:
    def __init__(self, context=None, position=0.0, playing=True):
        self._playback_context = context or {}
        self._last_known_time = position
        self._current_candidate = PLAYING
        self.playing = playing

    def isPlayingVideo(self):
        return self.playing


class NextEpisodeWarmerTest(unittest.TestCase):
    def setUp(self):
        kodi_stubs.reset()
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.warmer = next_warm.NextEpisodeWarmer(self._dir.name)
        self.cleaned = []
        self.fetched = []
        self.candidates = [
            {"provider": "alldebrid", "provider_item_id": "777", "url": "https://x/e11.mkv", "name": "pack e11"},
            {"provider": "alldebrid", "provider_item_id": "888", "url": "https://x/other.mkv", "name": "single"},
        ]
        patches = {
            "next_episode_from_tmdb": lambda _c: {"season": 1, "episode": 11, "title": "Detox"},
            "cleanup_stream_candidate": lambda c: self.cleaned.append(c["provider_item_id"]),
            "fetch_all_cached_streams": self._fetch,
            "_playable": lambda candidates, _target: list(candidates),
        }
        for name, fake in patches.items():
            self.addCleanup(setattr, next_warm, name, getattr(next_warm, name))
            setattr(next_warm, name, fake)

    def _fetch(self, catalog_type, media_id, **_kwargs):
        self.fetched.append((catalog_type, media_id))
        return list(self.candidates)

    def _run(self, player):
        self.warmer.tick(player)
        if self.warmer._worker:
            self.warmer._worker.join(2)

    def test_looks_up_the_next_episode_once_after_two_minutes(self):
        self._run(Player(CONTEXT, position=30))
        self.assertEqual(self.fetched, [])
        self._run(Player(CONTEXT, position=130))
        self._run(Player(CONTEXT, position=400))
        self.assertEqual(self.fetched, [("series", "tt9:1:11")])
        cached = self.warmer._stream_cache().get("tt9:1:11")
        self.assertEqual(cached[0]["name"], "pack e11")

    def test_keeps_the_magnet_the_playing_episode_depends_on(self):
        self._run(Player(CONTEXT, position=130))
        self.assertEqual(self.cleaned, ["888"])

    def test_movies_and_disabled_setting_do_nothing(self):
        self._run(Player(dict(CONTEXT, is_movie=True), position=500))
        kodi_stubs.FakeAddon.settings[next_warm.SETTING_ID] = "false"
        self._run(Player(CONTEXT, position=500))
        self.assertEqual(self.fetched, [])

    def test_a_play_request_cancels_without_cleanup_or_save(self):
        def fetch_then_user_presses_play(catalog_type, media_id, cancel_event=None, **_k):
            cancel_event.set()
            return list(self.candidates)

        next_warm.fetch_all_cached_streams = fetch_then_user_presses_play
        self._run(Player(CONTEXT, position=130))
        self.assertEqual(self.cleaned, [])
        self.assertFalse(self.warmer._stream_cache().get("tt9:1:11"))

    def test_pending_playback_cancels_a_running_lookup(self):
        self.warmer._cancel.clear()
        kodi_stubs.FakeWindow.props[next_warm.PROP_PENDING_PLAYBACK] = "{}"
        self.warmer.tick(Player(CONTEXT, position=500))
        self.assertTrue(self.warmer._cancel.is_set())
        self.assertEqual(self.fetched, [])


if __name__ == "__main__":
    unittest.main()
