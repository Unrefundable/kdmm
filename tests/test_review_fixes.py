import json
import tempfile
import unittest
from pathlib import Path

import kodi_stubs
import cache
import dmm
import next_episode
import next_warm
import remote


class ProgressCacheTest(unittest.TestCase):
    def test_an_older_copy_does_not_restore_what_another_copy_cleared(self):
        with tempfile.TemporaryDirectory() as folder:
            service_copy = cache.ProgressCache(folder)
            service_copy.set_progress("tt1", 100.0, total_time=1000.0)
            # The plugin clears tt1 through its own instance.
            path = Path(folder) / "progress_cache.json"
            data = json.loads(path.read_text())
            del data["tt1"]
            path.write_text(json.dumps(data))
            service_copy.set_progress("tt2", 50.0, total_time=1000.0)
            self.assertEqual(sorted(json.loads(path.read_text())), ["tt2"])


class NextEpisodeTest(unittest.TestCase):
    def test_api_key_is_removed_from_error_text(self):
        text = next_episode._without_api_key(
            Exception("404 for url: https://api.themoviedb.org/3/tv/1?api_key=abc123&x=1")
        )
        self.assertNotIn("abc123", text)
        self.assertIn("x=1", text)

    def test_only_aired_episodes_count(self):
        self.assertTrue(next_episode._has_aired({"air_date": "2020-01-01"}, today="2026-11-16"))
        self.assertTrue(next_episode._has_aired({"air_date": "2026-11-16"}, today="2026-11-16"))
        self.assertFalse(next_episode._has_aired({"air_date": "2027-01-01"}, today="2026-11-16"))
        self.assertFalse(next_episode._has_aired({"air_date": None}, today="2026-11-16"))

    def test_an_unaired_next_episode_means_nothing_is_next(self):
        original = next_episode._tmdb_get_json
        self.addCleanup(setattr, next_episode, "_tmdb_get_json", original)

        def fake(path, _key):
            if path == "tv/9":
                return {"seasons": [{"season_number": 1}]}
            return {"episodes": [
                {"episode_number": 1, "air_date": "2020-01-01", "name": "One"},
                {"episode_number": 2, "air_date": "2999-01-01", "name": "Two"},
            ]}

        next_episode._tmdb_get_json = fake
        context = {"tmdb_id": "9", "imdb_id": "tt9", "season": 1, "episode": 1}
        self.assertIsNone(next_episode.next_episode_from_tmdb(context))


class RangeIgnoredTest(unittest.TestCase):
    def test_a_full_response_is_not_taken_for_the_end_of_the_file(self):
        class Response:
            status_code = 200

            def iter_content(self, chunk_size=0):
                yield b"x" * 100

            def close(self):
                pass

        class Session:
            def get(self, *_a, **_k):
                return Response()

        original = dmm._get_session
        self.addCleanup(setattr, dmm, "_get_session", original)
        dmm._get_session = lambda: Session()
        self.assertEqual(dmm._fetch_url_sample("https://x/y.mkv", start=5000, size=100), b"")
        self.assertEqual(dmm._fetch_url_sample("https://x/y.mkv", start=0, size=100), b"x" * 100)


class SeekAccumulatorTest(unittest.TestCase):
    def test_presses_add_up_and_commit_once(self):
        seeks = []

        class Player:
            def isPlayingVideo(self):
                return True

            def getTime(self):
                return 100.0

            def getTotalTime(self):
                return 5000.0

            def seekTime(self, target):
                seeks.append(target)

        accumulator = remote.SeekAccumulator()
        accumulator.press(remote.SEEK_FORWARD)
        accumulator.press(remote.SEEK_FORWARD)
        accumulator._last_press = 0
        accumulator.tick(Player())
        accumulator.tick(Player())
        self.assertEqual(seeks, [100.0 + 2 * remote.BIG_SEEK_STEP_SECONDS])


class NextWarmUnknownSourceTest(unittest.TestCase):
    def test_nothing_is_released_when_the_playing_source_is_unknown(self):
        kodi_stubs.reset()
        cleaned = []
        patches = {
            "next_episode_from_tmdb": lambda _c: {"season": 1, "episode": 2, "title": ""},
            "cleanup_stream_candidate": lambda c: cleaned.append(c),
            "fetch_all_cached_streams": lambda *_a, **_k: [
                {"provider": "alldebrid", "provider_item_id": "1", "url": "https://x/a.mkv", "name": "a"},
            ],
            "_playable": lambda candidates, _t: list(candidates),
        }
        for name, fake in patches.items():
            self.addCleanup(setattr, next_warm, name, getattr(next_warm, name))
            setattr(next_warm, name, fake)
        import threading
        with tempfile.TemporaryDirectory() as folder:
            warmer = next_warm.NextEpisodeWarmer(folder)
            context = {"imdb_id": "tt9", "tmdb_id": "9", "season": 1, "episode": 1, "showtitle": "S"}
            self.assertTrue(warmer._warm_next(context, None, threading.Event()))
        self.assertEqual(cleaned, [])


if __name__ == "__main__":
    unittest.main()
