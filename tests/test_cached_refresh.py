import unittest

import kodi_stubs  # noqa: F401  (installs the xbmc stubs)
import dmm

HASH = "a" * 40


class AllDebridCachedRefreshTest(unittest.TestCase):
    """A cached AllDebrid source that is no longer ready must lead to a new
    search (CachedStreamUnavailable), not a failed play."""

    def setUp(self):
        self.deleted = []
        for name in ("_ad_key", "_ad_post", "_ad_delete_magnet"):
            self.addCleanup(setattr, dmm, name, getattr(dmm, name))
        dmm._ad_key = lambda: "key"
        dmm._ad_delete_magnet = lambda magnet_id, _token: self.deleted.append(magnet_id)

    def _candidate(self):
        return {"provider": dmm._PROVIDER_AD, "hash": HASH, "name": "Show.S01E01.mkv"}

    def test_a_source_being_downloaded_again_asks_for_a_new_search(self):
        dmm._ad_post = lambda path, *_a, **_k: {
            "magnets": [{"id": 77, "ready": False}],
        }
        with self.assertRaises(dmm.CachedStreamUnavailable):
            dmm.refresh_cached_stream(self._candidate(), season=1, episode=1)
        # The download KDMM just started on AllDebrid is removed again.
        self.assertEqual(self.deleted, [77])

    def test_a_file_missing_from_the_magnet_asks_for_a_new_search(self):
        def post(path, *_a, **_k):
            if path == "/magnet/upload":
                return {"magnets": [{"id": 78, "ready": True}]}
            return {"magnets": [{"files": [{"n": "other.nfo", "s": 10, "l": "x"}]}]}

        dmm._ad_post = post
        with self.assertRaises(dmm.CachedStreamUnavailable):
            dmm.refresh_cached_stream(self._candidate(), season=1, episode=1)
        self.assertEqual(self.deleted, [78])

    def test_an_upload_error_still_fails_the_play(self):
        dmm._ad_post = lambda path, *_a, **_k: {
            "magnets": [{"error": {"code": "MAGNET_INVALID_URI", "message": "bad"}}],
        }
        with self.assertRaises(RuntimeError) as caught:
            dmm.refresh_cached_stream(self._candidate(), season=1, episode=1)
        self.assertNotIsInstance(caught.exception, dmm.CachedStreamUnavailable)


if __name__ == "__main__":
    unittest.main()
