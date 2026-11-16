import unittest

import kodi_stubs  # noqa: F401
import dmm

URL = "https://example.invalid/episode.mkv"


class SampleCacheTest(unittest.TestCase):
    def setUp(self):
        self.fetches = []
        self.lengths = 0
        self._fetch = dmm._fetch_url_sample
        self._length = dmm._fetch_remote_content_length

        def fake_fetch(url, headers=None, start=0, size=0, timeout=4):
            self.fetches.append((start, size))
            return bytes((start + i) % 251 for i in range(size))

        def fake_length(url, headers=None, timeout=4):
            self.lengths += 1
            return 10_000

        dmm._fetch_url_sample = fake_fetch
        dmm._fetch_remote_content_length = fake_length
        dmm._forget_url_samples(URL)
        self.addCleanup(setattr, dmm, "_fetch_url_sample", self._fetch)
        self.addCleanup(setattr, dmm, "_fetch_remote_content_length", self._length)
        self.addCleanup(dmm._forget_url_samples, URL)

    def test_same_range_is_downloaded_once(self):
        first = dmm._read_url_sample(URL, start=0, size=400)
        second = dmm._read_url_sample(URL, start=0, size=400)
        self.assertEqual(first, second)
        self.assertEqual(self.fetches, [(0, 400)])

    def test_smaller_range_inside_a_fetched_one_is_sliced(self):
        whole = dmm._read_url_sample(URL, start=100, size=400)
        part = dmm._read_url_sample(URL, start=200, size=100)
        self.assertEqual(part, whole[100:200])
        self.assertEqual(self.fetches, [(100, 400)])

    def test_range_outside_the_fetched_one_is_downloaded(self):
        dmm._read_url_sample(URL, start=0, size=200)
        dmm._read_url_sample(URL, start=100, size=200)
        self.assertEqual(self.fetches, [(0, 200), (100, 200)])

    def test_length_is_asked_once_and_forgetting_clears_everything(self):
        self.assertEqual(dmm._remote_content_length(URL), 10_000)
        self.assertEqual(dmm._remote_content_length(URL), 10_000)
        self.assertEqual(self.lengths, 1)
        dmm._read_url_sample(URL, start=0, size=50)
        dmm._forget_url_samples(URL)
        dmm._read_url_sample(URL, start=0, size=50)
        dmm._remote_content_length(URL)
        self.assertEqual(self.fetches, [(0, 50), (0, 50)])
        self.assertEqual(self.lengths, 2)

    def test_short_read_is_not_cached(self):
        dmm._fetch_url_sample = lambda *a, **k: (self.fetches.append("short"), b"abc")[1]
        dmm._read_url_sample(URL, start=0, size=50)
        dmm._read_url_sample(URL, start=0, size=50)
        self.assertEqual(self.fetches, ["short", "short"])


if __name__ == "__main__":
    unittest.main()
