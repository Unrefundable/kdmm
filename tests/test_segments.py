import unittest

import kodi_stubs
import introdb_client as client


def seg(start_ms, end_ms, confidence=0.5):
    return {"start_ms": start_ms, "end_ms": end_ms, "confidence": confidence, "submission_count": 1}


class PickBestSegmentsTest(unittest.TestCase):
    def test_near_duplicate_intros_from_two_sources_become_one(self):
        # Real data for one episode: TheIntroDB said 0-24 s, IntroDB 0-23 s.
        picked = client._pick_best_segments_all_types(
            [seg(None, 24000, 0.7), seg(0, 23000, 1.0)], "intro",
        )
        self.assertEqual([(s["start"], s["end"]) for s in picked], [(0.0, 23.0)])

    def test_overlapping_credits_keep_the_better_scored_one(self):
        picked = client._pick_best_segments_all_types(
            [seg(678000, None, 0.6), seg(701490, 727180, 1.0)], "credits",
        )
        self.assertEqual([(s["start"], s["end"]) for s in picked], [(701.49, 727.18)])

    def test_separate_segments_of_one_type_both_survive(self):
        picked = client._pick_best_segments_all_types(
            [seg(0, 30000, 0.9), seg(120000, 180000, 0.8)], "intro",
        )
        self.assertEqual(sorted((s["start"], s["end"]) for s in picked), [(0.0, 30.0), (120.0, 180.0)])

    def test_back_to_back_segments_are_not_overlapping(self):
        picked = client._pick_best_segments_all_types(
            [seg(0, 30000, 0.9), seg(30000, 60000, 0.8)], "recap",
        )
        self.assertEqual(len(picked), 2)

    def test_invalid_segments_are_dropped(self):
        picked = client._pick_best_segments_all_types(
            [seg(None, None), seg(5000, 5000), "junk", seg(0, 10000)], "intro",
        )
        self.assertEqual([(s["start"], s["end"]) for s in picked], [(0.0, 10.0)])


    def test_malformed_numbers_drop_the_segment_instead_of_raising(self):
        bad = [
            {"start_ms": 0, "end_ms": 20000, "confidence": "high"},
            {"start_ms": 0, "end_ms": 21000, "confidence": 0.9, "submission_count": None},
            {"start_ms": "soon", "end_ms": 22000},
        ]
        picked = client._pick_best_segments_all_types(bad, "intro")
        self.assertEqual([(s["start"], s["end"]) for s in picked], [(0.0, 21.0)])


class QueryAllSegmentsTest(unittest.TestCase):
    def setUp(self):
        kodi_stubs.reset()
        kodi_stubs.FakeAddon.settings["segment_lookups_enabled"] = "true"
        self._request = client._do_request
        self._wait = client._wait_rate_limit
        client._wait_rate_limit = lambda: True
        self.addCleanup(setattr, client, "_do_request", self._request)
        self.addCleanup(setattr, client, "_wait_rate_limit", self._wait)

    def query(self):
        return client.query_all_segments(tmdb_id="1408", imdb_id="tt0412142", season=1, episode=2)

    def test_none_when_no_service_answered(self):
        client._do_request = lambda url, key: None
        self.assertIsNone(self.query())

    def test_none_while_rate_limited(self):
        client._wait_rate_limit = lambda: False
        self.assertIsNone(self.query())

    def test_empty_when_a_service_answered_with_nothing(self):
        client._do_request = lambda url, key: {}
        self.assertEqual(self.query(), {})

    def test_one_answer_is_enough(self):
        answers = iter([None, None, {"intro": {"start_ms": 0, "end_ms": 23000, "confidence": 1}}])
        client._do_request = lambda url, key: next(answers)
        self.assertEqual([s["end"] for s in self.query()["intro"]], [23.0])


if __name__ == "__main__":
    unittest.main()
