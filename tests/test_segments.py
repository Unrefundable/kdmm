import unittest

import kodi_stubs  # noqa: F401
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


if __name__ == "__main__":
    unittest.main()
