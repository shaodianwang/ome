#!/usr/bin/env python3

import threading
import time
import unittest

import batch


class ConcurrentCaseRunnerTest(unittest.TestCase):
    def test_limits_sum_of_active_case_peaks(self):
        peaks = {"a": 21, "b": 21, "c": 21, "d": 29, "e": 8}
        lock = threading.Lock()
        active_pods = 0
        max_active_pods = 0
        recorded = []

        def runner(case_id):
            nonlocal active_pods, max_active_pods
            with lock:
                active_pods += peaks[case_id]
                max_active_pods = max(max_active_pods, active_pods)
            time.sleep(0.02)
            with lock:
                active_pods -= peaks[case_id]
            return case_id, 0, 0.02

        batch.run_concurrent_cases(
            list(peaks), 4, 64, peaks,
            lambda case_id, _rc, _secs: recorded.append(case_id),
            runner=runner,
        )

        self.assertCountEqual(recorded, peaks)
        self.assertLessEqual(max_active_pods, 64)
        self.assertGreater(max_active_pods, max(peaks.values()))

    def test_canary_pd_peaks_are_budgeted_from_headers(self):
        self.assertEqual(batch.case_peak_pods("p309"), 21)
        self.assertEqual(batch.case_peak_pods("p354"), 29)


if __name__ == "__main__":
    unittest.main()
