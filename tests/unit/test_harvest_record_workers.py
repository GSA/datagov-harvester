import threading
import time
from unittest import TestCase
from unittest.mock import Mock, patch

from harvester.harvest import HarvestSource
from harvester.lib.harvest_timings import HarvestTimings


class FakeRecord:
    def __init__(self, n, log, tracker=None, error=None):
        self.n = n
        self.log = log
        self.tracker = tracker
        self.error = error

    def harvest(self):
        if self.tracker:
            self.tracker.enter()
        try:
            time.sleep(0.01)
            if self.error:
                raise self.error
            self.log.append((self.n, threading.current_thread().name))
        finally:
            if self.tracker:
                self.tracker.exit()


class ConcurrencyTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0

    def enter(self):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)

    def exit(self):
        with self.lock:
            self.active -= 1


class TestHarvestRecordWorkers(TestCase):
    def setUp(self):
        self.source = object.__new__(HarvestSource)
        self.source.timings = HarvestTimings()
        self.source._db_interface = Mock()
        self.source._opensearch = None
        self.source._opensearch_initialized = True
        self.log = []

    def test_single_worker_runs_in_order_on_calling_thread(self):
        records = [FakeRecord(n, self.log) for n in range(5)]

        with self.patch_workers("1"):
            self.source.harvest_records(iter(records))

        self.assertEqual([n for n, _ in self.log], [0, 1, 2, 3, 4])
        self.assertEqual({name for _, name in self.log}, {"MainThread"})
        self.source.db_interface.db.remove.assert_not_called()

    def test_workers_process_every_record_concurrently(self):
        tracker = ConcurrencyTracker()
        records = [FakeRecord(n, self.log, tracker) for n in range(40)]

        with self.patch_workers("4"):
            self.source.harvest_records(iter(records))

        self.assertEqual(sorted(n for n, _ in self.log), list(range(40)))
        self.assertGreater(tracker.peak, 1)
        self.assertLessEqual(tracker.peak, 4)
        self.assertEqual(self.source.db_interface.db.remove.call_count, 40)

    def test_queued_records_are_capped(self):
        pulled = []

        def records():
            for n in range(20):
                pulled.append(n)
                # the generator never runs more than workers * 2 ahead
                self.assertLessEqual(len(pulled) - len(self.log), 2 * 2 + 1)
                yield FakeRecord(n, self.log)

        with self.patch_workers("2"):
            self.source.harvest_records(records())

        self.assertEqual(len(self.log), 20)

    def test_worker_error_reaches_the_caller(self):
        records = [
            FakeRecord(0, self.log),
            FakeRecord(1, self.log, error=ValueError("boom")),
        ]

        with self.patch_workers("2"), self.assertRaises(ValueError):
            self.source.harvest_records(iter(records))

    def patch_workers(self, value):
        return patch.dict("os.environ", {"HARVEST_RECORD_WORKERS": value})
