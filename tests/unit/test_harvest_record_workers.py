import threading
import time
from unittest import TestCase
from unittest.mock import Mock, patch

from dcatus_validation.catalog import munge_title_to_name
from harvester.harvest import HarvestSource, Record, record_worker_count
from harvester.lib.harvest_timings import HarvestTimings


class FakeRecord:
    def __init__(self, n, log, tracker=None, error=None, key=None, delay=0.01):
        self.n = n
        self.log = log
        self.tracker = tracker
        self.error = error
        self.key = key if key is not None else f"record-{n}"
        self.delay = delay

    def slug_key(self):
        return self.key

    def harvest(self):
        if self.tracker:
            self.tracker.enter()
        try:
            time.sleep(self.delay)
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
        self.source._opensearch_lock = threading.Lock()
        self.source._opensearch_documents = []
        self.source._opensearch_queued_since = None
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

    def test_same_slug_records_keep_catalog_order(self):
        # the first of each title is the slowest; it must still finish first
        records = [
            FakeRecord(n, self.log, key=f"title-{n % 3}", delay=0.05 if n < 3 else 0)
            for n in range(12)
        ]

        with self.patch_workers("4"):
            self.source.harvest_records(iter(records))

        for title in range(3):
            order = [n for n, _ in self.log if n % 3 == title]
            self.assertEqual(order, sorted(order))
            threads = {name for n, name in self.log if n % 3 == title}
            self.assertEqual(len(threads), 1)

    def test_stopping_cancels_records_not_started(self):
        started = []

        class Stop(FakeRecord):
            def harvest(self):
                started.append(self.n)
                time.sleep(0.05)

        def records():
            for n in range(4):
                yield Stop(n, self.log)
            raise SystemExit(143)

        with self.patch_workers("2"), self.assertRaises(SystemExit):
            self.source.harvest_records(records())

        self.assertLess(len(started), 4)

    def patch_workers(self, value):
        return patch.dict("os.environ", {"HARVEST_RECORD_WORKERS": value})


class TestRecordWorkerCount(TestCase):
    def test_parses_and_bounds_the_setting(self):
        cases = {"4": 4, "0": 1, "-3": 1, "99": 16, " 2 ": 2}
        for value, expected in cases.items():
            with patch.dict("os.environ", {"HARVEST_RECORD_WORKERS": value}):
                self.assertEqual(record_worker_count(), expected, value)

    def test_bad_value_falls_back_to_one(self):
        with patch.dict("os.environ", {"HARVEST_RECORD_WORKERS": "four"}):
            with self.assertLogs("harvest_runner", level="WARNING"):
                self.assertEqual(record_worker_count(), 1)


class TestSlugKey(TestCase):
    def record(self, schema_type, source_raw, identifier="id-1"):
        source = Mock()
        source.schema_type = schema_type
        return Record(source, identifier, source_raw)

    def test_dcatus_title(self):
        record = self.record("dcatus3.0", '{"title": "Water Quality 2024"}')
        self.assertEqual(record.slug_key(), munge_title_to_name("Water Quality 2024"))

    def test_iso_title_from_xml(self):
        xml = (
            "<gmd:title>\n  <gco:CharacterString>Coastal Wetlands"
            "</gco:CharacterString></gmd:title>"
        )
        record = self.record("iso19115_2", xml)
        self.assertEqual(record.slug_key(), munge_title_to_name("Coastal Wetlands"))

    def test_same_slug_titles_share_a_key(self):
        a = self.record("dcatus3.0", '{"title": "Water Quality"}', "a")
        b = self.record("dcatus3.0", '{"title": "water  quality"}', "b")
        self.assertEqual(a.slug_key(), b.slug_key())

    def test_no_title_falls_back_to_identifier(self):
        for raw in [None, "not json", '{"description": "x"}', '{"title": ""}']:
            record = self.record("dcatus3.0", raw, "the-id")
            self.assertEqual(record.slug_key(), "identifier:the-id", raw)

    def test_iso_title_skips_citations_before_identification_info(self):
        xml = (
            "<gmd:referenceSystemInfo><gmd:authority><gmd:CI_Citation>"
            "<gmd:title><gco:CharacterString>North American Datum of 1983"
            "</gco:CharacterString></gmd:title></gmd:CI_Citation></gmd:authority>"
            "</gmd:referenceSystemInfo><gmd:identificationInfo>"
            "<gmd:MD_DataIdentification><gmd:citation><gmd:CI_Citation>"
            "<gmd:title><gco:CharacterString>Census Tracts 2026"
            "</gco:CharacterString></gmd:title>"
        )
        record = self.record("iso19115_2", xml)
        self.assertEqual(record.slug_key(), munge_title_to_name("Census Tracts 2026"))
