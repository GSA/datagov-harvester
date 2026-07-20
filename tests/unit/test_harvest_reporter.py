from harvester.lib.harvest_reporter import HarvestReporter


class TestHarvestReporter:
    def test_warned_counter_round_trip(self):
        """The "warned" action increments the warned counter and surfaces as
        records_warned in the report dict."""
        reporter = HarvestReporter()
        assert reporter.warned == 0
        assert reporter.report()["records_warned"] == 0

        reporter.update("warned")
        reporter.update("warned")

        assert reporter.warned == 2
        assert reporter.report()["records_warned"] == 2

    def test_warned_excluded_from_processed_count(self):
        """warned overlaps validated/errored and must not inflate progress
        math (mirrors validated, which is also excluded)."""
        reporter = HarvestReporter()
        reporter.update("warned")

        assert reporter.processed_count == 0
