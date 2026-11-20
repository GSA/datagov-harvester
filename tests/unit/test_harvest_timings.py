from unittest.mock import patch

import pytest

from harvester.lib.harvest_timings import HarvestTimings


@patch("harvester.lib.harvest_timings.time.perf_counter")
def test_time_sums_seconds_and_calls_per_step(perf_counter):
    perf_counter.side_effect = [0.0, 1.5, 10.0, 10.25, 20.0, 22.0]
    timings = HarvestTimings()

    with timings.time("sync"):
        pass
    with timings.time("sync"):
        pass
    with timings.time("validate"):
        pass

    assert timings.report() == {
        "sync": {"seconds": 1.75, "calls": 2},
        "validate": {"seconds": 2.0, "calls": 1},
    }
    assert timings.summary() == "sync=1.8s/2 validate=2.0s/1"


@patch("harvester.lib.harvest_timings.time.perf_counter")
def test_time_counts_steps_that_raise(perf_counter):
    perf_counter.side_effect = [0.0, 3.0]
    timings = HarvestTimings()

    with pytest.raises(ValueError):
        with timings.time("compare"):
            raise ValueError

    assert timings.report() == {"compare": {"seconds": 3.0, "calls": 1}}
