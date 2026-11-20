import time
from collections import defaultdict
from contextlib import contextmanager


class HarvestTimings:
    """Wall-clock seconds spent in each harvest step, summed over a job.

    Steps can nest; a dotted name such as "sync.opensearch_index" is time
    already counted inside its parent "sync".
    """

    def __init__(self):
        self._seconds = defaultdict(float)
        self._calls = defaultdict(int)

    @contextmanager
    def time(self, step: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            self._seconds[step] += time.perf_counter() - start
            self._calls[step] += 1

    def report(self) -> dict:
        return {
            step: {"seconds": round(self._seconds[step], 3), "calls": self._calls[step]}
            for step in sorted(self._seconds)
        }

    def summary(self) -> str:
        return " ".join(
            f"{step}={self._seconds[step]:.1f}s/{self._calls[step]}"
            for step in sorted(self._seconds)
        )
