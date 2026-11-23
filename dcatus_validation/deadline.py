"""A time budget for one validator request, so a catalog that would take too
long to check gets an answer before CloudFront's 30s origin timeout cuts the
request off with a bare 504.

Gunicorn runs requests in worker threads, which can't be interrupted from
outside, so the work checks the budget itself between steps. With no budget
set (the harvest runner, tests), every check passes.
"""

import time
from contextlib import contextmanager
from contextvars import ContextVar


class ValidationTimedOut(Exception):
    """The request ran past its validation_deadline."""


_deadline: ContextVar[float | None] = ContextVar("validation_deadline", default=None)


@contextmanager
def validation_deadline(seconds: float):
    """Budget the work inside the block to `seconds` from now."""
    token = _deadline.set(time.monotonic() + seconds)
    try:
        yield
    finally:
        _deadline.reset(token)


def check_deadline(expected_seconds: float = 0.0) -> None:
    """Raise ValidationTimedOut if the budget is spent, or would be by a step
    expected to take `expected_seconds`, which can't be interrupted once
    started."""
    deadline = _deadline.get()
    if deadline is not None and time.monotonic() + expected_seconds > deadline:
        raise ValidationTimedOut
