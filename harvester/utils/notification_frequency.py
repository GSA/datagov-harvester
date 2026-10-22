"""Shared notification-frequency guidance for refresh links."""

from typing import NamedTuple


class NotificationFrequencyNotice(NamedTuple):
    value: str
    before_always: str
    after_always: str

    @property
    def text(self):
        return (
            f"Notification frequency: {self.value}. "
            f"{self.before_always}Always{self.after_always}"
        )


def notification_frequency_notice(frequency):
    """Return the same concise notice for the page and both email versions."""
    labels = {
        "on_error": "On error",
        "on_error_or_update": "On error or update",
    }
    value = labels.get(frequency)
    if value is None:
        return None
    return NotificationFrequencyNotice(
        value=value,
        before_always="Ask the Data.gov team to set it to ",
        after_always=" to receive a new link after every finished harvest job.",
    )
