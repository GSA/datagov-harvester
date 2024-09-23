import math

from database.interface import PAGINATE_ENTRIES_PER_PAGE


class Pagination:
    def __init__(self, current: int = 1, count: int = 1):
        self.current = current
        self.count = count
        self.page_count = math.ceil(count / PAGINATE_ENTRIES_PER_PAGE)
        self.per_page = PAGINATE_ENTRIES_PER_PAGE

    def to_dict(self):
        return {
            "current": self.current,
            "count": self.count,
            "page_count": self.page_count,
            "page_label": "Page",
            "per_page": self.per_page,
            "next": {"label": "Next"},
            "previous": {"label": "Previous"},
            "last_item": {
                "label": "Last page",
            },
        }

    def update_current(self, current: int) -> dict:
        self.current = int(current)
