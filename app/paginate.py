import math

from database.interface import PAGINATE_ENTRIES_PER_PAGE


class Pagination:
    def __init__(
        self, current: int = 1, count: int = 1, per_page=PAGINATE_ENTRIES_PER_PAGE
    ):
        self.current = current
        self.count = count
        self.page_count = math.ceil(count / per_page)
        self.per_page = per_page

    @property
    def db_current(self):
        return self.current - 1

    def to_dict(self):
        return {
            "current": self.current,
            "db_current": self.db_current,
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
