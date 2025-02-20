from app.paginate import Pagination
from database.interface import PAGINATE_ENTRIES_PER_PAGE


class TestPagination:
    def test_return_defaults(self):
        pagination = Pagination()
        assert pagination.current == 1
        assert pagination.db_current == 0
        assert pagination.count == 1
        assert pagination.page_count == 1
        assert pagination.per_page == PAGINATE_ENTRIES_PER_PAGE

    def test_return_default_dict(self):
        pagination = Pagination()

        expected = {
            "current": 1,
            "db_current": 0,
            "count": 1,
            "page_count": 1,
            "page_label": "Page",
            "per_page": PAGINATE_ENTRIES_PER_PAGE,
            "next": {"label": "Next"},
            "previous": {"label": "Previous"},
            "last_item": {"label": "Last page"},
        }
        assert expected == pagination.to_dict()

    def test_update_current(self):
        pagination = Pagination()
        assert pagination.current == 1
        pagination.current = 12
        assert pagination.current == 12
        assert pagination.db_current == 11

    def test_change_pagination_val(self):
        pagination = Pagination(count=40, per_page=7)
        assert pagination.count == 40
        assert pagination.page_count == 6
