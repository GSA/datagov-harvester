import pytest
from bs4 import BeautifulSoup
from flask import render_template

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


class TestPaginationNumbersTemplate:
    @staticmethod
    def rendered_pages(app, current, page_count):
        htmx_vars = {
            "target_div": "#results",
            "endpoint_url": "/harvest_job/job-1",
            "page_param": "page",
        }
        pagination = Pagination(
            current=current, count=page_count * PAGINATE_ENTRIES_PER_PAGE
        )
        with app.test_request_context("/"):
            html = render_template(
                "components/pagination/pagination.html",
                pagination=pagination.to_dict(),
                htmx_vars=htmx_vars,
                data={"htmx_vars": htmx_vars},
            )
        items = BeautifulSoup(html, "html.parser").select(
            ".usa-pagination__page-no, .usa-pagination__overflow"
        )
        return [item.get_text(strip=True) for item in items]

    @pytest.mark.parametrize(
        "current,page_count,expected",
        [
            (5, 19, ["1", "…", "4", "5", "6", "…", "19"]),
            (22, 50, ["1", "…", "21", "22", "23", "…", "50"]),
            (15, 19, ["1", "…", "14", "15", "16", "…", "19"]),
        ],
    )
    def test_middle_page_shows_previous_and_next_page(
        self, app, current, page_count, expected
    ):
        assert self.rendered_pages(app, current, page_count) == expected

    def test_page_near_start_shows_first_five_pages(self, app):
        assert self.rendered_pages(app, 4, 19) == ["1", "2", "3", "4", "5", "…", "19"]

    def test_page_near_end_shows_last_five_pages(self, app):
        assert self.rendered_pages(app, 16, 19) == [
            "1",
            "…",
            "15",
            "16",
            "17",
            "18",
            "19",
        ]

    def test_few_pages_shows_every_page(self, app):
        assert self.rendered_pages(app, 3, 6) == ["1", "2", "3", "4", "5", "6"]
