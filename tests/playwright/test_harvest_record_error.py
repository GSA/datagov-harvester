import pytest


@pytest.fixture()
def ures(unauthed_page):
    # Cards group by record, so individual error ids are no longer shown in
    # the UI (#6476) — use the known fixture id for test_identifier-1's
    # first ValidationException row (see tests/generate_fixtures.py).
    res = unauthed_page.goto(
        "/api/v1/harvest_error/3ccb48db-21fc-427a-9ec7-36b0d0f621d0"
    )
    yield res


class TestHarvestRecordErrorUnauthed:
    def test_response(self, ures):
        res = ures.json()
        fixture_dict = {
            # The job page shows only this job's own errors (#6461).
            "harvest_job_id": "6bce761c-7a39-41c1-ac73-94234c139c76",
            "harvest_record_id": "0779c855-df20-49c8-9108-66359d82b77c",
            "message": "record is invalid",
            "type": "ValidationException",
        }
        for key in fixture_dict.keys():
            assert fixture_dict[key] == res[key]

    def test_iso(self, unauthed_page):
        unauthed_page.goto("/harvest_job/8c90626b-09c6-4098-8828-76c7b981639a")

        # Card heading is now the record identifier; "Title:" is the first
        # <p> in the card (was nth(1) when the heading used to be blank/id).
        assert (
            unauthed_page.locator(
                "#error_results_pagination .error-list .error-block:first-child p"
            )
            .nth(0)
            .inner_text()
        ) == "Title: Test ISO19115 title"


class TestHarvestRecordErrorAuthed:
    pass
