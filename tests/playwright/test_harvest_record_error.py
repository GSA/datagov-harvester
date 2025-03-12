import pytest


@pytest.fixture()
def ures(unauthed_page):
    unauthed_page.goto("/harvest_job/6bce761c-7a39-41c1-ac73-94234c139c76")
    err_id = unauthed_page.locator(
        "#error_results_pagination .error-list .error-block:first-child h3"
    ).inner_text()
    res = unauthed_page.goto(f"/harvest_error/{err_id}")
    yield res


class TestHarvestRecordErrorUnauthed:
    def test_response(self, ures):
        res = ures.json()
        fixture_dict = {
            "harvest_job_id": "6bce761c-7a39-41c1-ac73-94234c139c76",
            "harvest_record_id": "0779c855-df20-49c8-9108-66359d82b77c",
            "message": "record is invalid",
            "type": "ValidationException",
        }
        for key in fixture_dict.keys():
            assert fixture_dict[key] == res[key]


class TestHarvestRecordErrorAuthed:
    pass
