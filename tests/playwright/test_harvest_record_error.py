import pytest
from playwright.sync_api import expect
import json


@pytest.fixture()
def ures(unauthed_page):
    res = unauthed_page.goto("/harvest_error/04728898-237d-483a-be8c-b395bb21d199")
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
