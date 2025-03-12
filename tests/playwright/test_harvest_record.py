import pytest
from playwright.sync_api import expect
import json


@pytest.fixture()
def ures(unauthed_page):
    res = unauthed_page.goto("/harvest_record/0779c855-df20-49c8-9108-66359d82b77c")
    yield res


class TestHarvestRecordUnauthed:
    def test_response(self, ures):
        res = ures.json()
        fixture_dict = {
            "action": "create",
            "harvest_job_id": "6bce761c-7a39-41c1-ac73-94234c139c76",
            "harvest_source_id": "2f2652de-91df-4c63-8b53-bfced20b276b",
            "id": "0779c855-df20-49c8-9108-66359d82b77c",
            "identifier": "test_identifier-1",
            "source_raw": '{"title": "test-0", "identifier": "test-0"}',
            "status": "error",
        }
        for key in fixture_dict.keys():
            assert fixture_dict[key] == res[key]


class TestHarvestRecordAuthed:
    pass
