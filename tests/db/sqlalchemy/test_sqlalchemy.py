import pytest
from harvester.db.models.models import (
    HarvestSource,
    HarvestJob,
    HarvestRecord,
    HarvestError,
)
import uuid
from datetime import datetime

MODELS = [HarvestSource, HarvestJob, HarvestRecord, HarvestError]


@pytest.mark.parametrize(
    "model, test_record",
    zip(
        MODELS,
        [
            "test_harvest_source",
            "test_harvest_job",
            "test_harvest_record",
            "test_harvest_error",
        ],
    ),
)
def test_insert_record(session, model, test_record, request):
    test_record = request.getfixturevalue(test_record)
    session.add(model(**test_record))
    session.commit()

    row = session.query(model).filter(model.id == test_record["id"]).first()
    assert row


@pytest.mark.parametrize(
    "model, new_info",
    zip(
        MODELS[:-1],
        [
            "test_update_source",
            "test_update_job",
            "test_update_record",
        ],
    ),
)
def test_update_record(session, model, new_info, request):
    new_info = request.getfixturevalue(new_info)
    session.query(model).filter(model.id == new_info["id"]).update(new_info)

    row = session.query(model).filter(model.id == new_info["id"]).first()

    new_info["id"] = uuid.UUID(new_info["id"])
    row = {
        k: v.replace(tzinfo=None) if type(v) == datetime else v
        for k, v in row.__dict__.items()
    }
    assert new_info.items() <= row.items()
