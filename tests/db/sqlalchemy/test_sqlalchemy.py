from sqlalchemy.orm import close_all_sessions

from harvester.db.models import Base
from harvester.db.models.models import (HarvestError, HarvestJob,
                                        HarvestRecord, HarvestSource,
                                        create_sqlalchemy_engine,
                                        create_sqlalchemy_session)


def test_add_tables():
    engine = create_sqlalchemy_engine()
    Base.metadata.create_all(engine)


def test_add_harvest_source(create_test_harvest_source):
    engine = create_sqlalchemy_engine()
    session = create_sqlalchemy_session(engine)

    harvest_source = HarvestSource(**create_test_harvest_source)

    session.add(harvest_source)
    session.commit()

    for item in session.query(HarvestSource).all():
        assert item

    close_all_sessions()


def test_add_harvest_job(create_test_harvest_job):
    engine = create_sqlalchemy_engine()
    session = create_sqlalchemy_session(engine)

    harvest_job = HarvestJob(**create_test_harvest_job)

    session.add(harvest_job)
    session.commit()

    for item in session.query(HarvestJob).all():
        assert item

    close_all_sessions()


def test_add_harvest_record(create_test_harvest_record):
    engine = create_sqlalchemy_engine()
    session = create_sqlalchemy_session(engine)

    harvest_record = HarvestRecord(**create_test_harvest_record)

    session.add(harvest_record)
    session.commit()

    for item in session.query(HarvestRecord).all():
        assert item

    close_all_sessions()


def test_add_harvest_error(create_test_harvest_error):
    engine = create_sqlalchemy_engine()
    session = create_sqlalchemy_session(engine)

    harvest_error = HarvestError(**create_test_harvest_error)

    session.add(harvest_error)
    session.commit()

    for item in session.query(HarvestError).all():
        assert item

    close_all_sessions()
