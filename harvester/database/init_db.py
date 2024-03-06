from sqlalchemy import create_engine
from harvester.database.models import Base
from sqlalchemy.engine.reflection import Inspector
from . import DATABASE_URI

def create_tables():
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    inspector = Inspector.from_engine(engine)
    table_names = inspector.get_table_names()
    return (f"Database tables : {table_names}")



