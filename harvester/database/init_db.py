from sqlalchemy import create_engine
from harvester.database.models import Base, HarvestSource, HarvestJob, HarvestError
import os
from dotenv import load_dotenv
from sqlalchemy.engine.reflection import Inspector

def create_tables():
    load_dotenv()
    DATABASE_URI=os.getenv("DATABASE_URI")
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    inspector = Inspector.from_engine(engine)
    table_names = inspector.get_table_names()
    return (f"Created database tables : {table_names}")



