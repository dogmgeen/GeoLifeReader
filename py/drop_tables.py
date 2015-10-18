from raw.record import GeoLifeUser
from raw.record import RawRecord
from schema import HomogenizedRecord
from schema import HomogenizedGeoLifeUser
from sqlalchemy import create_engine
from config import getEngine

engine = getEngine()
GeoLifeUser.__table__.drop(engine, checkfirst=True)
engine.execute("DROP VIEW day_records_view;")
RawRecord.__table__.drop(engine, checkfirst=True)
HomogenizedGeoLifeUser.__table__.drop(engine, checkfirst=True)
HomogenizedRecord.__table__.drop(engine, checkfirst=True)
