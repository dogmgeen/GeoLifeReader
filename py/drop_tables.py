from raw.record import GeoLifeUser
from raw.record import RawRecord
from sqlalchemy import create_engine
from config import getEngine

engine = getEngine()
GeoLifeUser.__table__.drop(engine, checkfirst=True)
RawRecord.__table__.drop(engine, checkfirst=True)
