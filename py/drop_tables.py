from raw.record import GeoLifeUser
from raw.record import WRecord
from sqlalchemy import create_engine
from schema import HomogenizedGeoLifeUser
from schema import HomogenizedRecord
from config import getEngine

engine = getEngine()
GeoLifeUser.__table__.drop(engine, checkfirst=True)
WRecord.__table__.drop(engine, checkfirst=True)
HomogenizedRecord.__table__.drop(engine, checkfirst=True)
HomogenizedGeoLifeUser.__table__.drop(engine, checkfirst=True)
