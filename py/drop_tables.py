from raw.record import GeoLifeUser
from raw.record import WRecord
from sqlalchemy import create_engine
from schema import HomogenizedGeoLifeUser
from schema import HomogenizedRecord

engine = create_engine(
  "{dialect}://{username}:{password}@{host}/{database}".format(
  dialect='postgresql+psycopg2',
  username='postgres',
  password='nope27rola',
  host='localhost',
  database='geolife'
))

GeoLifeUser.__table__.drop(engine, checkfirst=True)
WRecord.__table__.drop(engine, checkfirst=True)
HomogenizedRecord.__table__.drop(engine, checkfirst=True)
HomogenizedGeoLifeUser.__table__.drop(engine, checkfirst=True)
