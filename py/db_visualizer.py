import logging
logger = logging.getLogger("geolife.db_visualizer")

from record import GeoLifeRecord

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import func

engine = create_engine(
  "{dialect}://{username}:{password}@{host}/{database}".format(
  dialect='mysql',
  username='kp',
  password='nope',
  host='localhost',
  database='geolife'
))

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

results = session.query(GeoLifeRecord.date).distinct().order_by(GeoLifeRecord.date)

for d, in results:
    print(d)

