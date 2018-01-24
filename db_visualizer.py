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

results = session.query(func.count(GeoLifeRecord.date), GeoLifeRecord.date)\
                 .group_by(GeoLifeRecord.date).order_by(GeoLifeRecord.date)
with open("date_histogram.csv", "w") as f:
  for n, d in results:
    f.write("{0};{1}\n".format(n, d))

