import logging
logger = logging.getLogger("geolife.db_visualizer")

from datetime import datetime
from datetime import timedelta
from record import GeoLifeRecord
from utils import datetimerange

from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

engine = create_engine(
  "{dialect}://{username}:{password}@{host}/{database}".format(
  dialect='mysql',
  username='kp',
  password='nope',
  host='localhost',
  database='geolife'
))
conn = engine.connect()
"""
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

# Determine the minimum and maximum dates in the database.
beginning = session.query(GeoLifeRecord)\
                   .order_by(GeoLifeRecord.datetime).first()
end = session.query(GeoLifeRecord)\
             .order_by(GeoLifeRecord.datetime.desc()).first()
print(beginning.datetime, beginning.date)
print(end.datetime, end.date)
"""

from sqlalchemy.sql import select

stmt = select([GeoLifeRecord.date]).distinct().order_by(GeoLifeRecord.date)
results = conn.execute(stmt)
for r, in results:
  print(r)
 
"""
    #.with_hint(GeoLifeRecord, 'USE INDEX ix_records_date')\
    self.result_set = self.result_set\
        .filter(GeoLifeRecord.date==date)\
        .order_by(GeoLifeRecord.datetime)
"""

