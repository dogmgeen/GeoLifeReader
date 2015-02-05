import logging
logging.basicConfig(
  level=logging.DEBUG,
  filename='/tmp/sqlite2mysql.log',
  filemode='w'
)
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.INFO)
logger.addHandler(stdout)

import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import record

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Float
from sqlalchemy import DateTime
from sqlalchemy import Date
from sqlalchemy import Time
from sqlalchemy import Sequence
from sqlalchemy import Boolean
Base = declarative_base()
class OldGeoLifeRecord(Base):
  __tablename__ = "records"
  id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
  user = Column(Integer, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  datetime = Column(DateTime, index=True)
  date = Column(Date, index=True)
  time = Column(Time, index=True)

  def __repr__(self):
    return "<GeoLifeRecord(name={0}, (x,y)=({1}, {2}), datetime={3})>".format(
      self.user, self.latitude, self.longitude,
      self.datetime
    )

from sqlalchemy.sql import select
def get_unique_users(conn):
  s = select([OldGeoLifeRecord.user]).distinct()
  return [r for r, in conn.execute(s)]


if __name__ == "__main__":
  try:
    sqlite_file = "d793830ab93515e3505abf56035a6753.db" 
    sqlite_engine = create_engine('sqlite:///{0}'.format(sqlite_file))

    """
    mysql_engine = create_engine(
      "{dialect}://{username}:{password}@{host}/{database}".format(
      dialect='mysql',
      username='kp',
      password='nope',
      host='localhost',
      database='geolife'
    ))
    record.initialize_table(mysql_engine)
    """

    SQLiteSession = sessionmaker(bind=sqlite_engine)
    sqlite_session = SQLiteSession()
    result_set = sqlite_session.query(OldGeoLifeRecord)
    for r in result_set:
      print(r)

    """
    MySQLSession  = sessionmaker(bind=mysql_engine)
    mysql_session = MySQLSession()
    print(mysql_session)
    """
    
    conn = sqlite_engine.connect()
    print("Unique users: {0}".format(get_unique_users(conn)))

    """
      mysql_session.add(record.GeoLifeRecord(
        user=r.user,
        latitude=r.latitude,
        longitude=r.longitude,
        datetime=r.datetime,
        date=r.date,
        time=r.time,
        is_synthesized=False,
      ))"""

 
  except:
    logger.exception("Stuff didn't do")
