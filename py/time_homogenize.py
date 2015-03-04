import logging
logger = logging.getLogger("geolife.record")
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import BigInteger
from sqlalchemy import Float
from sqlalchemy import Time
from sqlalchemy import Sequence
from sqlalchemy import SmallInteger
from datetime import timedelta
from datetime import time
from utils import datetimerange
from raw.record import GeoLifeUser

WEEKDAY_STRINGS = [
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
  "Sunday",
]

Base = declarative_base()
class HomogenizedRecord(Base):
  __tablename__ = "time_homogenized"
  id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
  user = Column(BigInteger)#, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  time = Column(Time)
  weekday = Column(SmallInteger)

  MONDAY = 0
  TUESDAY = 1
  WEDNESDAY = 2
  THURSDAY = 3
  FRIDAY = 4
  SATURDAY = 5
  SUNDAY = 6
  WEEKDAYS = [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY]

  def __repr__(self):
    return "<WeekdayRecord(name={0}, (x,y)=({1}, {2}), datetime={3})>".format(
      self.user, self.latitude, self.longitude,
      self.datetime
    )

class HomogenizedGeoLifeUser(Base):
  __tablename__ = "time_homo_user_weekday_count"
  id = Column(BigInteger, primary_key=True)
  count = Column(Integer)


def initialize_table(engine):
  Base.metadata.create_all(engine)


from sqlalchemy import create_engine
engine = create_engine(
  "{dialect}://{username}:{password}@{host}/{database}".format(
  dialect='postgresql+psycopg2',
  username='postgres',
  password='nope27rola',
  host='localhost',
  database='geolife'
))

from sqlalchemy.orm import sessionmaker
Session = sessionmaker()
Session.configure(bind=engine)

def get_users_present_on(weekday):
  session = Session()
  result_set = session.query(GeoLifeUser.id).filter(GeoLifeUser.weekday == weekday).all()
  users = []
  for u, in result_set:
    users.append(u)

  session.close()
  return users


if __name__ == "__main__":
  for weekday in HomogenizedRecord.WEEKDAYS:
    users = get_users_present_on(weekday)
    print(users)
    users_present = []
    homogenized_records = []
    """
    for t in datetimerange(time.min, tim.max, delta):
      records = SELECT * FROM records
              WHERE datetime in (t, t+delta)
      for r in records:
        users_present.append(r.user)
        r = modify_record_timestamp(r, t)
        homogenized_records.append(r)

      users_not_present = users - users_present
      for u in users_not_present:
        most_recent_record = most_recent_user_record_before(t, u)
        r = modify_record_timestamp(most_recent_record, t)
        homogenized_records.append(r)

      insert_into_time_homogenized_database(homogenized_records)

      del users_present[:]
      del homogenized_records[:]
    """
