import logging
logger = logging.getLogger("geolife.schema")

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import BigInteger
from sqlalchemy import Float
from sqlalchemy import Time
from sqlalchemy import SmallInteger

Base = declarative_base()

class HomogenizedRecord(Base):
  __tablename__ = "time_homogenized"
  id = Column(Integer, primary_key=True)
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

