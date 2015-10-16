import logging
logger = logging.getLogger("geolife.schema")

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import BigInteger
from sqlalchemy import Float
from sqlalchemy import Time
from sqlalchemy import SmallInteger
from sqlalchemy import Table
from sqlalchemy import distinct
import random
from sqlalchemy import MetaData
metadata = MetaData()

import config
RecordsOnOneDay = Table(
  'day_records_view', metadata, autoload=True, autoload_with=config.getEngine()
)


Base = declarative_base()
class HomogenizedRecord(Base):
  __tablename__ = "time_homogenized"
  id = Column(Integer, primary_key=True)
  user = Column(BigInteger)#, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  time = Column(Time)

  def __repr__(self):
    return "<HomogenizedTimeRecord(name={0}, (x,y)=({1}, {2}), time={3})>".format(
      self.user, self.latitude, self.longitude,
      self.time
    )

class HomogenizedGeoLifeUser(Base):
  __tablename__ = "time_homo_user_weekday_count"
  id = Column(BigInteger, primary_key=True)
  count = Column(Integer)


def initialize_table(engine):
  Base.metadata.create_all(engine)


def get_users(session):
  query = session.query(distinct(RecordsOnOneDay.c.new_user_id))
  result_set = query.all()

  users = set()
  for u, in result_set:
    users.add(u)

  return users


def getUserSubset(n, session):
  users = get_users(session)
  if n is None:
    return users
  else:
    return random.sample(users, n)

