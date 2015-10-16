import logging
logger = logging.getLogger("geolife.record")
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import BigInteger
from sqlalchemy import Float
from sqlalchemy import Time
from sqlalchemy import SmallInteger
from sqlalchemy import Date
from datetime import timedelta
import random

Base = declarative_base()
class RawRecord(Base):
  __tablename__ = "raw_records"
  id = Column(Integer, primary_key=True)
  user = Column(BigInteger)#, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  time = Column(Time)#, index=True)
  date = Column(Date)

  def __repr__(self):
    return "<RawRecord(name={0}, (x,y)=({1}, {2}), time={3})>".format(
      self.user, self.latitude, self.longitude,
      self.time
    )

class GeoLifeUser(Base):
  __tablename__ = "users"
  id = Column(BigInteger, primary_key=True)
  count = Column(Integer)


def initialize_table(engine):
  Base.metadata.create_all(engine)

def getUserSubset(n, session, randomize=False):
  records = session.query(GeoLifeUser.id).filter(
    GeoLifeUser.weekday == weekday
  ).order_by(GeoLifeUser.id).all()
  users = [r for r, in records]
  if n is None:
    return users

  if randomize:
    return random.sample(users, n)

  else:
    return users[:n]

