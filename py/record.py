import logging
logger = logging.getLogger("geolife.record")
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Float
from sqlalchemy import DateTime
from sqlalchemy import Date
from sqlalchemy import Time
from sqlalchemy import Sequence
from datetime import timedelta


DATETIME_STR_FORMAT = "%A, %d. %B %Y %I:%M:%S%p"


Base = declarative_base()
class GeoLifeRecord(Base):
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
      self.datetime.strftime(DATETIME_STR_FORMAT)
    )


class TimeModifiedGeoLifeRecord:
  def __init__(self, base_record, timestamp):
    self.user = base_record.user
    self.latitude = base_record.latitude
    self.longitude = base_record.longitude
    self.datetime = timestamp
    self.date = timestamp.date()
    self.time = timestamp.time()

  def __repr__(self):
    return "<GeoLifeRecord(name={0}, (x,y)=({1}, {2}), datetime={3})>".format(
      self.user, self.latitude, self.longitude,
      self.datetime.strftime(DATETIME_STR_FORMAT)
    )


def initialize_table(engine):
  Base.metadata.create_all(engine)


class LinkedRecords:
  """Linked-list style record collection"""
  def __init__(self, records=None):
    self.prev = None
    self.next = None
    self.record = None

    if records:
      self.add(records)

  def add(self, records, start=0):
    self.record = records[start]
    logger.info(self.record)

    if len(records) != start+1:
      self.next = LinkedRecords()
      self.next.prev = self
      self.next.add(records, start+1)

    else:
      logger.info("end of linked records")


  def getMinTimeDelta(self, current_min=timedelta.max):
    """Assume there is at least one more element in front of the current
    element."""
    
    delta = self.next.record.datetime - self.record.datetime
    if delta < current_min:
      logger.info(
        "New smaller time delta ({0} seconds) found between"
        " {1} and {2}".format(
        delta,
        self.record,
        self.next.record,
      ))
      current_min = delta

    if self.next.next is None:
      return current_min

    else:
      return self.next.getMinTimeDelta(current_min=current_min)


  def insertBefore(self, node):
    node.next = self
    node.prev = self.prev
    if self.prev is not None:
      self.prev.next = node
    self.prev = node
