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
from sqlalchemy import Boolean
from datetime import timedelta
from extent import RectangularExtent

Base = declarative_base()
class GeoLifeRecord(Base):
  __tablename__ = "records"
  id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
  user = Column(Integer)#, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  datetime = Column(DateTime)#, index=True)
  date = Column(Date)#, index=True)
  time = Column(Time)#, index=True)
  is_synthesized = Column(Boolean, default=False)

  def __repr__(self):
    return "<GeoLifeRecord(name={0}, (x,y)=({1}, {2}), datetime={3})>".format(
      self.user, self.latitude, self.longitude,
      self.datetime
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
    logger.debug("Adding record #{0} to linked list: {1}".format(
      start+1, self.record
    ))

    if len(records) != start+1:
      self.next = LinkedRecords()
      self.next.prev = self
      self.next.add(records, start+1)
      
      if self.next.extent is None:
        logger.debug("Initializing extents")
        self.extent = RectangularExtent(
          self.record.longitude,
          self.next.record.longitude,
          self.record.latitude,
          self.next.record.latitude,
        )

      else:
        self.extent = self.next.extent
        self.extent.addPoint(self.record.longitude, self.record.latitude)

    else:
      logger.debug("End of linked list. Total records: {0}".format(start+1))
      logger.debug("+"*80)
      self.extent = None

  def getMinTimeDelta(self, current_min=timedelta.max):
    """Assume there is at least one more element in front of the current
    element."""
    delta = self.next.record.datetime - self.record.datetime
    if delta < current_min:
      logger.info(
        "User #{0} has new smaller time delta ({1}) found between"
        "\n\t{2} and\n\t{3}".format(
        self.record.user,
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

  def getTimeDeltaWithNextNode(self):
    if self.next is None:
      raise "No future nodes exist beyond current node!"

    return self.next.record.datetime - self.record.datetime


def getExtent(x1, x2, y1, y2):
  return (min(x1, x2), max(x1, x2), min(y1, y2), max(y1, y2))
