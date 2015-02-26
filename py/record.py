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
from sqlalchemy import SmallInteger
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


class WRecord(Base):
  __tablename__ = "beep"
  id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
  user = Column(Integer)#, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  datetime = Column(DateTime)#, index=True)
  weekday = Column(SmallInteger)

  def __repr__(self):
    return "<WeekdayRecord(name={0}, (x,y)=({1}, {2}), datetime={3})>".format(
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
    self.extent = None

    if records is not None:
      # Create the linked list, with the end node having None as the next
      current = self
      for i in range(len(records)):
        logger.debug("Linking record #{0} of {1}: {2}".format(
          i, len(records), records[i],
        ))
        current.record = records[i]
        n = LinkedRecords()
        current.next = n
        n.prev = current
        current = n
      logger.info("Linking of {0} records complete!".format(len(records)))
      current.prev.next = None

      # Create the extents. The last node will have no extent, but the second
      #  to last node will include the extent of itself and the next node.
      current = current.prev.prev
      current.extent = RectangularExtent(
        current.record.longitude, current.next.record.longitude,
        current.record.latitude, current.next.record.latitude,
      )
      while current.prev is not None:
        current = current.prev
        current.extent = current.next.extent
        current.extent.addPoint(
          current.record.longitude, current.record.latitude
        )
      logger.debug("Finished setting up extents") 

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


  def insertBefore(self, record):
    n = LinkedRecords()
    n.next = self
    n.prev = self.prev
    if self.prev is not None:
      self.prev.next = n
    self.prev = n
    n.record = record

  def insertAfter(self, record):
    n = LinkedRecords()
    n.prev = self
    n.next = self.next
    if self.next is not None:
      self.next.prev = n
    self.next = n
    n.record = record

  def getTimeDeltaWithNextNode(self):
    if self.next is None:
      raise "No future nodes exist beyond current node!"

    return self.next.record.datetime - self.record.datetime

  def __str__(self):
    return str(self.record)

  def removeSegmentEndingAt(self, ending_node):
    if self == ending_node:
      logger.warning("You almost deleted an incorrect node!")
      return

    # The ending point of the segment (last_node.next) should point to None
    ending_node.prev.next = None

    # Connect the nodes surrounding this segment.
    ending_node.prev = self.prev
    self.prev.next = ending_node

    # The beginning point of the segment (first_node.prev) should point to None
    self.prev = None
    
    # Only deleting one node.
    #  i.e. self and ending_node are neighbors
    if self.next is None:
      self.record = None
      return

    else:
    # Begin iterating and deleting.
      current = self
      while current.next is not None:
        current.record = None
        current = current.next
        current.prev.next = None
        current.prev = None

      current.record = None


def getExtent(x1, x2, y1, y2):
  return (min(x1, x2), max(x1, x2), min(y1, y2), max(y1, y2))
