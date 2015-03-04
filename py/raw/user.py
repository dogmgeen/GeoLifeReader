import logging
logger = logging.getLogger("geolife.user")
import os
import pltfile
from collections import defaultdict
from record import LinkedRecords
from record import WRecord as GeoLifeRecord
from utils import datetimerange
from sqlalchemy import update

class BaseGeoLifeUser:
  def __init__(self):
    self.id = None

  def __str__(self):
    return "<{0}(id={1}, size={2})>".format(
      self.__class__.__name__,
      self.id,
      len(self)
    )
  __repr__=__str__
  
  def __iter__(self):
    raise NotImplemented(
      "Class {0} must implement the __iter__() method.".format(
      self.__class__.__name__
    ))

  def __len__(self):
    raise NotImplemented(
      "Class {0} must implement the __len__() method.".format(
      self.__class__.__name__
    ))

  def __bool__(self):
    raise NotImplemented(
      "Class {0} must implement the __bool__() method.".format(
      self.__class__.__name__
    ))


class GeoLifeUserFromFile(BaseGeoLifeUser):
  def __init__(self, user_id, directory):
    self.id = int(user_id)
    self.files = pltfile.load_from_directory(
      directory=os.path.join(directory, user_id),
      user=self
    )

def from_directory(directory):
  users = []
  for d in os.listdir(directory):
    if os.path.isdir(os.path.join(directory, d)):
      logger.debug("Yielding user {0}".format(d))
      yield GeoLifeUserFromFile(user_id=d, directory=directory)


class GeoLifeUserFromDB(BaseGeoLifeUser):
  def __init__(self):
    BaseGeoLifeUser.__init__(self)
    self.records = []
    self.linked_list = None
    self.record_ptr = None
    self.synthesized_records = []
    self.modified_records = []

  def add(self, record):
    if self.id is None:
      self.id = record.user
    self.records.append(record)

  def sort(self):
    self.records.sort(key=lambda r: r.datetime)

  def __len__(self):
    return len(self.records)

  def __iter__(self):
    for i in self.records:
      yield i

  def __bool__(self):
    return bool(self.records)

  def __getitem__(self, key):
    return self.records[key]

  def homogenizeTimeDeltas(self, start, end, delta, session):
    current = self.linked_list
    reference_record = current.record
    c_datetime = current.record.datetime
    for d in datetimerange(start, end+delta, delta):
      logger.debug("="*80)
      logger.debug("Current record ptr: {0}".format(current.record))
      logger.debug("Homogenized date time: {0}".format(d))
      lower_bound = d-delta
      upper_bound = d+delta
      logger.debug("Looking between window of {0} to {1}".format(lower_bound, upper_bound))
      # Three possible states could be encountered.
      #  1. The current record is within a window
      #      surrounding d
      #      e.g.  d-delta <= c.datetime < d+delta
      if lower_bound < c_datetime < upper_bound:
        logger.debug("Current record falls within window! Modifying...")
        if lower_bound < c_datetime <= d:
          logger.debug("Current record occurs within previous window. We must"
                       " delete all nodes except the one less than or equal to"
                       " {0}".format(d))
          # Find the most recent record that is closest to the target date d
          start = current
          while current.next is not None and current.record.datetime <= d:
            logger.debug("Delete {0}".format(current.record))
            current = current.next
          
          if current.next is None:
            pass

          elif current.record.datetime == d:
            pass

          else:
            logger.debug("Don't delete {0}!!!".format(current.record))
            current = current.prev

          # Remove all nodes between current (inclusive) and searcher (exclusive)
          if start != current:
            start.removeSegmentEndingAt(current)
          start = None
          logger.debug("Searching for record less than or equal to {0} found {1}".format(
            d, current
          ))

        else:
          logger.debug("Current record occurs within next window.")

        # Adjust the current element's datetime.
        logger.debug(current.record)
        logger.debug("... to ...")
        current.record.datetime = d
        current.record.date = d.date()
        current.record.time = d.time()
        logger.debug(current.record)
        #self.modified_records.append(current.record)

        # Move forward if possible
        reference_record = current.record
        if current.next is not None:
          logger.debug("Moving forward to {0}".format(current.next.record))
          current = current.next

        else:
          logger.debug("Cannot move forward! current.next points to None.")
        c_datetime = current.record.datetime

      else:
        logger.debug("Current record falls outside window.")
        logger.debug("Generating a new record with current timestamp.")
        logger.debug("Base record: {0}".format(reference_record))
        modified_record = GeoLifeRecord(
          user=reference_record.user,
          latitude=reference_record.latitude,
          longitude=reference_record.longitude,
          datetime=d,
          weekday=d.weekday(),
        )
        logger.debug("Modified record: {0}".format(modified_record))
        self.synthesized_records.append(modified_record)

        #  2. The current record is before this window
        #      e.g.  c.datetime < d-delta
        if c_datetime <= lower_bound:
          logger.debug("Current record is behind!")
          logger.debug("Modified record will be added after current")
          logger.debug("Current record: {0}".format(current.record))
          logger.debug("     .next := {0}".format(current.next))
          logger.debug("     .prev := {0}".format(current.prev))
          current.insertAfter(modified_record)
          current = current.next
          c_datetime = current.record.datetime

        #  3. The current record is after this window
        #      e.g   d+delta <= c.datetime
        elif upper_bound <= c_datetime:
          logger.debug("Current record is ahead!")
          logger.debug("Modified record will be added before current")
          logger.debug("Current record: {0}".format(current.record))
          logger.debug("     .next := {0}".format(current.next))
          logger.debug("     .prev := {0}".format(current.prev))
          current.insertBefore(modified_record)


        else:
          logger.error("Uh... something went wrong.")

      # Verify previous node is ahead of current node by delta
      if current.prev.prev is not None:
            assert (current.prev.record.datetime - current.prev.prev.record.datetime) == delta, "Previous record {0} is not {1} ahead of {2}".format(
              current.prev.prev.record,
              delta,
              current.prev.record,
            )


      if len(self.synthesized_records) > 10000:
        self.__commit_synthesized_records(session)
      #if len(self.modified_records) > 10000:
      #  self.__commit_modified_records(session)

    self.__commit_synthesized_records(session)
    #self.__commit_modified_records(session)

  def __commit_synthesized_records(self, session):
    if self.synthesized_records:
      logger.info("Adding synthesized records for {0}".format(self))
      #session.add_all(self.synthesized_records)
      #session.commit()
      del self.synthesized_records[:]

  def link_listify_records(self):
    logger.info("Link listifying {0}".format(self))
    self.linked_list = LinkedRecords(self.records)

    # Have record_ptr point to the head of the linked list
    self.record_ptr = self.linked_list

  def is_time_homogenized(self):
    logger.info("Verifying time homogenization for {0}".format(self))
    current = self.linked_list
    expected_delta = current.getTimeDeltaWithNextNode()

    while current.next.next is not None:
      current = current.next
      actual_delta = current.getTimeDeltaWithNextNode()

      if expected_delta != actual_delta:
        """
        c = current
        for i in range(5):
          logger.debug(" "*80 + str(c.record))
          logger.debug(" "*90 + "|   next")
          logger.debug(" "*90 + "v")
          c = c.next
          if c is None:
            break
        logger.debug("#"*100)
        c = current
        for i in range(5):
          logger.debug(" "*80 + str(c.record))
          logger.debug(" "*90 + "|   prev")
          logger.debug(" "*90 + "v")
          c = c.prev
          if c is None:
            break
        """
        logger.error("Following records do not have expected time delta of"
                     " {0}\n\t{1}\n\t{2}".format(
                       expected_delta,
                       current.record,
                       current.next.record
        ))
       
        return False

    return True

  def getExtent(self):
    return self.linked_list.extent

  def getRecordOn(self, timestamp):
    """Assume the timestamps are incrementing upward at a regular interval"""
    assert self.record_ptr.record.datetime == timestamp, (
      "Record {0} does not have expected timestamp of {1}".format(
      self.record_ptr.record, timestamp
    ))
    r = self.record_ptr.record
    self.record_ptr = self.record_ptr.next
    r.node_address = self.address
    return r

  def verifyLinkListPointsToTrueHead(self):
    head = self.linked_list
    while head.prev is not None:
      head = head.prev
    self.linked_list = head
    self.record_ptr = head

  def setAddress(self, addr):
    self.address = addr


def from_Query(query, normalize_ids):
  # The query provided may have user IDs that are not consecutive. For
  #   instance, user IDs 3, 4, 9, 21, etc. If normalize_ids is True,
  #   then these IDs will be mapped to consecutive IDs: 0, 1, 2, ...

  # Split up the results by user
  users = defaultdict(GeoLifeUserFromDB)
  for record in query:
    users[record.user].add(record)

  users_list = users.values()
  i = 0
  for u in users_list:
    u.sort()
    u.link_listify_records()

    if normalize_ids:
      logger.info("{0} will have address {1}".format(u, i))
      u.setAddress(i)
      i += 1

  return users_list

