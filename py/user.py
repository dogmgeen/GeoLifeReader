import logging
logger = logging.getLogger("geolife.user")
import os
import pltfile
from collections import defaultdict

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


class GeoLifeUserFromFile(BaseGeoLifeUser):
  def __init__(self, user_id, directory):
    self.id = int(user_id)
    self.files = pltfile.load_from_directory(
      directory=os.path.join(directory, user_id),
      user=self
    )

  def __iter__(self):
    for f in self.files:
      import os
      logger.debug("Reading from file {0}".format(os.path.basename(f.url)))
      for r in f:
        yield r


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


def from_Query(query):
  # Split up the results by user
  users = defaultdict(GeoLifeUserFromDB)
  for record in query:
    users[record.user].add(record)
    logger.info(record)
  logger.info(users)

  # Verify the records for each user are in order
  for uid in users:
    users[uid].sort()
    for r in users[uid]:
      logger.info(r)

 
