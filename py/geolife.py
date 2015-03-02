import logging
logger = logging.getLogger("geolife.geolife")

import sys
import os
from datetime import datetime
from datetime import timedelta
from record import WEEKDAY_STRINGS
from record import WRecord as GeoLifeRecord
from record import GeoLifeUserCountPerDay
import user
from stats import StatisticsCalculator
from one import ExternalMovementReaderConverter
from sqlalchemy.sql import select
import time
from utils import datetimerange
from utils import ETACalculator
from collections import defaultdict


def find_geolife_root(directory_to_search):
  directory_containing_plt = None

  # Walk down tree until a PLT file is encountered.
  for d, subd, files in os.walk(directory_to_search):
    for f in files:
      if f.lower().endswith(".plt"):
        directory_containing_plt = d
        break

  # Return the "Data" directory, which contains all users
  #  and subsequently all raw data files.
  return os.path.dirname(os.path.dirname(directory_containing_plt))


def get_num_files(directory):
  n = 0
  for d, subd, files in os.walk(directory):
    for f in files:
      if f.lower().endswith(".plt"):
        n += 1
  return n

class GeoLifeDataset:
  def __init__(self, directory=None):
    """Perform a lazy load of the raw PLT files that are located somewhere
    in the provided directory. Code has been written to automatically locate
    these files, so the exact directory is not necessary. All that is required
    is that the raw PLT files be SOMEWHERE within the tree rooted at that
    directory.

    An external database will be populated with the data associated with this
    dataset. If this database has already been created, the database will
    be loaded instead of reading in the raw files based on the hash associated
    with the provided directory."""
    self.db_session = self.__load_db(directory)
    self.result_set = self.db_session.query(GeoLifeRecord)
    
  def close(self):
    self.db_session.close()

  def __load_db(self, directory):
    """Load a database session corresponding to the data within the provided
    directory. This session may need to be created now if it was not created
    prior."""
    from sqlalchemy import create_engine
    self.engine = create_engine(
      "{dialect}://{username}:{password}@{host}/{database}".format(
      dialect='postgresql+psycopg2',
      username='postgres',
      password='nope27rola',
      host='localhost',
      database='geolife'
    ))

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker()
    Session.configure(bind=self.engine)
    session = Session()
    if directory is not None:
      logger.info("-"*50)
      logger.info("Database will be created and populated from files"
                  " in {0}".format(directory))
      import record
      record.initialize_table(self.engine)
      logger.info("Table initialized")

      timer = ETACalculator(iterations=get_num_files(directory))
      user_weekday_counts = defaultdict(int)
      for u in user.from_directory(directory):
        logger.info("Beginning yielding of records from user {0.id}".format(u))
        for f in u.files:
          session.add_all(f)
          session.commit()

          # Now that this file has been read, the user-date-file information
          #  has been populated.
          synthesized_users_in_file = 0
          for user_id in f.weekday_counts:
            user_weekday_counts[user_id] += f.weekday_counts[user_id]
            synthesized_users_in_file += 1
          logger.info("File {0} has {1} synthesized users, who will be summarized in db".format(
            os.path.basename(f.url), synthesized_users_in_file
          ))

          timer.checkpoint()
          logger.info("="*60)
          logger.info("File {0}".format(os.path.basename(f.url)))
          logger.info(timer.eta())

      # Store the user-date-count information.
      user_weekday_counts_db = []
      for user_id in user_weekday_counts:
        weekday_counts = user_weekday_counts[user_id]
        user_weekday_counts_db.append(GeoLifeUserCountPerDay(
            id=user_id,
            count=weekday_counts,
        ))

      session.add_all(user_weekday_counts_db)
      session.commit()

    return session

  def removeUsersWithTooFewRecords(self, weekday=0, min_records=2):
    logger.info("Removing users with fewer than {0} records".format(
      min_records
    ))

    user_ids = self.getUniqueUsers()
    n = len(user_ids)
    logger.info("Before filtering, there are {0} users".format(n))
    users_to_filter = []
    for u in user_ids:
      logger.info("Analyzing records for user {0}".format(u))
      count = self.db_session.query(GeoLifeUserCountPerDay.count)\
                  .filter_by(id=u).scalar()
      logger.info("User {0} has {1} records".format(u, count))
      if count < min_records:
        logger.debug("User {0} only has {1} records. User will not"
                     " be present in dataset.".format(u, count
        ))
        users_to_filter.append(u)

      else:
        logger.info("Passed")

    if users_to_filter:
      logger.info("After filtering, there will be {0} users".format(
        n-len(users_to_filter)
      ))
      self.result_set = self.result_set.filter(
        GeoLifeRecord.user.notin_(users_to_filter)
      )

    else:
      logger.info("No users have fewer than {0} records".format(min_records))

    return self

  def onlyRetrieveSomeUsers(self, n=None, randomize=False):
    if n is None:
      return self

    # retrieve all unique user IDs present in the database
    logger.info("Retrieving unique users from database.")
    logger.info("At most {0} users will exist in the output dataset.".format(n))
    if randomize:
      logger.info("The set of users will be randomly selected")

    else:
      logger.info("The set of users will be 0, 1, 2, ...")

    user_ids = self.getUniqueUsers()
 
    # obtain a subset of the users of size n
    if len(user_ids) > n:
      if randomize:
        import random
        import time
        random.seed(time.time())
        selected_user_ids = random.sample(user_ids, n)

      else:
        user_ids = list(user_ids)
        user_ids.sort()
        selected_user_ids = user_ids[:n]

      assert len(selected_user_ids) == n, (
        "Selected set of user IDs is not of size {0}, but of size {1}".format(
          n, len(selected_user_ids
      )))

      logger.info("Selected user IDs: {0}".format(selected_user_ids))
      # Reduce result set such that only records that have a user in the subset
      #  are present.
      logger.info("Before reducing by user ID: {0}".format(
        self.result_set.count()
      ))

      self.result_set = self.result_set.filter(
        GeoLifeRecord.user.in_(selected_user_ids)
      )

      logger.info("After reducing by user ID: {0}".format(
        self.result_set.count()
      ))

    else:
      logger.warning("No shrinking of user set necessary. Only {0} users are"
                     " present in the database. A subset of size {1} will"
                     " only include {0} users.".format(
                     len(user_ids), n
      ))

    return self

  def getUniqueUsers(self):
    user_ids = set()
    for r in self.result_set:
      user_ids.add(r.user)
    logger.info("Number of User IDs: {0}".format(len(user_ids)))
    logger.debug(user_ids)
    return user_ids

  def retrieveByWeekday(self, weekday):
    logger.info("+"*80)
    logger.info("Reducing result set to those occuring only on {0}".format(
      WEEKDAY_STRINGS[weekday]
    ))

    logger.info("Before removing by date: {0}".format(
      self.result_set.count()
    ))

    #.with_hint(GeoLifeRecord, 'USE INDEX ix_records_date')\
    self.result_set = self.result_set\
        .filter(GeoLifeRecord.weekday==weekday)\
        .order_by(GeoLifeRecord.datetime)

    logger.info("After removing by date: {0}".format(self.result_set.count()))
    return self

  def boundByLocation(self, north=90., south=-90., east=180., west=-180.):
    logger.info("+"*80)
    logger.info("Reducing result set to within ({west}, {north}) and"
                " ({east}, {south})".format(**locals()))
    logger.info("Before removing by location: {0}".format(
      self.result_set.count()
    ))

    self.result_set = self.result_set.filter(
      GeoLifeRecord.latitude < north,
      GeoLifeRecord.latitude > south,
      GeoLifeRecord.longitude < east,
      GeoLifeRecord.longitude > west
    ).order_by(GeoLifeRecord.datetime)

    logger.info("After removing by location: {0}".format(
      self.result_set.count()
    ))
    return self

  def calculateStatistics(self):
    # Determine the unique user IDs within the result set.
    # It is assumed no further reductions on the dataset will be made.
    logger.info("+"*80)
    logger.info("Calculating statistics")
    self.users = user.from_Query(self.result_set, normalize_ids=True)
    logger.info("Found {0} users in result set".format(len(self.users)))

    self.statistics = StatisticsCalculator(self.users)
    self.statistics.run()
    return self

  def homogenizeTimeDeltas(self, delta):
    self.delta = delta
    start = self.statistics.min_time
    end = self.statistics.max_time

    print("#"*80)
    logger.info("Homogenizing time deltas to {0} seconds".format(delta))
    for u in self.users:
      u.homogenizeTimeDeltas(start, end, self.delta, self.db_session)
      u.verifyLinkListPointsToTrueHead()
      assert u.is_time_homogenized(), "User {0} is not time-homogenized!".format(u)
    return self

  def convertToONE(self, to_file):
    logger.info("#"*80)
    logger.info("Converting GeoLife to ONE format")
    logger.info("================================")
    start = self.statistics.min_time
    end = self.statistics.max_time
    c = ExternalMovementReaderConverter(self.statistics, 90000)
    self.metadata = c.getHeader()

    with open(to_file, "w") as f:
      logger.info("Opening file {0}".format(to_file))
      # Write out the header of the file.
      # minTime maxTime minX maxX minY maxY minZ maxZ
      f.write("{minTime} {maxTime} "
              "{minX} {maxX} "
              "{minY} {maxY} "
              "{minZ} {maxZ}\n".format(
        **c.getHeader()
      ))

      for d in datetimerange(start, end+self.delta, self.delta):
        #logger.debug("-"*40)
        #logger.debug("Iterating through records on {0}".format(d))
        for u in self.users:
          #logger.debug("Writing all records from {0} at {1}".format(u, d))
          f.write("{0}\n".format(c(u.getRecordOn(timestamp=d))))
    return self


def load_from_directory(directory):
  for u in user.from_directory(directory):
    logger.debug("Beginning yielding of records from user {0.id}".format(u))
    for record in u:
      yield record

