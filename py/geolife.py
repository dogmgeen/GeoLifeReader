import logging
logger = logging.getLogger("geolife.geolife")

import sys
import os
from datetime import datetime
from datetime import timedelta
from record import GeoLifeRecord
import user
from stats import StatisticsCalculator
from one import ExternalMovementReaderConverter
from sqlalchemy.sql import select
import time
from utils import datetimerange

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
    

  def __load_db(self, directory):
    """Load a database session corresponding to the data within the provided
    directory. This session may need to be created now if it was not created
    prior."""
    from sqlalchemy import create_engine
    self.engine = create_engine(
      "{dialect}://{username}:{password}@{host}/{database}".format(
      dialect='mysql',
      username='kp',
      password='nope',
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

      n = get_num_files(directory)
      i = 0
      avg = 0

      for u in user.from_directory(directory):
        logger.info("Beginning yielding of records from user {0.id}".format(u))
        for f in u.files:
          i += 1
          logger.info("{0:.4%} complete! Reading from file {1}"
                      " ({2} out of {3} files)".format(
            float(i)/n,
            os.path.basename(f.url),
            i, n
          ))
          start = datetime.now()

          session.add_all(f)
          session.commit()

          duration = datetime.now() - start
          avg = (duration.total_seconds() + i*avg)/(i+1)
          eta = avg*(n-i)
          average = timedelta(seconds=avg)
          eta_delta = timedelta(seconds=int(eta))
          logger.info("File {0} took {1} seconds (average: {2})".format(
            os.path.basename(f.url), duration, average
          ))
          logger.info(" "*100 + "ETA: {0}".format(eta_delta))
    return session

  def onlyRetrieveSomeUsers(self, n, randomize=False):
    # retrieve all unique user IDs present in the database
    logger.info("Retrieving unique users from database.")
    logger.info("At most {0} users will exist in the output dataset.".format(n))
    if randomize:
      logger.info("The set of users will be randomly selected")

    else:
      logger.info("The set of users will be 0, 1, 2, ...")

    #s = select([GeoLifeRecord.user]).distinct()
    #conn = self.engine.connect()
    #result = conn.execute(s)
    user_ids = set()
    for r in self.result_set:
      user_ids.add(r.user)

    #for r, in result:
    #  print("Unique user: {0}".format(r))
    #result.close()
    logger.info("User IDs: {0}".format(user_ids))

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

      assert len(selected_user_ids) == n, "Selected set of user IDs is not of size {0}, but of size {1}".format(n, len(selected_user_ids))

      logger.info("Selected user IDs: {0}".format(selected_user_ids))
      # Reduce result set such that only records that have a user in the subset
      #  are present.
      logger.debug("Before reducing by user ID: {0}".format(
        self.result_set.count()
      ))

      self.result_set = self.result_set.filter(
        GeoLifeRecord.user.in_(selected_user_ids)
      )

      logger.debug("After reducing by user ID: {0}".format(
        self.result_set.count()
      ))

    else:
      logger.warning("Only {0} users are present in the database. A subset of"
                     " size {1} will only include {0} users.".format(
                     len(user_ids), n
      ))

    return self

  def retrieveByDate(self, date):
    logger.info("+"*80)
    logger.info("Reducing result set to those occuring on {0}".format(date))
    if isinstance(date, str):
      date = datetime.strptime(date, "%Y-%m-%d").date()
    logger.debug("Before removing by date: {0}".format(
      self.result_set.count()
    ))

    #.with_hint(GeoLifeRecord, 'USE INDEX ix_records_date')\
    self.result_set = self.result_set\
        .filter(GeoLifeRecord.date==date)\
        .order_by(GeoLifeRecord.datetime)

    logger.debug("After removing by date: {0}".format(self.result_set.count()))
    return self


  def boundByLocation(self, north=90., south=-90., east=180., west=-180.):
    logger.info("+"*80)
    logger.info("Reducing result set to within ({west}, {north}) and"
                " ({east}, {south})".format(**locals()))
    logger.debug("Before removing by location: {0}".format(
      self.result_set.count()
    ))

    self.result_set = self.result_set.filter(
      GeoLifeRecord.latitude < north,
      GeoLifeRecord.latitude > south,
      GeoLifeRecord.longitude < east,
      GeoLifeRecord.longitude > west
    ).order_by(GeoLifeRecord.datetime)

    logger.debug("After removing by location: {0}".format(
      self.result_set.count()
    ))
    return self

  def calculateStatistics(self):
    # Determine the unique user IDs within the result set.
    # It is assumed no further reductions on the dataset will be made.
    logger.info("+"*80)
    logger.info("Calculating statistics")
    self.users = user.from_Query(self.result_set)
    logger.info("Found {0} users in result set".format(len(self.users)))

    self.statistics = StatisticsCalculator(self.users)
    self.statistics.run()
    return self

  def homogenizeTimeDeltas(self, delta=None):
    if delta is None:
      self.delta = self.statistics.min_time_delta
    else:
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
        logger.debug("-"*40)
        logger.debug("Iterating through records on {0}".format(d))
        for u in self.users:
          logger.debug("Writing all records from {0} at {1}".format(u, d))
          f.write("{0}\n".format(c(u.getRecordOn(timestamp=d))))


def load_from_directory(directory):
  for u in user.from_directory(directory):
    logger.debug("Beginning yielding of records from user {0.id}".format(u))
    for record in u:
      yield record

