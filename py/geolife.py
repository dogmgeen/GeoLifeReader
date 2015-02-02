import logging
logger = logging.getLogger("geolife.geolife")

import sys
import os
from datetime import datetime 
from record import GeoLifeRecord
import user
from stats import StatisticsCalculator
from utils import datetimerange
from one import ExternalMovementReaderConverter

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


class GeoLifeDataset:
  def __init__(self, directory):
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
    # The pre-existing SQLite database will be named according to the
    #  hash created by the directory.
    import hashlib
    directory_hash = hashlib.md5(directory.encode("utf-8")).hexdigest()

    db_name = "{0}.db".format(directory_hash)
    db_exists = os.path.isfile(db_name)

    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///{0}'.format(db_name))

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    if not db_exists:
      logger.info("-"*50)
      logger.info("Database does not pre-exist at {0}!".format(db_name))
      logger.info("Database will be created and populated from files"
                  " in {0}".format(directory))
      import record
      record.initialize_table(engine)
      for u in user.from_directory(directory):
        logger.debug("Beginning yielding of records from user {0.id}".format(u))
        for f in u.files:
          logger.debug("Reading from file {0}".format(os.path.basename(f.url)))
          session.add_all(f)
          session.commit()
        #del u.files[:]

      #for r in load_from_directory(directory):
      #  session.add(r)

    return session

  def onlyRetrieveSomeUsers(self, n, randomize=False):
    # retrieve all unique user IDs present in the database
    # obtain a subset of the users of size n
    # Reduce result set such that only records that have a user in the subset
    #  are present.
    self.result_set = self.result_set.filter(
      GeoLifeRecord.user.in_(selected_user_ids)
    )
    return self

  def retrieveByDate(self, date):
    logger.info("+"*80)
    logger.info("Reducing result set to those occuring on {0}".format(date))
    if isinstance(date, str):
      date = datetime.strptime(date, "%Y-%m-%d").date()
    logger.info("Before removing by date: {0}".format(
      self.result_set.count()
    ))

    self.result_set = self.result_set\
        .with_hint(GeoLifeRecord, 'USE INDEX ix_records_date')\
        .filter(GeoLifeRecord.date==date)\
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
    self.users = user.from_Query(self.result_set)
    logger.info("Found {0} users in result set".format(len(self.users)))

    self.statistics = StatisticsCalculator(self.users)
    self.statistics.run()
    return self

  def homogenizeTimeDeltas(self, delta=None):
    delta = self.statistics.min_time_delta
    start = self.statistics.min_time
    end = self.statistics.max_time

    print("#"*80)
    logger.info("Homogenizing time deltas to {0} seconds".format(delta))
    for d in datetimerange(start, end+delta, delta):
      logger.info("-"*60)
      logger.info(d)
      for u in self.users:
        u.add_record_if_not_present_for(time=d)

    # Verify time homogeneous time steps
    for u in self.users:
      assert u.is_time_homogenized(), "User {0} is not time-homogenized!".format(u)

    return self

  def convertToONE(self, to_file):
    logger.info("#"*80)
    logger.info("Converting GeoLife to ONE format")
    logger.info("================================")
    delta = self.statistics.min_time_delta
    start = self.statistics.min_time
    end = self.statistics.max_time
    c = ExternalMovementReaderConverter(self.statistics, 90000)

    with open(to_file, "w") as f:
      logger.debug("Opening file {0}".format(to_file))
      # Write out the header of the file.
      # minTime maxTime minX maxX minY maxY minZ maxZ
      f.write("{minTime} {maxTime} "
              "{minX} {maxX} "
              "{minY} {maxY} "
              "{minZ} {maxZ}\n".format(
        **c.getHeader()
      ))

      for d in datetimerange(start, end+delta, delta):
        logger.debug("-"*40)
        logger.debug("Iterating through records on {0}".format(d))
        for u in self.users:
          logger.debug(u)
          f.write("{0}\n".format(c(u.getRecordOn(timestamp=d))))


def load_from_directory(directory):
  for u in user.from_directory(directory):
    logger.debug("Beginning yielding of records from user {0.id}".format(u))
    for record in u:
      yield record

