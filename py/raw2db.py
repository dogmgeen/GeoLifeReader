import logging
logging.basicConfig(
  level=logging.DEBUG,
  filename='/tmp/geolife.log',
  filemode='w'
)
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.INFO)
logger.addHandler(stdout)

import argparse
import os
import geolife
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Index
from sqlalchemy.sql import func
from raw import record
from raw import user
from utils import ETACalculator
from collections import defaultdict
import config
from datetime import date
from datetime import datetime
from utils import get_users

# Parse the command-line arguments.
def get_arguments():
  parser = argparse.ArgumentParser(
    description='Populate local database with each raw trajectory.'
  )
  parser.add_argument(
    '-i', '--input_directory',
    dest="input_directory",
    help='Directory containing PLT files (default: current working directory)',
    type=geolife.find_geolife_root,
    required=True,
  )

  args = parser.parse_args()
  return args


if __name__ == "__main__":
  args = get_arguments()
  directory = args.input_directory
  logger.info("Loading database with raw Geolife records")
  
  logger.info("Source:  {0}".format(directory))
  
  engine = config.getEngine()

  Session = sessionmaker()
  Session.configure(bind=engine)
  session = Session()

  logger.info("-"*50)
  logger.info("Database will be created and populated from files"
              " in {0}".format(directory))
  record.initialize_table(engine)
  logger.info("Table initialized")

  """
  timer = ETACalculator(iterations=geolife.get_num_files(directory))
  for u in user.from_directory(directory):

    logger.info("Beginning yielding of records from user {0.id}".format(u))
    for f in u.files:
      session.add_all(f)
      session.commit()

      timer.checkpoint()
      logger.info("="*60)
      logger.info("File {0}".format(os.path.basename(f.url)))
      logger.info(timer.eta())
      u.num_records += f.num_records

    session.add(u.to_DB())
    session.commit()

  # Create an index on the time values
  logger.info("Creating index on raw record time columns")
  Index('raw_time', record.RawRecord.__table__.c.time).create(engine)
  Index('raw_latitude', record.RawRecord.__table__.c.latitude).create(engine)
  Index('raw_longitude', record.RawRecord.__table__.c.longitude).create(engine)
  Index('raw_newnames', record.RawRecord.__table__.c.date_user_id).create(engine)
  """
  #engine.execute("""
  #  CREATE VIEW day_records_view 
  #  AS SELECT
  #    latitude as lat,
  #    longitude as long,
  #    time as timestamp,
  #    "user" as old_user_id,
  #    date_user_id as new_user_id
  #    from raw_records
  #    WHERE latitude > {0}
  #    AND latitude < {1}
  #    AND longitude > {2}
  #    AND longitude < {3};
  #""".format(
  #  config.BOUNDS['south'],
  #  config.BOUNDS['north'],
  #  config.BOUNDS['west'],
  #  config.BOUNDS['east'],
  #))
  

  from schema import RecordsOnOneDay

  logger.info("Preloading RecentUserRecord object")
  users = get_users(session)
  records = session.query(RecordsOnOneDay).order_by(RecordsOnOneDay.c.timestamp)
  eta = ETACalculator(len(users), name="Earliest User Records")
  date_dummy = date.today()

  dayusers = []
  for u in users:
      logger.info("Calculating the user's centroid of movement")
      centroid_of_movement = session.query(
        func.avg(RecordsOnOneDay.c.lat).label('lat'),
        func.avg(RecordsOnOneDay.c.long).label('long'),
      ).filter(RecordsOnOneDay.c.new_user_id==u).first()

      count = records.filter(RecordsOnOneDay.c.new_user_id == u).count()
      r = records.filter(RecordsOnOneDay.c.new_user_id == u).first()
      last = session.query(RecordsOnOneDay).filter(
        RecordsOnOneDay.c.new_user_id == u
      ).order_by(
        RecordsOnOneDay.c.timestamp.desc()
      ).first()


      # If this node has no activity within the bounds, skip them.
      if r is None:
        continue

      latest_timestamp = datetime.combine(date_dummy, last.timestamp)
      earliest_timestamp = datetime.combine(date_dummy, r.timestamp)
      duration_seconds = latest_timestamp - earliest_timestamp

      #dayusers.append(
      session.add(
        record.DayUser(
        id=u, count=count,
        earliest_record_time=r.timestamp,
        latest_record_time=last.timestamp,
        duration=(duration_seconds).total_seconds(),
        centroid_lat=centroid_of_movement.lat,
        centroid_lon=centroid_of_movement.long,
      ))

      logger.info("First record for user {0}: {1}".format(u, r))
      eta.checkpoint()
      logger.info(eta.eta())

  #session.add_all(dayusers)
  session.commit()
  session.close()

