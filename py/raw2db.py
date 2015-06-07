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
from raw import record
from raw import user
from utils import ETACalculator
from collections import defaultdict
import config


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
  parser.add_argument(
    '-w', '--weekday',
    dest="weekday",
    help='Numerical indicator of weekday (0 is Monday, 1 is Tuesday, ..., 6 is Sunday)',
    default=None,
  )

  args = parser.parse_args()
  return args


if __name__ == "__main__":
  args = get_arguments()
  directory = args.input_directory
  logger.info("Loading database with raw Geolife records")
  
  if args.weekday is None:
    logger.info("All weekdays")
    weekday = None

  else:
    weekday = int(args.weekday)
    logger.info("Weekday: {0}".format(record.WEEKDAY_STRINGS[weekday]))

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

  timer = ETACalculator(iterations=geolife.get_num_files(directory))
  user_weekday_counts = defaultdict(int)
  for u in user.from_directory(directory):
    logger.info("Beginning yielding of records from user {0.id}".format(u))
    for f in u.files:
      f.restrictRecordsTo(weekday=weekday, aoi=config.BEIJING_80)
      if weekday is None or f.occursOn(weekday):
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

  # Create an index on the time values
  logger.info("Creating index on raw record time columns")
  Index('raw_time', record.WRecord.__table__.c.time).create(engine)

  # Store the user-date-count information.
  user_weekday_counts_db = []
  for key in user_weekday_counts:
        weekday_counts = user_weekday_counts[key]
        user_id, weekday = key
        user_weekday_counts_db.append(record.GeoLifeUser(
            id=user_id,
            count=weekday_counts,
            weekday=weekday,
        ))

  session.add_all(user_weekday_counts_db)
  session.commit()

