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

  timer = ETACalculator(iterations=geolife.get_num_files(directory))
  user_counts = defaultdict(int)
  for u in user.from_directory(directory):
    logger.info("Beginning yielding of records from user {0.id}".format(u))
    for f in u.files:
      f.restrictRecordsTo(aoi=config.BEIJING_80)
      session.add_all(f)
      session.commit()

      if f.count:
        user_counts[u.id] += f.count

      logger.info("File {0} has {1} records".format(
        os.path.basename(f.url), f.count
      ))

      timer.checkpoint()
      logger.info(timer.eta())
      logger.info("="*60)

  # Create an index on the time values
  #logger.info("Creating index on raw record time columns")
  #Index('raw_time_idx', record.RawRecord.__table__.c.time)

  # Store the user-date-count information.
  user_counts_db = []
  for key in user_counts:
    user_counts_db.append(record.GeoLifeUser(
      id=key, count=user_counts[key],
    ))
    logger.info("User {0} has {1} records".format(key, user_counts[key]))

  session.add_all(user_counts_db)
  session.commit()

