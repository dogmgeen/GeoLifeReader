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

  engine.execute("""
    CREATE VIEW day_records_view 
    AS SELECT
      latitude as lat,
      longitude as long,
      time as timestamp,
      "user" as old_user_id,
      concat(to_char(date, 'IYYYMMDD'), "user") as new_user_id
      from raw_records
      WHERE latitude > {0}
      AND latitude < {1}
      AND longitude > {2}
      AND longitude < {3};
  """.format(
    config.BOUNDS['south'],
    config.BOUNDS['north'],
    config.BOUNDS['west'],
    config.BOUNDS['east'],
  ))
