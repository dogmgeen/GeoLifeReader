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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from raw import record
from raw import user
from utils import ETACalculator
from collections import defaultdict


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

  engine = create_engine(
      "{dialect}://{username}:{password}@{host}/{database}".format(
      dialect='postgresql+psycopg2',
      username='postgres',
      password='nope27rola',
      host='localhost',
      database='geolife'
  ))

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
        user_weekday_counts_db.append(record.GeoLifeUserCountPerDay(
            id=user_id,
            count=weekday_counts,
        ))

  session.add_all(user_weekday_counts_db)
  session.commit()

