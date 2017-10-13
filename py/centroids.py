import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("geolife")
#stdout = logging.StreamHandler()
#stdout.setLevel(logging.INFO)
#logger.addHandler(stdout)

from utils import ETACalculator
from schema import get_users
from schema import RecordsOnOneDay
import argparse
from sqlalchemy.sql import func
from sqlalchemy import Index
import csv
from raw import record

import config
engine = config.getEngine()

from sqlalchemy.orm import sessionmaker
Session = sessionmaker()
Session.configure(bind=engine)


def get_arguments():
  parser = argparse.ArgumentParser(
    description='Extract centroids of user movements.'
  )
  parser.add_argument(
    '-t', '--dry-run',
    dest="dry_run",
    action="store_true",
    help=('Boolean indicator controlling if data should be added to the'
          ' database (default: False).'),
  )

  args = parser.parse_args()
  return args


def createCentroids(session):
  eta = ETACalculator(len(users), "User iteration")

  with open('centroids.csv', 'w') as csvfile:
    fieldnames = ['user', 'lat', 'long']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for u in users:
      centroid_of_movement = session.query(
        func.avg(RecordsOnOneDay.c.lat).label('lat'),
        func.avg(RecordsOnOneDay.c.long).label('long'),
      ).filter(RecordsOnOneDay.c.new_user_id==u).first()

      print("User #{0} has centroid {1}".format(u, centroid_of_movement))
      writer.writerow({
        'user': u,
        'lat': centroid_of_movement.lat,
        'long': centroid_of_movement.long,
      })

      eta.checkpoint()
      logger.info(eta.eta())


if __name__ == "__main__":
  args = get_arguments()
  dry_run = args.dry_run

  # Only users for a particular day will be selected.
  # If this argument is not specified, then all users will be selected.
  session = Session()
  createCentroids(session)


