import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("geolife")
#stdout = logging.StreamHandler()
#stdout.setLevel(logging.INFO)
#logger.addHandler(stdout)

from utils import ETACalculator
from raw.record import GeoLifeUser
from raw.record import WRecord
import argparse
from sqlalchemy.sql import func
import csv

import config
engine = config.getEngine()

from sqlalchemy.orm import sessionmaker
Session = sessionmaker()
Session.configure(bind=engine)


def get_users_present_on(weekday):
  session = Session()
  query = session.query(GeoLifeUser.id)
  if weekday is not None:
    result_set = query.filter(
      GeoLifeUser.weekday == weekday
    ).all()

  else:
    result_set = query.all()

  users = set()
  for u, in result_set:
    users.add(u)

  session.close()
  return users


def get_arguments():
  parser = argparse.ArgumentParser(
    description='Extract centroids of user movements.'
  )
  parser.add_argument(
    '-w', '--weekday',
    dest="weekday",
    help=('Numerical indicator of weekday (0 is Monday, 1 is Tuesday, ..., 6'
          ' is Sunday). Default: None (all weekdays will be accounted for).'),
    type=int,
    default=None,
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


if __name__ == "__main__":
  args = get_arguments()
  dry_run = args.dry_run

  # Only users for a particular day will be selected.
  # If this argument is not specified, then all users will be selected.
  weekday = args.weekday

  session = Session()

  users = get_users_present_on(weekday)

  logger.debug("#"*80)
  logger.debug("Users selected: {0}".format(users))

  eta = ETACalculator(len(users), "User iteration")

  with open('centroids.csv', 'w') as csvfile:
    fieldnames = ['user', 'lat', 'long']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for u in users:
      centroid_of_movement = session.query(
        func.avg(WRecord.latitude).label('lat'),
        func.avg(WRecord.longitude).label('long'),
      ).filter(WRecord.user==u).first()

      print("User #{0} has centroid {1}".format(u, centroid_of_movement))
      writer.writerow({
        'user': u,
        'lat': centroid_of_movement.lat,
        'long': centroid_of_movement.long,
      })

      eta.checkpoint()
      logger.info(eta.eta())
