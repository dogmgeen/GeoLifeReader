import logging
logging.basicConfig(
  level=logging.DEBUG,
  filename='/tmp/histogram.log',
  filemode='w'
)
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.INFO)
logger.addHandler(stdout)

from datetime import timedelta
from datetime import time
from utils import timerange
from utils import timeAdd
from utils import timeDifferenceSeconds
from utils import num_elements_in_time_range
from utils import ETACalculator
from raw.record import GeoLifeUser
from raw.record import WRecord
from schema import HomogenizedRecord
from schema import HomogenizedGeoLifeUser
from schema import initialize_table
import argparse

import config
engine = config.getEngine()

from sqlalchemy.orm import sessionmaker
Session = sessionmaker()
Session.configure(bind=engine)


def get_users_present_on(weekday):
  session = Session()
  result_set = session.query(GeoLifeUser.id).filter(
    GeoLifeUser.weekday == weekday
  ).all()
  users = set()
  for u, in result_set:
    users.add(u)

  session.close()
  return users


def get_arguments():
  parser = argparse.ArgumentParser(
    description='Homogenize time deltas between records.'
  )
  parser.add_argument(
    '-w', '--weekday',
    dest="weekday",
    help='Numerical indicator of weekday (0 is Monday, 1 is Tuesday, ..., 6 is Sunday)',
    type=int,
    default=0,
  )
  parser.add_argument(
    '-d', '--time-delta',
    dest='time_delta',
    help="Number of seconds that should be between any two consecutive records",
    type=lambda x: timedelta(seconds=int(x)),
    default=timedelta(seconds=5),
  )

  args = parser.parse_args()
  return args


import csv
if __name__ == "__main__":
  args = get_arguments()
  weekday = args.weekday
  delta = args.time_delta

  initialize_table(engine)
  session = Session()

  users = get_users_present_on(weekday)
  n = num_elements_in_time_range(start=time.min, end=time.max, step=delta)
  eta_til_completed_day = ETACalculator(n, "Synthesis")
  with open("time_histogram.csv", "w") as csv_file:
    histogram_file = csv.DictWriter(csv_file, ["time", "count"])
    histogram_file.writeheader()
    for t in timerange(time.min, time.max, delta):
      logger.debug("="*60)
      logger.debug("Querying for time {0}".format(t))
      number_of_records = session.query(WRecord).filter(
        WRecord.time >= t,
        WRecord.time < timeAdd(t, delta),
        WRecord.user.in_(users)
      ).order_by(WRecord.time).count()
      
      i = 0
      histogram_file.writerow({"time": t, "count": number_of_records})

      eta_til_completed_day.checkpoint()
      logger.info(eta_til_completed_day.eta())

