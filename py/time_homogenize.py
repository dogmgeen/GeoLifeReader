import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("geolife")
#stdout = logging.StreamHandler()
#stdout.setLevel(logging.INFO)
#logger.addHandler(stdout)

from sqlalchemy import Index
from datetime import timedelta
from datetime import time
from utils import timerange
from utils import timeAdd
from utils import ETACalculator
from utils import timeDifferenceSeconds
from utils import num_elements_in_time_range
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


class RecentUserRecord:
  def __init__(self, users):
    self.most_recent_record = {}
    
    s = Session()

    logger.info("Preloading RecentUserRecord object")
    records = s.query(WRecord).order_by(WRecord.time)
    eta = ETACalculator(len(users), name="Earliest User Records")

    for u in users:
      r = records.filter(WRecord.user == u).first()
      self.most_recent_record[u] = r
      logger.info("First record for user {0}: {1}".format(u, r))
      eta.checkpoint()
      logger.info(eta.eta())
    s.close()

  def get(self, time, user):
    logger.debug("Fetching closest record preceeding {0} for user {1}".format(
      time, user
    ))
    if not user in self.most_recent_record:
      logger.debug("Most recent record not available. Querying datebase.")
      s = Session()
      r = s.query(WRecord).filter(
        WRecord.time < t,
        WRecord.user == u
      ).order_by(WRecord.time).first()
      self.most_recent_record[user] = r
      s.close()

    else:
      r = self.most_recent_record[user]

    logger.debug("Record: {0}".format(r))
    return r

  def update(self, user, record):
    logger.debug("Updating most recent record for user {0} from {1}"
                 " to {2}".format(
      user, self.most_recent_record[user], record,
    ))
    self.most_recent_record[user] = record


def get_arguments():
  parser = argparse.ArgumentParser(
    description='Homogenize time deltas between records.'
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
    '-d', '--time-delta',
    dest='time_delta',
    help="Number of seconds that should be between any two consecutive records",
    type=lambda x: timedelta(seconds=int(x)),
    default=timedelta(seconds=5),
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


def verify_time_homogeniety(users, time_delta, db_session):
  logger.info("Verifying time homogeneity between {0} users".format(
    len(users)
  ))
  eta = ETACalculator(len(users), "Time Homogeneity Verification")
  for u in users:
    result_set = db_session.query(HomogenizedRecord.time)\
                           .filter(HomogenizedRecord.user == u)\
                           .order_by(HomogenizedRecord.time)
    previous = None
    i = 0
    for r, in result_set:
      if previous is not None:
        diff = timeDifferenceSeconds(r, previous)
        assert diff == time_delta.total_seconds(), (
          "Time homogeniety was not preserved for user {user}, record #{record}.\n"
          "Expected time delta: {exp}\n"
          "Actual time delta:   {act}".format(
            user=u, record=i, exp=time_delta, act=diff
        ))

      previous = r
      i += 1
    eta.checkpoint()
    logger.info(eta.eta())

if __name__ == "__main__":
  args = get_arguments()
  dry_run = args.dry_run
  weekday = args.weekday
  delta = args.time_delta

  # Create the Time Homogenized tables.
  if not dry_run:
    initialize_table(engine)
  session = Session()

  users = get_users_present_on(weekday)

  logger.debug("#"*80)
  logger.debug("Users selected: {0}".format(users))

  most_recent_records = RecentUserRecord(users)
  users_present = set()
  homogenized_records = []

  n = num_elements_in_time_range(start=time.min, end=time.max, step=delta)
  eta_til_completed_day = ETACalculator(n, "Synthesis")

  for t in timerange(time.min, time.max, delta):
      logger.debug("="*60)
      logger.debug("Querying for time {0}".format(t))
      record_set = session.query(WRecord).filter(
        WRecord.time >= t,
        WRecord.time < timeAdd(t, delta),
        WRecord.user.in_(users)
      ).order_by(WRecord.time).all()
      
      i = 0
      for r in record_set:
        if r.user not in users_present:
          logger.debug("-"*40)
          logger.debug("Record from DB: {0}".format(r))
          users_present.add(r.user)
          r.time = t
          homogenized_records.append(r)
          most_recent_records.update(r.user, r)
          i += 1
      logger.debug("{0} records on {1}".format(i, t))

      users_not_present = users - users_present
      logger.debug("Users not present: {0}".format(users_not_present))
      for u in users_not_present:
        logger.debug("+"*40)
        most_recent_record = most_recent_records.get(t, u)
        most_recent_record.time = t
        homogenized_records.append(most_recent_record)

      logger.info("Adding {0} homogenized records to database".format(
        len(homogenized_records)
      ))
      if not dry_run:
        session.add_all([HomogenizedRecord(
            user=r.user, latitude=r.latitude, longitude=r.longitude,
            time=r.time, weekday=r.weekday,
          ) for r in homogenized_records
        ])
        session.commit()

      users_present.clear()

      assert len(homogenized_records) == len(users), "Number of homogenized records {0} is not equal to the number of users {1}".format(len(homogenized_records), len(users))
      del homogenized_records[:]

      eta_til_completed_day.checkpoint()
      logger.info(eta_til_completed_day.eta())

  # Create the indices needed for fast performance!
  logger.info("Creating index on raw record time columns")
  if not dry_run:
    Index('homo_time', HomogenizedRecord.__table__.c.time).create(engine)
    Index('homo_user', HomogenizedRecord.__table__.c.user).create(engine)
    Index(
      'homo_usertime',
      HomogenizedRecord.__table__.c.time,
      HomogenizedRecord.__table__.c.user
    ).create(engine)

  #verify_time_homogeniety(users=users, time_delta=delta, db_session=session)

