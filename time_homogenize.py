import logging

import progressbar
progressbar.streams.wrap_stderr()

logging.basicConfig(level=logging.DEBUG, filename='/tmp/geolife.timehomo.log')
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.DEBUG)
logger.addHandler(stdout)

from sqlalchemy import Index
from sqlalchemy import and_
from datetime import timedelta
from datetime import time
from datetime import date
from datetime import datetime
from utils import timerange
from utils import timeAdd
from utils import timeDifferenceSeconds
from utils import num_elements_in_time_range
from schema import HomogenizedRecord
from schema import HomogenizedGeoLifeUser
from schema import initialize_table
from schema import RecordsOnOneDay
from raw.record import DayUser
import argparse

import config
engine = config.getEngine()

from sqlalchemy.orm import sessionmaker
Session = sessionmaker()
Session.configure(bind=engine)


class FakeHomogenizedRecord:
  def __init__(self, u, t, lat, lon):
    self.timestamp = t
    self.new_user_id = u
    self.lat = lat
    self.long = lon


class RecentUserRecord:
  def __init__(self, users):
    self.most_recent_record = {}
    
    s = Session()

    logger.info("Preloading RecentUserRecord object")
    records = s.query(RecordsOnOneDay).order_by(RecordsOnOneDay.c.timestamp)

    with progressbar.ProgressBar(max_value=len(users)) as progress:
        for i, u in enumerate(users):
            r = records.filter(RecordsOnOneDay.c.new_user_id == u).first()

            # If this node has no activity within the bounds, skip them.
            if r is None:
              continue

            mutable_record = FakeHomogenizedRecord(
              u=r.new_user_id, t=r.timestamp, lat=r.lat, lon=r.long
            )
            self.most_recent_record[u] = mutable_record

            logger.info("First record for user {0}: {1}".format(u, r))
            progress.update(i)

    s.close()

  def get(self, time, user):
    #logger.debug("Fetching closest record preceeding {0} for user {1}".format(
    #  time, user
    #))
    if not user in self.most_recent_record:
      #logger.debug("Most recent record not available. Querying datebase.")
      s = Session()
      r = s.query(RecordsOnOneDay).filter(
        RecordsOnOneDay.c.timestamp < t,
        RecordsOnOneDay.c.new_user_id == u
      ).order_by(RecordsOnOneDay.c.timestamp).first()
      self.most_recent_record[user] = r
      s.close()

    else:
      r = self.most_recent_record[user]

    #logger.debug("Record: {0}".format(r))
    return r

  def update(self, user, record):
    #logger.debug("Updating most recent record for user {0} from {1}"
    #             " to {2}".format(
    #  user, self.most_recent_record[user], record,
    #))
    self.most_recent_record[user] = record

  def __repr__(self):
    string_rep = ""
    for u in self.most_recent_record:
      r = self.most_recent_record[u]
      string_rep = string_rep + "\n<{0}: User #{1}\tat ({2}, {3})>".format(
        r.timestamp, r.new_user_id, r.lat, r.long
      )
    return string_rep

def get_arguments():
  parser = argparse.ArgumentParser(
    description='Homogenize time deltas between records.'
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
  with progressbar.ProgressBar(max_value=len(users)) as progress:
      for index, u in enumerate(users):
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

          progress.update(index)


if __name__ == "__main__":
  args = get_arguments()
  dry_run = args.dry_run
  delta = args.time_delta

  # Create the Time Homogenized tables.
  if not dry_run:
    initialize_table(engine)
  session = Session()
  logger.info("Loading users...")
  users = set([u for u, in session.query(DayUser.id).filter(and_(
    DayUser.duration>7200,
    DayUser.count>500,
  )).all()])
  logger.info("User loading complete!")

  logger.debug("#"*80)
  logger.debug("Users selected: {0}".format(users))
  most_recent_records = RecentUserRecord(users)
  users_present = set()
  homogenized_records = []

  n = num_elements_in_time_range(start=time.min, end=time.max, step=delta)
  logger.info('Walking through {} time units'.format(n))
  with progressbar.ProgressBar(max_value=n) as progress:

    for index, t in enumerate(timerange(time.min, time.max, delta)):
        logger.debug("="*60)
        logger.debug("Querying for time {0}".format(t))
        record_set = session.query(RecordsOnOneDay).filter(
          RecordsOnOneDay.c.timestamp >= t,
          RecordsOnOneDay.c.timestamp < timeAdd(t, delta),
          RecordsOnOneDay.c.new_user_id.in_(users)
        ).order_by(RecordsOnOneDay.c.timestamp).all()

        i = 0
        for r in record_set:
          if r.new_user_id not in users_present:
            #logger.debug("-"*40)
            #logger.debug("Record from DB: {0}".format(r))
            users_present.add(r.new_user_id)
            mutable_record = FakeHomogenizedRecord(
              u=r.new_user_id, t=t, lat=r.lat, lon=r.long
            )
            homogenized_records.append(mutable_record)
            most_recent_records.update(r.new_user_id, mutable_record)
            i += 1
        logger.debug("{0} records on {1}".format(i, t))

        users_not_present = users - users_present
        logger.debug("Users not present: {0}".format(len(users_not_present)))
        for u in users_not_present:
          #logger.debug("+"*40)
          most_recent_record = most_recent_records.get(t, u)
          most_recent_record.timestamp = t
          homogenized_records.append(most_recent_record)

        logger.info("Adding {0} homogenized records to database".format(
          len(homogenized_records)
        ))
        if not dry_run:
          session.add_all([HomogenizedRecord(
              user=r.new_user_id, latitude=r.lat, longitude=r.long,
              time=r.timestamp,
            ) for r in homogenized_records
          ])
          session.commit()

        users_present.clear()

        assert len(homogenized_records) == len(users), "Number of homogenized records {0} is not equal to the number of users {1}".format(len(homogenized_records), len(users))
        del homogenized_records[:]

        progress.update(index)

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
