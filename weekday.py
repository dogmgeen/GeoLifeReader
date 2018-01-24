# The purpose of this script is to take the daily SQL records from Geolife and
#  compress them so that the resulting dataset spans seven days. This will be
#  accomplished by faking the existence of additional users. For instance, if
#  user A had trajectories on Monday, March 7th, and Monday, March 14th, then
#  the resulting dataset would have user A1 corresponding to the records for
#  Monday, March 7th, and user A2 would have records corresponding to Monday,
#  March 14th.

import logging
import sys
logger = logging.getLogger("geolife.weekday")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

from record import GeoLifeRecord
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Sequence
from sqlalchemy import Float
from sqlalchemy import DateTime
from sqlalchemy import SmallInteger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime

WEEKDAY_VALUES = [
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
  "Sunday",
]


Base = declarative_base()
class WeekdayRecord(Base):
  __tablename__ = "weekday"
  id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
  user = Column(Integer)#, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  datetime = Column(DateTime)#, index=True)
  weekday = Column(SmallInteger)

  def __repr__(self):
    return "<WeekdayRecord(name={0}, (x,y)=({1}, {2}), datetime={3})>".format(
      self.user, self.latitude, self.longitude,
      self.datetime
    )


class TimingInfo:
    def __init__(self, total_checkpoints):
      self.start = datetime.datetime.now()
      self.i = 0
      self.n = total_checkpoints
      self.avg = 0

    def checkpoint(self):
      duration = datetime.datetime.now() - self.start
      seconds = duration.total_seconds()

      self.avg += (seconds - self.avg)/(self.i + 1)
      self.i += 1

      self.start = datetime.datetime.now()
      return duration

    def eta(self):
      remaining_checkpoints = self.n - self.i
      eta_seconds = self.avg * remaining_checkpoints
      eta = datetime.timedelta(seconds=eta_seconds)
      return eta


if __name__ == "__main__":
  # Load raw GeoLife records.
  logger.info("Connecting to database")
  engine = create_engine(
      "{dialect}://{username}:{password}@{host}/{database}".format(
      dialect='mysql',
      username='kp',
      password='nope',
      host='localhost',
      database='geolife'
  ))
  Base.metadata.create_all(engine)

  Session = sessionmaker()
  Session.configure(bind=engine)
  session = Session()

  #############################################################################
  ## DATA LOADING ##
  ##################
  #
  logger.info("Counting records...")
  n = session.query(GeoLifeRecord).count()
  logger.info("There are {n} records in the GeoLifeRecord table".format(n=n))

  i = 0
  new_data = []
  # This is a Monday
  origin_date = datetime.date(year=2013, month=5, day=20)
  assert origin_date.weekday() == 0, (
    "Origin date {0} is not a Monday".format(origin_date)
  )

  DELTA_CHECKPOINT = 25000
  timing_info = TimingInfo(total_checkpoints=int(float(n)/DELTA_CHECKPOINT))

  logger.info("Querying raw database for all GeoLifeRecords")
  lastID = -1
  query = session.query(GeoLifeRecord)
  while True:
    result = query.filter(GeoLifeRecord.id > lastID).limit(DELTA_CHECKPOINT).all()
    if not result or len(result) == 0:
      break

    for r in result:
      lastID = r.id

      datetime_suffix = r.datetime.strftime("%y%m%d")
      new_user_id = int("{0}{1}".format(datetime_suffix, r.user))

      weekday = r.date.weekday()
      days_after_origin_date = datetime.timedelta(days=weekday)
      synthesized_date = origin_date + days_after_origin_date
      synthesized_datetime = datetime.datetime.combine(synthesized_date, r.time)
      assert synthesized_datetime.weekday() == weekday, (
        "Synthesized datetime {0} does not fall on day {1}".format(
          synthesized_datetime, WEEKDAY_VALUES[weekday]
      ))

      new_data.append(WeekdayRecord(
        user=new_user_id,
        latitude=r.latitude,
        longitude=r.longitude,
        datetime=synthesized_datetime,
        weekday=weekday,
      ))

      another_set_of_records_complete = (i % DELTA_CHECKPOINT == 0)
      if another_set_of_records_complete:
        logger.info("{0:.4%} complete ({1} out of {2} records)".format(
          float(i)/n, i, n
        ))
        session.add_all(new_data)
        session.commit()
        del new_data[:]
        duration = timing_info.checkpoint()
        logger.info("{0} since last commit | ETA: {1}".format(
          duration, timing_info.eta()
        ))
      i += 1

  logger.info("Final committing of data ({0} records)".format(len(new_data)))
  session.add_all(new_data)
  session.commit()
  del new_data[:]
  logger.info("Data loading complete!")

