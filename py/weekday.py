# The purpose of this script is to take the daily SQL records from Geolife and
#  compress them so that the resulting dataset spans seven days. This will be
#  accomplished by faking the existence of additional users. For instance, if
#  user A had trajectories on Monday, March 7th, and Monday, March 14th, then
#  the resulting dataset would have user A1 corresponding to the records for
#  Monday, March 7th, and user A2 would have records corresponding to Monday,
#  March 14th.

import logging
logger = logging.getLogger("geolife.weekday")
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


if __name__ == "__main__":
  # Load raw GeoLife records.
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
  original_data = session.query(GeoLifeRecord)
  n = original_data.count()
  print("There are {n} records in the GeoLifeRecord table".format(n=n))

  i = 0
  new_data = []
  # This is a Monday
  origin_date = datetime.date(year=2013, month=5, day=20)
  assert origin_date.weekday() == 0, (
    "Origin date {0} is not a Monday".format(origin_date)
  )

  for r in original_data:
    datetime_suffix = r.datetime.strftime("%y%m%d")
    new_user_id = int("{0}{1}".format(datetime_suffix, r.user))

    days_after_origin_date = timedelta(days=r.weekday)
    synthesized_date = origin_date + days_after_origin_date
    synthesized_datetime = datetime.combine(synthesized_date, r.time)
    assert synthesized_datetime.weekday() == r.weekday, (
      "Synthesized datetime {0} does not fall on day {1}".format(
        synthesized_datetime, WEEKDAY_VALUES[r.weekday]
    ))

    new_data.append(WeekdayRecord(
      user=new_user_id,
      latitude=r.latitude,
      longitude=r.longitude,
      datetime=synthesized_datetime,
      weekday=r.weekday,
    ))

    another_5_percent_complete = (int(float(i)*100/n) % 5 == 0)
    if another_5_percent_complete:
      print("{0:.2%} complete ({1} out of {2} records)".format(
        float(i)/n, i, n
      ))
      session.add_all(new_data)
      session.commit()
      del new_data[:]

    i += 1

  print("Final committing of data ({0} records)".format(len(new_data)))
  session.add_all(new_data)
  session.commit()
  del new_data[:]
  print("Data loading complete!")

  #############################################################################
  ## DATA VERIFYING ##
  ####################
  #
  new_data = session.query(WeekdayRecord)
  # Filter by each day.
  for weekday in range(len(WEEKDAY_VALUES)):
    # datetime's strptime format does not support ISO weekdays, but
    #  instead uses 0 for Monday, 6 for Sunday. Thus, the weekday
    #  values need to be decremented.
    weekday_str = WEEKDAY_VALUES[weekday]
    logger.info("Comparing size of data for {0}".format(weekday_str))

    original_weekday_data = original_data.filter(GeoLifeRecord.weekday==weekday)
    new_weekday_data = new_data.filter(WeekdayRecord.weekday==weekday)
    original_data_size = original_data.count()
    new_data_size = new_weekday_data.count()
    assert original_data_size == new_data_size, (
      "Original data has {0} records for {1}, but new data has {2}"
      " records".format(original_data_size, weekday_str, new_data_size)
    )

