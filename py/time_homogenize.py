import logging
logging.basicConfig(
  level=logging.DEBUG,
  filename='/tmp/time_homo.log',
  filemode='w'
)
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.DEBUG)
logger.addHandler(stdout)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import BigInteger
from sqlalchemy import Float
from sqlalchemy import Time
from sqlalchemy import Sequence
from sqlalchemy import SmallInteger
from datetime import timedelta
from datetime import time
from utils import timerange
from utils import timeAdd
from raw.record import GeoLifeUser
from raw.record import WRecord

WEEKDAY_STRINGS = [
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
  "Sunday",
]

Base = declarative_base()
class HomogenizedRecord(Base):
  __tablename__ = "time_homogenized"
  id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
  user = Column(BigInteger)#, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  time = Column(Time)
  weekday = Column(SmallInteger)

  MONDAY = 0
  TUESDAY = 1
  WEDNESDAY = 2
  THURSDAY = 3
  FRIDAY = 4
  SATURDAY = 5
  SUNDAY = 6
  WEEKDAYS = [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY]

  def __repr__(self):
    return "<WeekdayRecord(name={0}, (x,y)=({1}, {2}), datetime={3})>".format(
      self.user, self.latitude, self.longitude,
      self.datetime
    )

class HomogenizedGeoLifeUser(Base):
  __tablename__ = "time_homo_user_weekday_count"
  id = Column(BigInteger, primary_key=True)
  count = Column(Integer)


def initialize_table(engine):
  Base.metadata.create_all(engine)


from sqlalchemy import create_engine
engine = create_engine(
  "{dialect}://{username}:{password}@{host}/{database}".format(
  dialect='postgresql+psycopg2',
  username='postgres',
  password='nope',
  host='localhost',
  database='geolife'
))

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

class RecentUserRecord:
  def __init__(self, users):
    self.most_recent_record = {}
    
    s = Session()
    logger.debug("Preloading RecentUserRecord object")
    for u in users:
      r = s.query(WRecord).filter(WRecord.user == u)\
                          .order_by(WRecord.time).first()
      self.most_recent_record[u] = r
      logger.debug("First record for user {0}: {1}".format(u, r))
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


delta = timedelta(seconds=5)
#delta = timedelta(hours=1)
if __name__ == "__main__":
  initialize_table(engine)
  session = Session()
  for weekday in HomogenizedRecord.WEEKDAYS:
    users = get_users_present_on(weekday)
    most_recent_records = RecentUserRecord(users)
    logger.debug("#"*80)
    logger.debug("Users for {0}: {1}".format(weekday, users))
    users_present = set()
    homogenized_records = []
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
      s = Session()
      s.add_all([HomogenizedRecord(
          user=r.user, latitude=r.latitude, longitude=r.longitude,
          time=r.time, weekday=r.weekday,
        ) for r in homogenized_records
      ])
      s.commit()
      s.close()

      users_present.clear()

      assert len(homogenized_records) == len(users), "Number of homogenized recods {0} is not equal to the number of users {1}".format(len(homogenized_records), len(users))
      del homogenized_records[:]

