import logging
logging.basicConfig(
  level=logging.DEBUG,
  filename='/tmp/time_homo.log',
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
from utils import ETACalculator
from utils import timeDifferenceSeconds
from raw.record import GeoLifeUser
from raw.record import WRecord
from schema import HomogenizedRecord
from schema import HomogenizedGeoLifeUser
from schema import initialize_table


from sqlalchemy import create_engine
engine = create_engine(
  "{dialect}://{username}:{password}@{host}/{database}".format(
  dialect='postgresql+psycopg2',
  username='postgres',
  password='nope27rola',
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
  total_eta = ETACalculator(len(HomogenizedRecord.WEEKDAYS))

  for weekday in HomogenizedRecord.WEEKDAYS:
    users = get_users_present_on(weekday)
    most_recent_records = RecentUserRecord(users)
    logger.debug("#"*80)
    logger.debug("Users for {0}: {1}".format(weekday, users))
    users_present = set()
    homogenized_records = []

    def num_elements_in_time_range(start, end, step):
      timespan_seconds = timeDifferenceSeconds(start, end)
      step_seconds = step.total_seconds()
      return int(timespan_seconds/float(step_seconds))

    n = num_elements_in_time_range(start=time.min, end=time.max, step=delta)
    eta_til_completed_day = ETACalculator(n)

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
      session.add_all([HomogenizedRecord(
          user=r.user, latitude=r.latitude, longitude=r.longitude,
          time=r.time, weekday=r.weekday,
        ) for r in homogenized_records
      ])
      session.commit()

      users_present.clear()

      assert len(homogenized_records) == len(users), "Number of homogenized recods {0} is not equal to the number of users {1}".format(len(homogenized_records), len(users))
      del homogenized_records[:]

      eta_til_completed_day.checkpoint()
      logger.info(eta_til_completed_day.eta())

    total_eta.checkpoint()
    logger.info("#"*80)
    logger.info("#"*80)
    logger.info("#"*80)
    logger.info(total_eta.eta())
    logger.info("#"*80)
    logger.info("#"*80)
    logger.info("#"*80)

