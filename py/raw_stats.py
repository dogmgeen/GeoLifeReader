# Report some statistics about the raw GeoLife dataset.
#  Number of participants per day

import logging
logging.basicConfig(level=logging.DEBUG, filename='/tmp/geolife.rawstats.log')
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.DEBUG)
logger.addHandler(stdout)

import config
engine = config.getEngine()

from sqlalchemy.orm import sessionmaker
Session = sessionmaker()
Session.configure(bind=engine)


from raw.record import RawRecord
from collections import defaultdict
from utils import ETACalculator
if __name__ == "__main__":
  session = Session()
  records = session.query(RawRecord)
  print("Counting number of records...")
  n = records.count()
  print("{0} records to iterate over".format(n))
  eta = ETACalculator(n, name="Record iteration")
  print("="*80)
  unique_users_per_day = defaultdict(set)
  unique_records_per_day = defaultdict(int)
  for r in records.all():
    user_id = r.user
    date = r.date

    # Add the user id to the set of unique users per day.
    unique_users_per_day[date].add(user_id)
    unique_records_per_day[date] += 1
    eta.checkpoint()
    print(eta.eta())
  
  print("Number of unique users per day:")
  with open("unique_users_per_day.csv", "w") as outfile:
    for d in unique_users_per_day:
      print("{0}\t=>\t{1}".format(d, len(unique_users_per_day[d])))
      outfile.write("{0}\t{1}\n".format(d, len(unique_users_per_day[d])))
  
  print("#"*80)
  print("Number of unique records per day:")
  with open("unique_records_per_day.csv", "w") as numrecordsfile:
    for d in unique_records_per_day:
      print("{0}\t=>\t{1}".format(d, unique_records_per_day[d]))
      numrecordsfile.write("{0}\t{1}\n".format(d, unique_records_per_day[d]))


