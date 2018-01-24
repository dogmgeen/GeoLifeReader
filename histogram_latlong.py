import logging
logging.basicConfig(
  level=logging.DEBUG,
)
logger = logging.getLogger("geolife")

from utils import num_elements_in_time_range
from utils import ETACalculator
from raw.record import GeoLifeUser
from raw.record import WRecord
from schema import initialize_table

import config
engine = config.getEngine()

from sqlalchemy.orm import sessionmaker
Session = sessionmaker()
Session.configure(bind=engine)

STEP = 0.01

def drange(start, stop, step):
  r = start
  while r < stop:
    yield r
    r += step

import csv
if __name__ == "__main__":
  initialize_table(engine)
  session = Session()

  lat_values = drange(config.BEIJING["south"], config.BEIJING["north"], STEP)
  n = len([l for l in lat_values])
  lat_values = drange(config.BEIJING["south"], config.BEIJING["north"], STEP)
  eta_til_completed_day = ETACalculator(n, "Latitudes")
  with open("latitude_histogram.csv", "w") as csv_file:
    histogram_file = csv.DictWriter(csv_file, ["latitude", "count"])
    histogram_file.writeheader()
    for l in lat_values:
      number_of_records = session.query(WRecord).filter(
        WRecord.latitude >= l,
        WRecord.latitude < l+STEP,
      ).count()
      
      histogram_file.writerow({"latitude": l, "count": number_of_records})

      eta_til_completed_day.checkpoint()
      logger.info(eta_til_completed_day.eta())

  long_values = drange(config.BEIJING["west"], config.BEIJING["east"], STEP)
  n = len([l for l in long_values])
  long_values = drange(config.BEIJING["west"], config.BEIJING["east"], STEP)
  eta_til_completed_day = ETACalculator(n, "Longitude")
  with open("longitude_histogram.csv", "w") as csv_file:
    histogram_file = csv.DictWriter(csv_file, ["longitude", "count"])
    histogram_file.writeheader()
    for l in long_values:
      number_of_records = session.query(WRecord).filter(
        WRecord.longitude >= l,
        WRecord.longitude < l+STEP,
      ).count()
      
      histogram_file.writerow({"longitude": l, "count": number_of_records})

      eta_til_completed_day.checkpoint()
      logger.info(eta_til_completed_day.eta())

