import logging
logger = logging.getLogger("geolife.file")
import os
import csv
from utils import timestamp2datetime
#from record import GeoLifeRecord
from record import WRecord as GeoLifeRecord
import datetime

SCHEMA = ["lat", "long", "not_needed", "alt", "days_since_1900", "date", "time"]

origin_date = datetime.date(year=2013, month=5, day=20)



class GeoLifeFile:
  def __init__(self, url, user):
    logger.debug("Initializing GeoLifeFile at {0}".format(url))
    self.user = user.id
    self.url = url

  def __iter__(self):
    with open(self.url) as f:
      # Skip the first six lines, as they are useless.
      for i in range(6):
        f.readline()

      reader = csv.DictReader(f, fieldnames=SCHEMA)
      for entry in reader:
        d = timestamp2datetime(entry)
        datetime_suffix = d.strftime("%y%m%d")

        new_user_id = int("{0}{1}".format(datetime_suffix, self.user))
        weekday = d.weekday()
        days_after_origin_date = datetime.timedelta(days=weekday)
        synthesized_date = origin_date + days_after_origin_date
        synthesized_datetime = datetime.datetime.combine(
          synthesized_date, d.time()
        )
        yield GeoLifeRecord(
          user=new_user_id,
          latitude=entry["lat"],
          longitude=entry["long"],
          datetime=synthesized_date,
          weekday=weekday,
        )
  

def load_from_directory(directory, user):
  return [GeoLifeFile(url=f, user=user) for f in get_plt_files(directory)]


# Recursively search the input directory for PLT files, and build a list
#  of absolute paths for these files.
def get_plt_files(root_directory):
  plt_files = []
  for dirName, subdirectories, files in os.walk(root_directory):

    # If the filename ends with .plt, then its full relative path should
    #  be added to the list of plt files.
    plt_files.extend([
      os.path.join(dirName, f) for f in files if f.lower().endswith(".plt")
    ])
  return plt_files


