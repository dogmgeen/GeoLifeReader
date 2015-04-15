import logging
logger = logging.getLogger("geolife.file")
import os
import csv
from utils import timestamp2datetime
from utils import convertToBeijingTime
#from record import GeoLifeRecord
from record import WRecord as GeoLifeRecord
from collections import defaultdict
from datetime import datetime

SCHEMA = ["lat", "long", "not_needed", "alt", "days_since_1900", "date", "time"]
FILENAME_DATE_FMT = "%Y%m%d%H%M%S"

class GeoLifeFile:
  def __init__(self, url, user):
    logger.debug("Initializing GeoLifeFile at {0}".format(url))
    self.user = user.id
    self.url = url
    self.weekday_counts = defaultdict(int)

  def restrictRecordsTo(self, aoi, weekday=None):
    self.active_weekday = weekday
    self.latitude_min = aoi['south']
    self.latitude_max = aoi['north']
    self.longitude_min = aoi['west']
    self.longitude_max = aoi['east']

  def occursOn(self, weekday):
    filename = os.path.basename(self.url).split(".")[0]
    date_from_filename = datetime.strptime(filename, FILENAME_DATE_FMT)
    return (date_from_filename.weekday() == weekday)

  def __iter__(self):
    with open(self.url) as f:
      # Skip the first six lines, as they are useless.
      for i in range(6):
        f.readline()

      reader = csv.DictReader(f, fieldnames=SCHEMA)
      for entry in reader:
        # Timestamps were in GMT. Since the majority of movement occurs in
        #  Beijing, it is important to shift the actual time to local time.
        d = convertToBeijingTime(timestamp2datetime(entry))

        valid_conditions = [
          self.latitude_min < float(entry["lat"]) < self.latitude_max,
          self.longitude_min < float(entry["long"]) < self.longitude_max,
        ]
        if self.active_weekday is not None:
          weekday = d.weekday()
          valid_conditions.append(self.active_weekday == weekday)

        if all(valid_conditions):
          datetime_suffix = d.strftime("%Y%m%d")

          new_user_id = int("{0}{1}".format(datetime_suffix, self.user))
          key = (new_user_id, d.weekday())

          self.weekday_counts[key] += 1
          yield GeoLifeRecord(
            user=new_user_id,
            latitude=entry["lat"],
            longitude=entry["long"],
            time=d.time(),
            weekday=d.weekday(),
          )

        else:
          pass


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

