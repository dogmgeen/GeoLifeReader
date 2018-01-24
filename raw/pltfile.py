import logging
logger = logging.getLogger("geolife.file")
import os
import csv
from utils import timestamp2datetime
from utils import convertToBeijingTime
from raw.record import RawRecord
from datetime import datetime

SCHEMA = ["lat", "long", "not_needed", "alt", "days_since_1900", "date", "time"]
FILENAME_DATE_FMT = "%Y%m%d%H%M%S"


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


class GeoLifeFile:
  def __init__(self, url, user):
    logger.debug("Initializing GeoLifeFile at {0}".format(url))
    self.user = user.id
    self.url = url
    self.num_records = 0

  def __iter__(self):
    with open(self.url) as f:
      # Skip the first six lines, as they are useless.
      for i in range(6):
        f.readline()

      reader = csv.DictReader(f, fieldnames=SCHEMA)
      for entry in reader:
        self.num_records += 1

        # Timestamps were in GMT. Since the majority of movement occurs in
        #  Beijing, it is important to shift the actual time to local time.
        d = convertToBeijingTime(timestamp2datetime(entry))
        yield RawRecord(
            user=self.user,
            latitude=entry["lat"],
            longitude=entry["long"],
            time=d.time(),
            date=d.date(),
            date_user_id=int("{0}{1}".format(
              d.strftime("%Y%m%d"), self.user
            ))
        )



