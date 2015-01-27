import os
import csv
from datetime import datetime
from record import GeoLifeRecord

SCHEMA = ["lat", "long", "not_needed", "alt", "days_since_1900", "date", "time"]
GEOLIFE_DATE_FMT = "%Y-%m-%d %H:%M:%S" 


class GeoLifeFile:
  def __init__(self, url, user):
    self.user = user
    self.url = url

  def __iter__(self):
    with open(self.url) as f:
      # Skip the first six lines, as they are useless.
      for i in range(6):
        f.readline()

      reader = csv.DictReader(f, fieldnames=SCHEMA)
      for entry in reader:
        timestamp = datetime.strptime(
          "{date} {time}".format(**entry),
          GEOLIFE_DATE_FMT
        )
        yield GeoLifeRecord(
          user=self.user.id,
          latitude=entry["lat"],
          longitude=entry["long"],
          datetime=timestamp
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


