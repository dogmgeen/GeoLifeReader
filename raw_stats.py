# Report some statistics about the raw GeoLife dataset.
#  Number of participants per day

import logging
logging.basicConfig(level=logging.DEBUG, filename='/tmp/geolife.rawstats.log')
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.DEBUG)
logger.addHandler(stdout)
import os
import geolife
from raw import user
from collections import defaultdict
from utils import ETACalculator
import argparse
# Parse the command-line arguments.
def get_arguments():
  parser = argparse.ArgumentParser(
    description='Populate local database with each raw trajectory.'
  )
  parser.add_argument(
    '-i', '--input_directory',
    dest="input_directory",
    help='Directory containing PLT files (default: current working directory)',
    type=geolife.find_geolife_root,
    required=True,
  )

  args = parser.parse_args()
  return args




if __name__ == "__main__":
  args = get_arguments()
  directory = args.input_directory
  eta = ETACalculator(iterations=geolife.get_num_files(directory))

  unique_users_per_day = defaultdict(set)
  unique_records_per_day = defaultdict(int)
  for u in user.from_directory(directory):

    logger.info("Beginning yielding of records from user {0.id}".format(u))
    for f in u.files:
      for r in f:
        user_id = r.user
        date = r.date

        # Add the user id to the set of unique users per day.
        unique_users_per_day[date].add(user_id)
        unique_records_per_day[date] += 1

      eta.checkpoint()
      logger.info("="*60)
      logger.info("File {0}".format(os.path.basename(f.url)))
      logger.info(eta.eta())
      u.num_records += f.num_records

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


