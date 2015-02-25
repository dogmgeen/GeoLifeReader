import logging
logging.basicConfig(
  level=logging.DEBUG,
  filename='/tmp/geolife.log',
  filemode='w'
)
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.INFO)
logger.addHandler(stdout)

import geolife
import messages
import sys
from datetime import timedelta
sys.setrecursionlimit(100000)

import time

if __name__ == "__main__":
  start = time.time()
  try:
    sample_dir = "/home/djmvfb/Downloads/reduced/"#"/home/kp/Development/GeoLifeReader/sample/"
    search_date = "2010-05-20"
    num_users = 400
    delta = timedelta(seconds=5)
    geolife_root_directory = geolife.find_geolife_root(sample_dir)
    num_messages = 8640
    logger.info("GeoLife root found at {0}".format(geolife_root_directory))

    processor = geolife.GeoLifeDataset()
    processor.retrieveByDate(date=search_date)\
           .onlyRetrieveSomeUsers(n=num_users)\
           .boundByLocation(north=53.567732, south=18.126, east=122.6, west=73.4)\
           .calculateStatistics()\
           .homogenizeTimeDeltas(delta=delta)\
           .convertToONE(to_file="geolife2one_{0}_{1}users.csv".format(search_date, num_users))

    num_users = len(processor.users)
    duration = processor.metadata["maxTime"]
    msgs = messages.create(
      n=num_messages, num_users=num_users, duration=duration, delta=delta
    )
    msgs.convertToONE("/tmp/msgs.csv")
    msgs.createChitChatFiles(
      num_social_interests=2*num_users,
      social_interests_per_user=5,
      metadata_descriptors_per_msg=4,
      social_interests_file="/tmp/social_interests.csv",
      metadata_descriptors_file="/tmp/metadata.csv"
    )

  except:
    logger.exception("Stuff didn't do")
  """
  finally:
    logger.warning("#"*80)
    logger.warning("DATABASE IS BEING DELETED.")
    logger.warning("BETTER DELETE THIS LINE BEFORE RUNNING THIS ON THE DATA FOR FIVE HOURS.")
    logger.warning("#"*80)
    import os
    os.remove("05d6c855fdfce881d0ddba777c3fcfcd.db")
  """

  duration = time.time() - start
  duration_delta = timedelta(seconds=duration)
  print("Total execution time: {0}".format(duration_delta))
