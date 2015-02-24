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
import time
from datetime import timedelta

if __name__ == "__main__":
  start = time.time()
  try:
    sample_dir = "/home/djmvfb/Downloads/geo/"#"/home/kp/Development/GeoLifeReader/sample/"
    geolife_root_directory = geolife.find_geolife_root(sample_dir)
    logger.info("GeoLife root found at {0}".format(geolife_root_directory))

    processor = geolife.GeoLifeDataset(directory=geolife_root_directory)

  except:
    logger.exception("Stuff didn't do")

  duration = time.time() - start
  duration_delta = timedelta(seconds=duration)
  print("Total execution time: {0}".format(duration_delta))
