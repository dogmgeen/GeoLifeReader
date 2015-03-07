# Run this code to load only a small subset of the PLT files into the database.
#  This code should not be used for further research, but just to demonstrate
#  the underlying code is fully functional.

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
import tempfile
from raw import pltfile
import random
import os
import shutil
import sys
random.seed(0)

def make_subset_copy(src, dst):
    pltfiles = pltfile.get_plt_files(src)
    small_subset_of_pltfiles = random.sample(pltfiles, 100)
    for f in small_subset_of_pltfiles:
      logger.debug(f)
      # Isolate the subdirectory that will need to be created in the temporary
      #  destination to preserve user information.
      directory = os.path.dirname(f)
      destination_subpath = directory.split(src+"/")[1]
      logger.debug("{0} will be created in {1}".format(
        destination_subpath, sample_dir
      ))

      # Create subdirectories if not present, and copy file.
      destination_path = os.path.join(dst, destination_subpath)
      logger.debug("Create path {0} if non-existent".format(destination_path))
      try:
        os.makedirs(destination_path)
      except:
        pass
      shutil.copy(f, destination_path)


if __name__ == "__main__":
  start = time.time()
  try:
    # To lighten the load of demonstrating the underlying code works,
    #  the following code will copy only a small subset of the GeoLife
    #  dataset for loading into the database.

    # Copy a subset of the actual GeoLife dataset into a temporary directory.
    import sys
    geolife_source = sys.argv[-1]
    geolife_root_directory = geolife.find_geolife_root(geolife_source)
    logger.info("PLT files will come from {0}".format(geolife_root_directory))

    sample_dir = tempfile.mkdtemp()
    logger.info("Selected PLT files will be copied to {0}".format(sample_dir))

    make_subset_copy(src=geolife_root_directory, dst=sample_dir)

  except:
    logger.exception("Stuff didn't do")

  duration = time.time() - start
  duration_delta = timedelta(seconds=duration)
  print("Total execution time: {0}".format(duration_delta))
