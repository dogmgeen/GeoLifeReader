import logging
logging.basicConfig(
  level=logging.INFO,
  filename='/tmp/geolife.log',
  filemode='w'
)
logger = logging.getLogger("geolife")
logger.addHandler(logging.StreamHandler())

import geolife

if __name__ == "__main__":
  try:
    sample_dir = "/home/djmvfb/Development/GeoLifeReader/"
    geolife_root_directory = geolife.find_geolife_root(sample_dir)
    logger.info("GeoLife root found at {0}".format(geolife_root_directory))

    geolife.GeoLifeDataset(geolife_root_directory)\
           .retrieveByDate(date="2008-11-09")\
           .boundByLocation(north=53.567732, south=18.126, east=122.6, west=73.4)
    """
     .homogenizeTimeDeltas(time_delta="")\
     .convertToONE(to_file="")\
    """

  except:
    logger.exception("Stuff didn't do")

  finally:
    logger.warning("#"*80)
    logger.warning("DATABASE IS BEING DELETED.")
    logger.warning("BETTER DELETE THIS LINE BEFORE RUNNING THIS ON THE DATA FOR FIVE HOURS.")
    logger.warning("#"*80)
    import os
    os.remove("05d6c855fdfce881d0ddba777c3fcfcd.db")
