import logging
logger = logging.getLogger("geolife.stats")
import extent

class StatisticsCalculator:
  def __init__(self, users):
    self.users = users
    self.min_longitude = None
    self.max_longitude = None
    self.min_latitude = None
    self.max_latitude = None

  def run(self):
    # Determine the record with the earliest timestamp
    self.min_time = min([
      u[0].datetime for u in self.users
    ])
    logger.info("Minimum time record occurs at {0}".format(
      self.min_time
    ))

    # Determine the record with the latest timestamp
    self.max_time = max([
      u[-1].datetime for u in self.users
    ])
    logger.info("Maximum time record occurs at {0}".format(
      self.max_time
    ))

    # Min and Max lat/longs.
    ext = reduce(
      extent.extendExtents, 
      [u.getExtent() for u in self.users],
    )
    self.min_longitude, self.max_longitude, self.min_latitude, self.max_latitude = ext.asMinMaxLongLatTuple()

