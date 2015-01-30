import logging
logger = logging.getLogger("geolife.stats")

class StatisticsCalculator:
  def __init__(self, users):
    self.users = users

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

    # Minimum difference between consecutive elements
    self.min_time_delta = min([
      u.linked_list.getMinTimeDelta() for u in self.users
    ])
    logger.info("Smallest time interval is {0} seconds".format(
      self.min_time_delta
    ))

