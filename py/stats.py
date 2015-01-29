import logging
logger = logging.getLogger("geolife.stats")
import user
from record import LinkedRecords

class StatisticsCalculator:
  def __init__(self, result_set):
    self.raw_result_set = result_set

  def run(self):
    users = self.categorizeByUsers()

    # Verify the records for each user are in order, and build linked
    #  -list-style records for that user's records.
    self.all_linked_records = []
    for u in users:
      u.sort()
      self.all_linked_records.append(LinkedRecords(u))

    # Determine the record with the earliest timestamp
    self.min_time = min([
      u[0].datetime for u in users
    ])
    logger.info("Minimum time record occurs at {0}".format(
      self.min_time
    ))

    # Determine the record with the latest timestamp
    self.max_time = max([
      u[-1].datetime for u in users
    ])
    logger.info("Maximum time record occurs at {0}".format(
      self.max_time
    ))

    # Minimum difference between consecutive elements
    self.min_time_delta = min([
      r.getMinTimeDelta() for r in self.all_linked_records
    ])
    logger.info("Smallest time interval is {0} seconds".format(
      self.min_time_delta
    ))


  def categorizeByUsers(self):
    # Determine the unique user IDs within the result set.
    users = user.from_Query(self.raw_result_set)
    logger.info("Found {0} users in result set".format(len(users)))
    return users
 
