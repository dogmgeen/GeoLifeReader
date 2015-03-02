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
from record import WRecord
import os

from config import TRACE_FILENAME_FORMAT
from config import TRACE_DESTINATION_DIRECTORY
from config import DELTA
from config import NUM_USERS
from config import NUM_MESSAGES
from config import BEIJING


if __name__ == "__main__":
  
  for d in WRecord.WEEKDAYS:
    outfile = os.path.join(
      TRACE_DESTINATION_DIRECTORY,
      TRACE_FILENAME_FORMAT.format(d, NUM_USERS)
    )

    processor = geolife.GeoLifeDataset()
    processor.retrieveByWeekday(weekday=d)\
           .removeUsersWithTooFewRecords(min_records=4, weekday=d)\
           .onlyRetrieveSomeUsers(n=NUM_USERS)\
           .boundByLocation(**BEIJING)\
           .calculateStatistics()\
           .homogenizeTimeDeltas(delta=DELTA)\
           .convertToONE(to_file=outfile)\
           .close()

