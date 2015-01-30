import logging
logger = logging.getLogger("geolife.utils")
from datetime import datetime
from datetime import timedelta

GEOLIFE_DATE_FMT = "%Y-%m-%d %H:%M:%S"

def timestamp2datetime(entry):
  return datetime.strptime(
    "{date} {time}".format(**entry),
    GEOLIFE_DATE_FMT
  )


def datetimerange(start, end, step=timedelta(seconds=1)):
  # start: datetime object
  # end:   datetime object
  # step:  timedelta object
  element = start
  while element < end:
    yield element
    element += step
