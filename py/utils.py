import logging
logger = logging.getLogger("geolife.utils")
from datetime import datetime
from datetime import timedelta
import os
import pytz

GEOLIFE_DATE_FMT = "%Y-%m-%d %H:%M:%S"

def timestamp2datetime(entry):
  return datetime.strptime(
    "{date} {time}".format(**entry),
    GEOLIFE_DATE_FMT
  )

GMT = pytz.timezone('GMT')
beijing_timezone = pytz.timezone('Asia/Shanghai')
def convertToBeijingTime(d):
  gmt_datetime = GMT.localize(d)
  beijing_datetime = gmt_datetime.astimezone(beijing_timezone)
  return beijing_datetime
  

def datetimerange(start, end, step=timedelta(seconds=1)):
  # start: datetime object
  # end:   datetime object
  # step:  timedelta object
  element = start
  while element < end:
    yield element
    element += step

def timerange(start, end, step=timedelta(seconds=1)):
  start_date = time2datetime(start)
  end_date = time2datetime(end)
  
  element = start_date
  while element < end_date:
    yield element.time()
    element += step

def timeAdd(t, delta):
  return (time2datetime(t) + delta).time()

def time2datetime(t):
  dummy_date = datetime.now().date()
  return datetime.combine(dummy_date, t)

def timeDifferenceSeconds(t1, t2):
  if t1 <= t2:
    return (time2datetime(t2) - time2datetime(t1)).total_seconds()

  else:
    return timeDifferenceSeconds(t2, t1)


def num_elements_in_time_range(start, end, step):
  timespan_seconds = timeDifferenceSeconds(start, end)
  step_seconds = step.total_seconds()
  return int(timespan_seconds/float(step_seconds))


class ETACalculator:
  def __init__(self, iterations, name=None):
    self.n = iterations
    self.i = 0
    self.start = datetime.now()
    self.avg = 0
    self.name = name

  def init(self):
    self.start = datetime.now()

  def checkpoint(self):
    duration = datetime.now() - self.start
    self.avg = (duration.total_seconds() + self.i*self.avg)/(self.i+1)
    self.init()
    self.i += 1

  def eta(self):
    eta_seconds = self.avg*(self.n - self.i)
    average = timedelta(seconds=self.avg)
    eta = timedelta(seconds=int(eta_seconds))
    return ("===== {0} =====\n"
            "Average:   {1}\n"
            "ETA:       {2}\n"
            "{3} out of {4}".format(self.name, average, eta, self.i, self.n))

