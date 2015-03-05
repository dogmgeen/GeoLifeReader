import logging
logger = logging.getLogger("geolife.utils")
from datetime import datetime
from datetime import timedelta
import os

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

def timerange(start, end, step=timedelta(seconds=1)):
  dummy_date = datetime.now().date()
  start_date = datetime.combine(dummy_date, start)
  end_date = datetime.combine(dummy_date, end)
  
  element = start_date
  while element < end_date:
    yield element.time()
    element += step

def timeAdd(t, delta):
  dummy_date = datetime.now().date()
  return (datetime.combine(dummy_date, t) + delta).time()
  

class ETACalculator:
  def __init__(self, iterations):
    self.n = iterations
    self.i = 0
    self.start = datetime.now()
    self.avg = 0

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
    return ("Average:   {0}\n"
            "ETA:       {1}".format(average, eta))


from pympler import summary, muppy
import psutil

def get_virtual_memory_usage_kb():
    """
    The process's current virtual memory size in Kb, as a float.

    """
    return float(psutil.Process().memory_info_ex().vms) / 1024.0

def memory_usage(where):
    """
    Print out a basic summary of memory usage.

    """
    mem_summary = summary.summarize(muppy.get_objects())
    print "Memory summary:", where
    summary.print_(mem_summary, limit=2)
    print "VM: %.2fMb" % (get_virtual_memory_usage_kb() / 1024.0)


