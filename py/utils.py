from datetime import datetime

GEOLIFE_DATE_FMT = "%Y-%m-%d %H:%M:%S"

def timestamp2datetime(entry):
  return datetime.strptime(
    "{date} {time}".format(**entry),
    GEOLIFE_DATE_FMT
  )

