from datetime import timedelta
from sqlalchemy import create_engine

ALL_CHINA = {
  "north": 53.567732,
  "south": 18.126,
  "east": 122.6,
  "west": 73.4,
}

BEIJING = {
  "north": 41.1398565,
  "south": 38.5089264,
  "east": 118.3662329,
  "west": 115.3983897,
}

TRACE_DESTINATION_DIRECTORY = "/home/djmvfb/one"
TRACE_FILENAME_FORMAT = "geolife2one_{0}_{1}users.csv"
DELTA = timedelta(seconds=5)
NUM_MESSAGES = 5000
NUM_USERS = 20

def getEngine():
  return create_engine(
    "{dialect}://{username}:{password}@{host}/{database}".format(
    dialect='postgresql+psycopg2',
    username='postgres',
    password='nope',
    host='localhost',
    database='geolife'
  ))

