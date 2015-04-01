#Input: Database with time-homogenized records.
#Output: ONE simulator ready files.
#Parameters: Number of users to output.
#            Weekday of interest.
#            Area of interest.
#            Time delta between records.
import logging
logging.basicConfig(
  level=logging.DEBUG,
  filename='/tmp/geolife.log',
  filemode='w'
)
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.DEBUG)
logger.addHandler(stdout)

from schema import HomogenizedRecord
from raw.record import getUserSubset
from raw.record import WEEKDAY_STRINGS
from config import getEngine
from config import BEIJING
from config import DECIMAL_DEGREES_TO_GRID_SCALE
from config import CONFIG_TEMPLATE
from config import CONFIG_FILE
import argparse
from utils import timerange
from utils import ETACalculator
from utils import num_elements_in_time_range
from datetime import time
from datetime import timedelta
from sqlalchemy.orm import sessionmaker
import os
from one import ExternalMovementReaderConverter
from utils import timeDifferenceSeconds
import messages
import pystache

Session = sessionmaker()
engine = getEngine()
Session.configure(bind=engine)


# Parse the command-line arguments.
def get_arguments():
  parser = argparse.ArgumentParser(
    description='Write out files for simulation.'
  )
  parser.add_argument(
    '-o', '--output_directory',
    dest="output_directory",
    help='Directory to store created files (default: ./out)',
    default="./out",
    type=os.path.abspath,
  )
  parser.add_argument(
    '-w', '--weekday',
    dest="weekday",
    help='Numerical indicator of weekday (0 is Monday, 1 is Tuesday, ..., 6 is Sunday)',
    type=int,
    default=0,
  )
  parser.add_argument(
    '-n', '--num-users',
    dest="num_users",
    help='Number of users to simulate',
    type=int,
    default=50,
  )
  parser.add_argument(
    '-d', '--time-delta',
    dest='time_delta',
    help="Number of seconds that should be between any two consecutive records",
    type=lambda x: timedelta(seconds=x),
    default=timedelta(seconds=5),
  )
  parser.add_argument(
    '-m', '--num-messages',
    dest="num_messages",
    help='Number of messages to create in total',
    type=int,
    default=5000,
  )

  args = parser.parse_args()
  return args


def prepare_output(args):
  weekday_dir = os.path.join(
    args.output_directory, WEEKDAY_STRINGS[args.weekday]
  )
  leaf_directory = os.path.join(weekday_dir, str(args.num_users))
  if not os.path.exists(leaf_directory):
    os.makedirs(leaf_directory)
    new_fileno = 0

  else:
    # Filenames start with 0.csv, and increase from there. Find the most recent
    #  filename.
    try:
      last_fileno = max([
        int(f.split(".")[0]) for f in os.listdir(leaf_directory) if f.endswith(".plt")
      ])
    except:
      new_fileno = 0
    else:
      new_fileno = last_fileno + 1

  return os.path.join(leaf_directory, "{0}.csv".format(new_fileno))


def write_to_file(records, f, converter):
  for r in records:
    f.write("{0}\n".format(converter(r)))

if __name__ == "__main__":
  session = Session()

  args = get_arguments()
  if args.num_users == 0:
    num_users = None
  else:
    num_users = args.num_users
  weekday = args.weekday
  delta = args.time_delta
  output_directory = args.output_directory
  one_movement_filepath = prepare_output(args)

  logger.info("Exporting time-homogenized records from database")
  logger.info("Number of users to be written out: {0}".format(num_users))
  logger.info("Weekday: {0}".format(WEEKDAY_STRINGS[weekday]))
  logger.info("Output directory: {0}".format(output_directory))
  logger.info("Time delta between records: {0}".format(delta))
  logger.info("Written movement file: {0}".format(one_movement_filepath))

  users = getUserSubset(num_users, weekday, session, randomize=False)
  if len(users) < num_users:
    logger.warning("Instead of {0}, there are only {1} users in the"
                   " database.".format(num_users, len(users)))
    num_users = len(users)

  # num_users is None when all users are to be output
  if num_users is None:
    num_users = len(users)

  logger.info("Users selected: {0}".format(users))

  n = num_elements_in_time_range(start=time.min, end=time.max, step=delta)
  eta_til_completed = ETACalculator(n, "DB to ONE output")
  converter = ExternalMovementReaderConverter(
    extent=BEIJING,
    decimal_degree_scaling_factor=DECIMAL_DEGREES_TO_GRID_SCALE,
    users=users
  )
  with open(one_movement_filepath, "w") as f:
    logger.info("Writing converted and normalized records to {0}".format(
      one_movement_filepath
    ))
    f.write(
      "{minTime} {maxTime} "
      "{minX} {maxX} "
      "{minY} {maxY} "
      "{minZ} {maxZ}\n".format(
      **converter.getHeader()
    ))
    for t in timerange(time.min, time.max, delta):
      records = session.query(HomogenizedRecord).filter(
        HomogenizedRecord.time == t,
        HomogenizedRecord.user.in_(users),
      )
      write_to_file(records, f, converter)
      eta_til_completed.checkpoint()
      logger.info(eta_til_completed.eta())

  # Create message files and configuration files.
  duration = int(timeDifferenceSeconds(time.max, time.min))
  num_messages = args.num_messages
  leaf_directory = os.path.dirname(one_movement_filepath)
  msgs_file = os.path.join(leaf_directory, "msgs.csv")
  interests_file = os.path.join(leaf_directory, "interests.csv")
  descriptors_file = os.path.join(leaf_directory, "metadata.csv")

  msgs = messages.create(
    n=num_messages,
    num_users=num_users,
    duration=duration,
    delta=delta
  )
  msgs.convertToONE(msgs_file)
  msgs.createChitChatFiles(
    num_social_interests=2*num_users,
    social_interests_per_user=5,
    metadata_descriptors_per_msg=4,
    social_interests_file=interests_file,
    metadata_descriptors_file=descriptors_file
  )

  # Create configuration file.
  config_file = os.path.join(output_directory, CONFIG_FILE)
  logger.info("Writing out config file to {0}".format(config_file))
  with open(config_file, "w") as outfile:
    with open(CONFIG_TEMPLATE, 'r') as infile:
      outfile.write(pystache.render(
        infile.read(), {
        'num_hosts': num_users,
        'duration': duration,
        'max_x': converter.normalized_max_x,
        'max_y': converter.normalized_max_y,
        'external_movement_file': one_movement_filepath,
        'date': WEEKDAY_STRINGS[weekday],
        'messages_file': msgs_file,
        'social_interests': interests_file,
        'message_metadata': descriptors_file,
        'beta': 3.0,
        'sigma': 10,
      }))
