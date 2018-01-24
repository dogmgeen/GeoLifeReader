#Input: Database with time-homogenized records.
#Output: ONE simulator ready files.
#Parameters: Number of users to output.
#            Weekday of interest.
#            Area of interest.
#            Time delta between records.
import logging
logging.basicConfig(
  level=logging.DEBUG,
  filename='/tmp/geolife.geostats.log',
  filemode='w'
)
logger = logging.getLogger("geolife")
stdout = logging.StreamHandler()
stdout.setLevel(logging.DEBUG)
logger.addHandler(stdout)

from schema import HomogenizedRecord
from config import getEngine
import argparse
from utils import timerange
from utils import ETACalculator
from utils import num_elements_in_time_range
from datetime import time
from datetime import timedelta
from sqlalchemy.orm import sessionmaker
import os
import numpy
import csv
from config import DECIMAL_DEGREES_TO_GRID_SCALE

Session = sessionmaker()
engine = getEngine()
Session.configure(bind=engine)


# Parse the command-line arguments.
def get_arguments():
  parser = argparse.ArgumentParser(
    description='Write out files for simulation.'
  )

  parser.add_argument(
    '-d', '--time-delta',
    dest='time_delta',
    help="Number of seconds that should be between any two consecutive records",
    type=lambda x: timedelta(seconds=int(x)),
    default=timedelta(seconds=5),
  )
  parser.add_argument(
    '-o', '--output_directory',
    dest="output_directory",
    help='Directory to store created files (default: ./out)',
    default="./out",
    type=os.path.abspath,
  )

  args = parser.parse_args()
  return args


import math
def densityWithinCircleAroundCentroidContainingPercentageOfNodes(percentage, locations):
  # Sort nodes based on their distance to centroid.
  sorted_locations = sorted(locations, key=lambda r: r['distance'])

  # Calculate how many nodes comprise of the percentage.
  n = int(math.ceil(percentage * len(sorted_locations)))
  
  # Find the node with the maximum distance in the sorted subset.
  sorted_subset = sorted_locations[:n]
  maximum_distance = sorted_subset[-1]['distance']

  # Calculate the area of the circle.
  area = maximum_distance**2 * math.pi

  # Divide area of circle by the number of nodes.
  density = area / n
  return density, maximum_distance, area


def main():
  session = Session()

  args = get_arguments()
  delta = args.time_delta
  output_directory = args.output_directory

  n = num_elements_in_time_range(start=time.min, end=time.max, step=delta)
  eta_til_completed = ETACalculator(n, "Geographic distribution over time")

  with open("density_stats.csv", 'w') as output_file:
    fieldnames = ['time', 'd10', 'd20', 'd40', 'd60', 'd80', 'd100', 'distance10', 'distance20', 'distance40', 'distance60', 'distance80', 'distance100', 'area10', 'area20', 'area40', 'area60', 'area80', 'area100', 'centroid_lat', 'centroid_lon']
    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
    writer.writeheader()

    for t in timerange(time.min, time.max, delta):
      location_info_for_current_time_period = []
      x_coords_of_records = []
      y_coords_of_records = []
      records = session.query(HomogenizedRecord).filter(
        HomogenizedRecord.time == t,
      )
      for r in records:
        location_info_for_current_time_period.append({
          'lat': r.latitude,
          'lon': r.longitude,
        })
        x_coords_of_records.append(r.longitude)
        y_coords_of_records.append(r.latitude)

      centroid = numpy.array((
        numpy.mean(x_coords_of_records),
        numpy.mean(y_coords_of_records)
      ))

      distances = []
      for r in location_info_for_current_time_period:
        p = numpy.array((r['lon'], r['lat']))
        d = numpy.linalg.norm(p-centroid) * DECIMAL_DEGREES_TO_GRID_SCALE
        r['distance'] = d

      locations = location_info_for_current_time_period
      density10, md1, a1 = densityWithinCircleAroundCentroidContainingPercentageOfNodes(percentage=0.1, locations=locations)
      density20, md2, a2 = densityWithinCircleAroundCentroidContainingPercentageOfNodes(percentage=0.2, locations=locations)
      density40, md3, a3 = densityWithinCircleAroundCentroidContainingPercentageOfNodes(percentage=0.4, locations=locations)
      density60, md4, a4 = densityWithinCircleAroundCentroidContainingPercentageOfNodes(percentage=0.6, locations=locations)
      density80, md5, a5 = densityWithinCircleAroundCentroidContainingPercentageOfNodes(percentage=0.8, locations=locations)
      density100,md6, a6 = densityWithinCircleAroundCentroidContainingPercentageOfNodes(percentage=1., locations=locations)

      print("Densities: {0}, {1}, {2}, {3}, {4}, {5}".format(density10, density20, density40, density60, density80, density100))

      writer.writerow({
        "time": t,
        "d10": density10,
        'distance10': md1,
        'area10': a1,
        'd20': density20,
        'distance20': md2,
        'area20': a2,
        'd40': density40,
        'distance40': md3,
        'area40': a3,
        'd60': density60,
        'distance60': md4,
        'area60': a4,
        'd80': density80,
        'distance80': md5,
        'area80': a5,
        'd100': density100,
        'distance100': md6,
        'area100': a6,
        'centroid_lat': centroid[1],
        'centroid_lon': centroid[0],
      })

      # Calculate the average x and y coordinate
      eta_til_completed.checkpoint()
      logger.info(eta_til_completed.eta())

if __name__ == "__main__":
  main()
