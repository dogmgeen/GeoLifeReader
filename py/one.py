from utils import timeDifferenceSeconds
from datetime import time

class ExternalMovementReaderConverter:
  def __init__(self, extent, decimal_degree_scaling_factor, users):
    """Convert the GeoLife data into an appropriate format for the ONE
    simulator's ExternalMovementReader"""
    self.scale = decimal_degree_scaling_factor

    # Raw data values
    self.min_x = extent['west']
    self.min_y = extent['south']

    # Normalized values
    self.normalized_max_x = int(decimal_degree_scaling_factor*(
      extent['east'] - extent['west']
    ))
    self.normalized_max_y = int(decimal_degree_scaling_factor*(
      extent['north'] - extent['south']
    ))

    # Node addresses need to range from 0 to len(users)-1.
    self.user_to_addr_map = {}
    for i in range(len(users)):
      self.user_to_addr_map[users[i]] = i

  def __call__(self, record):
    """Convert record to a normalized string format."""
    return "{time} {id} {xPos} {yPos}".format(
      time=int(timeDifferenceSeconds(record.time, time.min)),
      id=self.user_to_addr_map[record.user],
      xPos=int(self.scale*(record.longitude - self.min_x)),
      yPos=int(self.scale*(record.latitude - self.min_y))
    )

  def getHeader(self):
    return dict(
      minTime=0,
      maxTime=int(timeDifferenceSeconds(time.max, time.min)),
      minX=0,
      maxX=self.normalized_max_x,
      minY=0,
      maxY=self.normalized_max_y,
      minZ=0,
      maxZ=1,
    )
 
