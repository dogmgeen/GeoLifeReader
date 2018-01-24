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
    i = 0
    for u in users:
      self.user_to_addr_map[int(u)] = i
      i += 1

  def __call__(self, record):
    """Convert record to a normalized string format."""
    xPos=int(self.scale*(record.longitude - self.min_x))
    yPos=int(self.scale*(record.latitude - self.min_y))

    if xPos < 0 or yPos < 0:
      xPos=int(self.scale*(self.most_recent_record.longitude - self.min_x))
      yPos=int(self.scale*(self.most_recent_record.latitude - self.min_y))

    else:
      self.most_recent_record = record

    return "{time} {id} {xPos} {yPos}".format(
      time=int(timeDifferenceSeconds(record.time, time.min)),
      id=self.user_to_addr_map[record.user],
      xPos=xPos, yPos=yPos,
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
 
