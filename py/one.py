class ExternalMovementReaderConverter:
  def __init__(self, statistics, decimal_degree_scaling_factor):
    """Convert the GeoLife data into an appropriate format for the ONE
    simulator's ExternalMovementReader"""
    self.scale = decimal_degree_scaling_factor

    # Raw data values
    self.min_time = statistics.min_time
    self.min_x = statistics.min_longitude
    self.min_y = statistics.min_latitude

    # Normalized values
    self.normalized_max_x = int(decimal_degree_scaling_factor*(
      statistics.max_longitude - statistics.min_longitude
    ))
    self.normalized_max_y = int(decimal_degree_scaling_factor*(
      statistics.max_latitude - statistics.min_latitude
    ))
    self.normalized_max_time = int(
      (statistics.max_time - statistics.min_time).total_seconds()
    )

  def __call__(self, record):
    """Convert record to a normalized string format."""
    return "{time} {id} {xPos} {yPos}".format(
      time=int((record.datetime - self.min_time).total_seconds()),
      id=record.node_address,
      xPos=int(self.scale*(record.longitude - self.min_x)),
      yPos=int(self.scale*(record.latitude - self.min_y))
    )

  def getHeader(self):
    return dict(
      minTime=0,
      maxTime=self.normalized_max_time,
      minX=0,
      maxX=self.normalized_max_x,
      minY=0,
      maxY=self.normalized_max_y,
      minZ=0,
      maxZ=1,
    )
 
