class RectangularExtent:
  """Given a known rectangle on a Cartesean plane, with one minimum corner and
  one maximum corner, and a new point, compute the new extent."""

  def __init__(self, x1, x2, y1, y2):
    self.min_x = min(x1, x2)
    self.max_x = max(x1, x2)
    self.min_y = min(y1, y2)
    self.max_y = max(y1, y2)

  def addPoint(self, x, y):
    self.min_x = min(self.min_x, x)
    self.max_x = max(self.max_x, x)
    self.min_y = min(self.min_y, y)
    self.max_y = max(self.max_y, y)

  def addExtent(self, extent):
    self.min_x = min(self.min_x, extent.min_x)
    self.max_x = max(self.max_x, extent.max_x)
    self.min_y = min(self.min_y, extent.min_y)
    self.max_y = max(self.max_y, extent.max_y)

  def asMinMaxLongLatTuple(self):
    return self.min_x, self.max_x, self.min_y, self.max_y

  def __str__(self):
    return "<Extent ({0}, {1}) to ({2}, {3})>".format(
      self.min_x, self.min_y,
      self.max_x, self.max_y
    )

def extendExtents(e1, e2):
  e1.addExtent(e2)
  return e1
