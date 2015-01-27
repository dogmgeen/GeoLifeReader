import os
import file

class GeoLifeUser:
  def __init__(self, user_id, directory):
    self.id = int(user_id)
    self.files = file.load_from_directory(
      directory=os.path.join(directory, user_id),
      user=self
    )

  def __iter__(self):
    for f in self.files:
      import os
      print("Reading from file {0}".format(os.path.basename(f.url)))
      for r in f:
        yield r


def from_directory(directory):
  users = []
  for d in os.listdir(directory):
    if os.path.isdir(os.path.join(directory, d)):
      print("Yielding user {0}".format(d))
      yield GeoLifeUser(user_id=d, directory=directory)

