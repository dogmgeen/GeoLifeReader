import logging
logger = logging.getLogger("geolife.raw.geolife")
import os


class IncorrectInputDirectoryException(Exception):
  def __init__(self, input_directory):
    self.input_directory = input_directory

  def __str__(self):
    return "'{0}' contains no PLT files. Aborting.".format(
      self.input_directory
    )


def find_geolife_root(directory_to_search):
  directory_containing_plt = None

  # Walk down tree until a PLT file is encountered.
  for d, subd, files in os.walk(directory_to_search):
    for f in files:
      if f.lower().endswith(".plt"):
        directory_containing_plt = d
        break

  if directory_containing_plt is None:
    raise IncorrectInputDirectoryException(directory_to_search)

  # Return the "Data" directory, which contains all users
  #  and subsequently all raw data files.
  return os.path.dirname(os.path.dirname(directory_containing_plt))


def get_num_files(directory):
  n = 0
  for d, subd, files in os.walk(directory):
    for f in files:
      if f.lower().endswith(".plt"):
        n += 1
  return n

