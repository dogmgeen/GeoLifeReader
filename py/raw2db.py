import argparse
import os
import geolife

# Parse the command-line arguments.
def get_arguments():
  parser = argparse.ArgumentParser(
    description='Populate local database with each raw trajectory.'
  )
  parser.add_argument(
    '-i', '--input_directory',
    default=".", # Directory argument defaulted to current directory.
    dest="input_directory",
    help='Directory containing PLT files (default: current working directory)',
    type=geolife.find_geolife_root,
  )

  args = parser.parse_args()
  return args


if __name__ == "__main__":
  args = get_arguments()
  print(args)
