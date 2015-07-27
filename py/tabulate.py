import os
import argparse

URL_FORMAT = "geolife-chitchat.ChitChatRouter-{interests}_of_{space}interests-1420users-500Mbuffer-300minTTL-10msgInterval_MessageStatsReport.txt"
DELIMITER = ";"

# Parse the command-line arguments.
def get_arguments():
  parser = argparse.ArgumentParser(
    description=('Read in logs from simulations and write them out to'
                 ' table format.')
  )
  parser.add_argument(
    '-k', '--key',
    dest='key',
    help="Key to be extracted from logs.",
    required=True
  )
  parser.add_argument(
    '-d', '--directory',
    dest="directory",
    help='Directory containing logs.',
    type=os.path.abspath,
    required=True
  )

  args = parser.parse_args()
  return args


def getFromFile(url, key):
  with open(url) as f:
    for l in f:
      if l.startswith(key):
        value = l.split(":")[1].strip()
        return float(value)

if __name__ == "__main__":
  args = get_arguments()

  print(DELIMITER + DELIMITER.join([str(r) for r in range(5, 55, 5)]))
  for spaceSize in range(20, 220, 20):
    line = str(spaceSize)
    for numberOfInterests in range(5, 55, 5):
      filename = URL_FORMAT.format(
        interests=numberOfInterests, space=spaceSize
      )
      try:
        value = getFromFile(
          url = os.path.join(args.directory, filename),
          key=args.key
        )
      except:
        value = "N/A"
      finally:
        line = "{0}{1}{2}".format(line, DELIMITER, value)

    print(line)
      

