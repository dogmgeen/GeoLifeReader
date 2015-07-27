import os

URL_FORMAT = "geolife-chitchat.ChitChatRouter-{interests}_of_{space}interests-1420users-500Mbuffer-300minTTL-10msgInterval_MessageStatsReport.txt"

def getFromFile(url, key):
  with open(url) as f:
    for l in f:
      if l.startswith(key):
        value = l.split(":")[1].strip()
        return float(value)

if __name__ == "__main__":
  print("\t" + "\t".join([str(r) for r in range(5, 55, 5)]))
  for spaceSize in range(20, 220, 20):
    line = str(spaceSize)
    for numberOfInterests in range(5, 55, 5):
      try:
        value = getFromFile(
          url=URL_FORMAT.format(interests=numberOfInterests, space=spaceSize),
          key="delivery_prob"
        )
      except:
        value = "N/A"
      finally:
        line = "{0}\t{1}".format(line, value)

    print(line)
      

