import random
import time
import logging
logger = logging.getLogger("geolife.messages")

def create(n, users, duration, delta, seed=None):
  delta_seconds = delta.total_seconds()
  logger.info("Creating {0} messages".format(n))
  if seed is None:
    seed = time.time()
  random.seed(seed)

  # Create n messages
  msgs = Messages()
  for i in range(n):
    # Randomly select two users
    sender, receiver = random.sample(users, 2)

    # Choose a time for the message to occur
    timestamp = random.randrange(0, duration, delta_seconds)

    msgs.add(Message(
      timestamp=timestamp,
      sender=sender,
      receiver=receiver,
      size=16000, 
    ))

  msgs.assignUniqueIDs()
  return msgs


class Message:
  def __init__(self, timestamp, sender, receiver, size):
    self.timestamp = timestamp
    self.sender = sender
    self.receiver = receiver
    self.size = size
    self.id = None

  def __str__(self):
    return "{time} C {msgId} {sender} {receiver} {size}\n".format(
      time=self.timestamp,
      msgId=self.id,
      sender=self.sender,
      receiver=self.receiver,
      size=self.size,
    )


class Messages:
  def __init__(self):
    self.msgs = []

  def add(self, msg):
    self.msgs.append(msg)

  def assignUniqueIDs(self):
    # Sort the messages based on time of creation.
    self.msgs.sort(key=lambda m: m.timestamp)
    for i in range(len(self.msgs)):
      self.msgs[i].id = i

  def convertToONE(self, outputfile):
    with open(outputfile, "w") as f:
      for m in self.msgs:
        f.write(str(m))

  def createChitChatFiles(self):
    pass
