import random
import time
random.seed(time.time())
import logging
logger = logging.getLogger("geolife.chitchat")

def createUsers(addresses):
  return [User(addr) for addr in addresses]


class User:
  def __init__(self, addr):
    self.addr = addr
    self.interests = None

  def setInterests(self, pool, num):
    self.interests = random.sample(pool, num)
    logger.debug("User {0} has interests {1}".format(
      self.addr, self.interests
    ))

  def __str__(self):
    return "\n".join([
      "{0};{1}".format(self.addr, i) for i in self.interests
    ])


class Message:
  def __init__(self, timestamp, sender, receiver, size):
    self.timestamp = timestamp
    self.sender = sender
    self.receiver = receiver
    self.size = size
    self.id = None
    self.metadata_descriptors = []

  def __str__(self):
    return "{time} C {msgId} {sender} {receiver} {size}\n".format(
      time=self.timestamp,
      msgId=self.id,
      sender=self.sender,
      receiver=self.receiver,
      size=self.size,
    )

  def toChitChat(self):
    return "\n".join([
      "{0};{1}".format(self.id, m) for m in self.metadata_descriptors
    ])

  def setMetadataDescriptors(self, sender_interests, receiver_interests, num):
    # Create a unioned set of the senter and receiver's interests
    unioned_interests = set(sender_interests)
    unioned_interests.union(receiver_interests)

    # Assign a random subset of this union as this messages metadata
    #  descriptors
    self.metadata_descriptors = random.sample(unioned_interests, num)

