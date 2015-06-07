import random
import time
import logging
logger = logging.getLogger("geolife.messages")
from chitchat import Message
from chitchat import createUsers


def create(n, num_users, duration, delta, size, seed=None):
  delta_seconds = delta.total_seconds()
  logger.info("Creating {0} messages".format(n))
  if seed is None:
    seed = time.time()
  random.seed(seed)
  users = range(num_users)

  # Create n messages
  msgs = Messages(user_addresses=users)
  for i in range(n):
    # Randomly select two users
    sender, receiver = random.sample(users, 2)

    # Choose a time for the message to occur
    timestamp = random.randrange(0, duration, delta_seconds)

    msgs.add(Message(
      timestamp=timestamp,
      sender=sender,
      receiver=receiver,
      size=size, 
    ))

  msgs.assignUniqueIDs()
  return msgs


class Messages:
  def __init__(self, user_addresses):
    self.msgs = []
    self.users = createUsers(user_addresses)

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

  def createChitChatFiles(self,
      num_social_interests,
      social_interests_per_user,
      metadata_descriptors_per_msg,
      social_interests_file,
      metadata_descriptors_file,
      social_interests_space_file,
    ):
    # Create social interests
    logger.info("Creating {0} unique social interests".format(num_social_interests))
    social_interests = range(num_social_interests)
    with open(social_interests_space_file, "w") as f:
      for i in social_interests:
        f.write("{0}\n".format(i)) 

    # Assign social interests to users
    for u in self.users:
      u.setInterests(pool=social_interests, num=social_interests_per_user)

    # Write social interest to file
    logger.info("Writing social interests for {0} users to {1}".format(
      len(self.users), social_interests_file
    ))
    with open(social_interests_file, "w") as f:
      for u in self.users:
        f.write("{0}\n".format(u))

    # Create metadata descriptors for messages based on sender and receiver
    for m in self.msgs:
      sender_addr = m.sender
      sender = self.users[sender_addr]

      receiver_addr = m.receiver
      receiver = self.users[receiver_addr]

      m.setMetadataDescriptors(
        sender_interests=sender.interests,
        receiver_interests=receiver.interests,
        num=metadata_descriptors_per_msg,
      )

    # Write out metadata descriptors to file
    with open(metadata_descriptors_file, "w") as f:
      for m in self.msgs:
        f.write("{0}\n".format(m.toChitChat()))
