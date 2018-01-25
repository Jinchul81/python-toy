import uuid

from random import randrange, choice

class UUIDS:
  def __init__(self):
    self.uuids = []
    for i in range(randrange(1, 5)):
      self.uuids.append(uuid.uuid4())

  def size(self):
    return len(self.uuids)

  def get(self):
    return str(choice(self.uuids))

  def print_all(self):
    return self.uuids

if __name__ == '__main__':
  uuids = UUIDS()
  print ("# of uuids: {0}".format(uuids.size()))
  print ("uuids: {0}".format(uuids.print_all()))
  for i in range(1, 10):
    print (uuids.get())
