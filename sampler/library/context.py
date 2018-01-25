from collections import Iterator
from datetime import timedelta
from random import randint
from .randomize.user import User
from .utility import convert_str_to_date

class Context(Iterator):
  def __init__(self, properties):
    # biding parameters
    self.hostname = properties['hostname']
    self.port = properties['port']
    self.idsite = properties['idsite']
    self.token_auth = properties['token_auth']
    self.num_users = properties['num_users']
    self.num_requests = properties['num_requests']
    self.start_date = convert_str_to_date(str(properties['start_date']))
    self.end_date = convert_str_to_date(str(properties['end_date']))
    self.num_requests = properties['num_requests']
    self.traffic_distribution = properties['traffic_distribution']
    self.controller = properties['controller']
    self.timezone = properties['timezone']

    # generated variables
    self.base_url = "http://{0}:{1}".format(self.hostname, self.port)
    self.tracker = self.base_url
    self.users = [User() for i in range(self.num_users)]

    # internal variables
    self.i = 0
    self.current_date = self.start_date
    self.current_datetime = None
    self.user = None
    self.total_delta_days = (self.end_date - self.start_date).days + 1
    self.tmp_distribution = {x: 0 for x in range(0, 24)}
    self.rebuild_tmp_distribution()
    self.set_user()

  def __next__(self):
    if self.i < self.num_requests:
      i = self.i
      self.i += 1
      self.set_current_datetime()
      self.set_user()
      return self
    else:
      raise StopIteration()

  def set_current_date(self):
    self.curr_delta_days = (self.current_date - self.start_date).days + 1
    size = self.num_requests / self.total_delta_days * self.curr_delta_days
    if size < self.i:
      self.current_date += timedelta(days=1)
      self.rebuild_tmp_distribution()

  def set_user(self):
    self.user = self.users[randint(0,self.num_users-1)]

  def rebuild_tmp_distribution(self):
    sum = 0
    for key in self.traffic_distribution.keys():
      self.tmp_distribution[key]  = int(self.traffic_distribution[key]
        * self.num_requests / self.total_delta_days)
      sum += self.tmp_distribution[key]

    all_empty = True
    for key in self.tmp_distribution.keys():
      if self.tmp_distribution[key] != 0:
        all_empty = False
        break

    if all_empty:
      for i in range(0, self.num_requests - sum):
        key = randint(0,23)
        self.tmp_distribution[key] += 1
    else:
      for i in range(0, self.num_requests - sum):
        while True:
          key = randint(0,23)
          if self.tmp_distribution[key] != 0:
            self.tmp_distribution[key] += 1
            break

  def get_hour(self):
    for key in self.tmp_distribution.keys():
      if self.tmp_distribution[key] > 0:
        self.tmp_distribution[key] -= 1
        return key
    raise Exception("Unexpected error: cannot get an hour")

  def set_current_datetime(self):
    self.set_current_date()
    self.current_datetime = "{date} {hour}:{minute}:{second}".format(
      date=self.current_date, hour=self.get_hour(),
      minute=randint(0,59), second=randint(0,59))

  def get_current_datetime(self):
    return self.current_datetime

  def get_timezone(self):
    return self.timezone

  def get_user(self):
    return self.user
