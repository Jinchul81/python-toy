from . import ad_vender
from . import birthday
from . import carrier
from . import domain
from . import email
from . import gender
from . import mac_addr
from . import purchase
from .uuids import UUIDS

import json

class User:
  def __init__(self):
    self.birthday = birthday.get_date()
    self.gender = gender.get()
    self.carrier = carrier.get()
    self.email = email.get(gender=self.gender)
    self.auth_id = self.email
    self.cell_mac = mac_addr.get()
    self.ad_vender = ad_vender.get()

    self.uuids = UUIDS()

  def get(self, uuid = None):
    if uuid is None:
      uuid = self.uuids.get()
    user_info = {
      '1' : ["birthday", self.birthday],
      '2' : ["gender", self.gender],
      '3' : ["carrier", self.carrier],
      '4' : ["email", self.email],
      '5' : ["auth_id", self.auth_id],
      '6' : ["cell_mac", self.cell_mac],
      '7' : ["ad_vender", self.ad_vender],
      '8' : ["uuid", uuid],
    }
    json_string = json.dumps(user_info)
    return json_string

  def get_email(self):
    return self.email

if __name__ == '__main__':
  for i in range(1, 10):
    user = User()
    print (user.get())
