from .context import Context
from .randomize.user import User
from .utility import convert_str_to_datetime, epoch

# URL example using custom variable
#
# http://piwik.dbp.skt.com/piwik.php
# ?action_name=
# &idsite=3
# &rec=1
# &r=100224
# &h=14
# &m=26
# &s=7
# &url=http://twifi.piwik.dbp.skt.com/
# &uid=myUserId@user.com
# &_id=943d87d1f567f0b0
# &_idts=1510549337
# &_idvc=1
# &_idn=0
# &_refts=0
# &_viewts=1510549337
# &send_image=1
# &pdf=1
# &qt=0
# &realp=0
# &wma=0
# &dir=0
# &fla=0
# &java=0
# &gears=0
# &ag=0
# &cookie=1
# &res=2560x1440
# &_cvar={"1":["Gender","Male"],
#         "2":["Email","jinchul@sk.com"],
#         "3":["Phone","01040001285"],
#         "4":["Name","jinchul"]}
# &gt_ms=23
# &pv_id=OulOts

def get(context):
  user = context.get_user()
  uuid = user.uuids.get()

  epoch_time = epoch(convert_str_to_datetime(
    context.get_current_datetime(), context.get_timezone()))
  params = []
  params.append(("action_name", ""))
  params.append(("idsite", str(context.idsite)))
  params.append(("rec", "1"))
  params.append(("r", "100224"))
  params.append(("h", "14"))
  params.append(("m", "26"))
  params.append(("s", "7"))
  params.append(("url", context.tracker))
  params.append(("uid", uuid))
  params.append(("_id", "943d87d1f567f0b0"))
  params.append(("token_auth", str(context.token_auth)))
  params.append(("cdt", epoch_time))
  params.append(("_idts", "1510549337"))
  params.append(("_idvc", "1"))
  params.append(("_idn", "0"))
  params.append(("_refts", "0"))
  params.append(("_viewts", "1510549337"))
  params.append(("send_image", "1"))
  params.append(("pdf", "1"))
  params.append(("qt", "0"))
  params.append(("realp", "0"))
  params.append(("wma", "0"))
  params.append(("dir", "0"))
  params.append(("fla", "0"))
  params.append(("java", "0"))
  params.append(("gears", "0"))
  params.append(("ag", "0"))
  params.append(("cookie", "1"))
  params.append(("res", "2560x1440"))
  params.append(("_cvar", user.get(uuid)))
  params.append(("gt_ms", "23"))
  params.append(("pv_id", "OulOts"))
  print (params)

  return params

if __name__ == '__main__':
  for i in range(1, 10):
    user = User()
    print (get(user))
