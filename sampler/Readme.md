## Sampler

### Purpose

Request a bunch of HTTP workloads using generated dummy data on-the-fly

### Dependencies

pyyaml, termcolor, aiohttp, names

```
  $ pip3 install pyyaml
  $ pip3 install termcolor
  $ pip3 install aiohttp
  $ pip3 install names
```

### Usage

```
$ python3.6 main.py -h
usage: main.py [-h] [-c YAML_PATH]

Web request generator using dummy data

optional arguments:
  -h, --help            show this help message and exit
  -c YAML_PATH, --conf YAML_PATH
                        use custom yaml config (default: conf/dev.yaml)
```

Example with default yaml config,

```
$ python3.6 main.py
[[2017/11/20 06:19:24]] validate(...) 0.00 sec
[[2017/11/20 06:19:24]] parse(...) 0.00 sec
[[2017/11/20 06:22:56]] run(...) 212.28 sec
```

Example on local web server,

```
$ python3.6 tests/server.py &
$ python3.6 main.py -c conf/local.yaml
```

Property information relies on .yaml in conf directory. dev.yaml is for development version.
You may create your own custom yaml.

```
# The hostname and port are used to create
# the url of the target web server
hostname: was01
port: 8290

# This is the start and end date of the workload
start_date: 2017-11-11
end_date: 2017-11-11

# The timezone consists of time zone name
# (e.g. UTC, EST, CST, GMT)
# and UTC offset in the form +HHMM or --HHMM
# (e.g., Asia/Seoul => GMT+0900)
timezone: GMT+0900

# The total number of http requests. The workload
# is divided into days between start an end.
num_requests: 10000

# The number of user instances created in the user pool
num_users: 50000

# Piwik's site number
# (c.f., https://plugins.piwik.org/SiteURLtrackingID)
idsite: 1

# (e.g., http://piwik/tracker?...)
controller: tracker

# Piwik's token auth is used to athenticate in API requests
# (c.f., https://piwik.org/faq/general/faq_114/)
token_auth: 6d016aec55497ecaa61b7dd83fa209aa

# Distribution of workload over time
#
# The way to obtain each hourly portion is calculated by
# dividing the total divided by the total number of
# allocated http requests.
# For more details,
# * All hours up to 0-23 must be present
# * If you want to mean 0%, you only need to specify (e.g., 11: ,)
# * Characters are meaningless and use only length. Hence,
#   you can use any of the available characters
traffic_distribution:
  {
    0:  +++++++++,
    1:  +++++++,
    2:  ++++,
    3:  ++,
    4:  ++,
    5:  +,
    6:  ++,
    7:  ++,
    8:  +++,
    9:  ++++,
    10: +++++,
    11: ++++++,
    12: +++++++,
    13: ++++++++,
    14: +++++++++,
    15: +++++++++,
    16: ++++++++++,
    17: +++++++++++,
    18: ++++++++++++,
    19: ++++++++++++++,
    20: ++++++++++++++++,
    21: +++++++++++++++++,
    22: +++++++++++++++,
    23: +++++++++++++,
  }
```

### Test

Here is an example URL which was generated internally,

```
http://piwik/piwik.php?action_name=&idsite=1&rec=1&r=100224&h=14&m=26&s=7&url=http://localhost:8080&uid=f2ed9618-a0a0-4ce8-844c-f66435a599ee&_id=943d87d1f567f0b0&token_auth=aa4559b9f29b1351124f58255c303bfa&cdt=1509889669&_idts=1510549337&_idvc=1&_idn=0&_refts=0&_viewts=1510549337&send_image=1&pdf=1&qt=0&realp=0&wma=0&dir=0&fla=0&java=0&gears=0&ag=0&cookie=1&res=2560x1440&_cvar={"1": ["birthday", "1957-03-23"], "2": ["gender", "female"], "3": ["carrier", "LGT"], "4": ["email", "linda@yahoo.com"], "5": ["auth_id", "linda@yahoo.com"], "6": ["cell_mac", "00:16:3e:13:33:1d"], "7": ["ad_vender", 8], "8": ["uuid", "f2ed9618-a0a0-4ce8-844c-f66435a599ee"]}&gt_ms=23&pv_id=OulOts
```

Visit information/pattern is tracked by timeline view or table.
#### * Timeline view

![Screenshot](images/2.png)
#### * Table view

![Screenshot](images/1.png)

Visitor information is delivered via custom variable.

![Screenshot](images/3.png)

User id comes from UUID. Custome variable, action or custom event can be received.

![Screenshot](images/4.png)
