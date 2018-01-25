import yaml

from datetime import datetime
from library.timer import timed
from .utility import convert_str_to_date

def generate_expected_hours():
  s = set()
  for i in range(0, 24):
    s.add(i)
  return s
hours = generate_expected_hours()

def check_traffic_distribution_range(actual):
  for e in hours.difference(actual):
    raise Exception("Found a missing element at traffic_distribution: {0}".format(e))

def check_date(start, end):
  check_date_delta(start, end)

def check_date_delta(start, end):
  if (convert_str_to_date(str(end)) - convert_str_to_date(str(start))).days < 0:
    raise Exception("start date should be earlier than the end one: \
      [start_date: {0}, end_date: {1}]".format(start, end))

@timed()
def validate(input):
  check_traffic_distribution_range(set(input['traffic_distribution'].keys()))
  check_date(input['start_date'], input['end_date'])

def to_portion(input):
  sum = 0
  for hour in hours:
    if input[hour] is not None:
      sum = sum + len(input[hour])

  portion = {}
  for hour in hours:
    if input[hour] is not None:
      portion[hour] = float(len(input[hour]) / sum)

  return portion

@timed()
def parse(input):
  validate(input)
  input['traffic_distribution'] = to_portion(input['traffic_distribution'])
  return input

def load(filepath):
  with open(filepath, 'r') as stream:
    try:
      return parse(yaml.load(stream.read()))
    except yaml.YAMLError as e:
      print (e)

if __name__ == '__main__':
  print (load_yaml("../conf/default.yaml"))
