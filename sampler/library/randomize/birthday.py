import time

from . import dt

start_timestamp = time.mktime(time.strptime('1950-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))
end_timestamp   = time.mktime(time.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))

def get_datetime(start_timestamp=start_timestamp,end_timestamp=end_timestamp):
    return dt.get_datetime(start_timestamp, end_timestamp)

def get_date(start_timestamp=start_timestamp,end_timestamp=end_timestamp):
    return dt.get_date(start_timestamp, end_timestamp)

def get_time(start_timestamp=start_timestamp,end_timestamp=end_timestamp):
    return dt.get_time(start_timestamp, end_timestamp)

if __name__ == '__main__':
  for i in range(1, 10):
    print (get_datetime())
    print (get_date())
    print (get_time())
