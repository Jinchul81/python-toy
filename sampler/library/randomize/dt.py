from random import randrange
import time

def get_datetime(start_timestamp, end_timestamp):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(
      randrange(start_timestamp,end_timestamp)))

def get_date(start_timestamp, end_timestamp):
    return time.strftime('%Y-%m-%d', time.localtime(
      randrange(start_timestamp,end_timestamp)))

def get_time(start_timestamp, end_timestamp):
    return time.strftime('%H:%M:%S', time.localtime(
      randrange(start_timestamp,end_timestamp)))
