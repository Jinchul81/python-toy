from datetime import datetime

def convert_str_to_datetime(str_datetime, timezone=None):
  if timezone is None:
    return datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S')
  else:
    return datetime.strptime("{datetime} {timezone}".
      format(datetime=str_datetime, timezone=timezone), '%Y-%m-%d %H:%M:%S %Z%z')

def convert_str_to_date(str_date, timezone=None):
  return datetime.strptime(str_date, '%Y-%m-%d').date()

def epoch(dt):
  return int(dt.timestamp())
