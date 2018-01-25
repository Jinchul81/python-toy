import decorator
import query
from dbapi import DBAPI
from utility import Env, System, Date, TextColor

target_database = Env.check_and_get_env('TARGET_DATABASE')
# end dt (string type)
end_dt = Env.check_and_get_env('END_DT')
# formatted end dt (datetime type)
formatted_end_dt = Date.get_date_format_YYYYMMDD(end_dt)
# formatted start dt (datetime type)
formatted_start_dt = Date.get_start_date_of_four_weeks_ago(formatted_end_dt)
end_weekofyear = Date.get_calendar_week(formatted_end_dt)
start_weekofyear = Date.get_calendar_week(formatted_start_dt)
w4edt = end_dt
w4sdt = Date.get_str_format_YYYYMMDD(formatted_start_dt)
dbapi = DBAPI(target_database)

@decorator.duration()
def print_parameters():
  print TextColor.magenta("""
  ==== parameters ====
- Database name          : {0}
- End dt                 : {1}
- Start dt of 4 weeks ago: {2}
- End dt of 4 weeks ago  : {3}
- Start week of year     : {4}
- End week of year       : {5}
"""
.format(target_database, end_dt, w4sdt, w4edt, start_weekofyear, end_weekofyear))

@decorator.duration()
def check():
  if False == Date.is_sunday(formatted_end_dt):
    raise RuntimeError("end week of year must be Sunday: {0}".format(formatted_end_dt))

@decorator.duration()
def prepare():
  for setting in query.settings:
    dbapi.execute(setting)

@decorator.duration()
def build_location_poi_set_001():
  dbapi.execute(query.create_table_location_poi_set_001)
  dbapi.execute(query.drop_partition_location_poi_set_001.format(start_weekofyear=start_weekofyear))
  dbapi.execute(query.insert_table_location_poi_set_001.format(w4sdt=w4sdt, w4edt=w4edt))
  dbapi.execute(query.analyze_table_location_poi_set_001)

if __name__ == '__main__':
  check()
  print_parameters()
  prepare()
  build_location_poi_set_001()

  System.exit_with_ok()
