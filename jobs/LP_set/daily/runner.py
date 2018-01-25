import decorator
import query
from dbapi import DBAPI
from utility import Env, System, TextColor

target_database = Env.check_and_get_env('TARGET_DATABASE')
end_dt = Env.check_and_get_env('END_DT')
dbapi = DBAPI(target_database)

@decorator.duration()
def prepare():
  for setting in query.settings:
    dbapi.execute(setting)

@decorator.duration()
def print_parameters():
  print TextColor.magenta("""
  ==== parameters ====
- Target database: {0}
- End dt         : {1}
"""
.format(target_database, end_dt))

@decorator.duration()
def build_lpwip_loc_time_zone():
  dbapi.execute(query.drop_table_lpwip_loc_time_zone)
  dbapi.execute(query.create_table_lpwip_loc_time_zone)
  for insert_query in query.insert_table_lpwip_loc_time_zone:
    dbapi.execute(insert_query)

@decorator.duration()
def build_lpwip_loc_cell_duration():
  dbapi.execute(query.create_table_lpwip_loc_cell_duration)
  dbapi.execute(query.drop_partition_lpwip_loc_cell_duration.format(end_dt=end_dt))
  dbapi.execute(query.insert_table_lpwip_loc_cell_duration.format(end_dt=end_dt))
  dbapi.execute(query.analyze_table_lpwip_loc_cell_duration)

@decorator.duration()
def build_lpwip_loc_cell_duration_sum():
  dbapi.execute(query.create_table_lpwip_loc_cell_duration_sum)
  dbapi.execute(query.drop_partition_lpwip_loc_cell_duration_sum.format(end_dt=end_dt))
  dbapi.execute(query.insert_table_lpwip_loc_cell_duration_sum.format(end_dt=end_dt))
  dbapi.execute(query.analyze_table_lpwip_loc_cell_duration_sum)

@decorator.duration()
def build_lpwip_loc_twifi_duration_sum():
  dbapi.execute(query.create_table_lpwip_loc_twifi_duration_sum)
  dbapi.execute(query.drop_partition_lpwip_loc_twifi_duration_sum.format(end_dt=end_dt))
  dbapi.execute(query.insert_table_lpwip_loc_twifi_duration_sum.format(end_dt=end_dt))
  dbapi.execute(query.analyze_table_lpwip_loc_twifi_duration_sum)

@decorator.duration()
def build_lpwip_loc_tmap_duration_sum():
  dbapi.execute(query.create_table_lpwip_loc_tmap_duration_sum)
  dbapi.execute(query.drop_partition_lpwip_loc_tmap_duration_sum.format(end_dt=end_dt))
  dbapi.execute(query.insert_table_lpwip_loc_tmap_duration_sum.format(end_dt=end_dt))
  dbapi.execute(query.analyze_table_lpwip_loc_tmap_duration_sum)

@decorator.duration()
def build_lpwip_p1_location_poi_set_daily():
  dbapi.execute(query.create_table_lpwip_p1_location_poi_set_daily)
  dbapi.execute(query.drop_partition_lpwip_p1_location_poi_set_daily.format(end_dt=end_dt))
  dbapi.execute(query.insert_table_lpwip_p1_location_poi_set_daily.format(end_dt=end_dt))
  dbapi.execute(query.analyze_table_lpwip_p1_location_poi_set_daily)

@decorator.duration()
def build_lpwip_p1_location_transport_poi_daily():
  dbapi.execute(query.create_table_lpwip_p1_location_transport_poi_daily)
  dbapi.execute(query.drop_partition_lpwip_p1_location_transport_poi_daily.format(end_dt=end_dt, source="cell"))
  dbapi.execute(query.drop_partition_lpwip_p1_location_transport_poi_daily.format(end_dt=end_dt, source="twifi"))
  dbapi.execute(query.drop_partition_lpwip_p1_location_transport_poi_daily.format(end_dt=end_dt, source="tmap"))
  dbapi.execute(query.insert_table_lpwip_p1_location_transport_poi_daily.format(end_dt=end_dt))
  dbapi.execute(query.analyze_table_lpwip_p1_location_transport_poi_daily)

@decorator.duration()
def build_lpwip_p1_tmap_routehistory_daily():
  dbapi.execute(query.create_table_lpwip_p1_tmap_routehistory_daily)
  dbapi.execute(query.drop_partition_lpwip_p1_tmap_routehistory_daily.format(end_dt=end_dt))
  dbapi.execute(query.insert_table_lpwip_p1_tmap_routehistory_daily.format(end_dt=end_dt))
  dbapi.execute(query.analyze_table_lpwip_p1_tmap_routehistory_daily)

@decorator.duration()
def build_lpwip_p1_pay_set_offline_daily():
  dbapi.execute(query.create_table_lpwip_p1_pay_set_offline_daily)
  dbapi.execute(query.drop_partition_lpwip_p1_pay_set_offline_daily.format(end_dt=end_dt))
  dbapi.execute(query.insert_table_lpwip_p1_pay_set_offline_daily.format(end_dt=end_dt))
  dbapi.execute(query.analyze_table_lpwip_p1_pay_set_offline_daily)

if __name__ == '__main__':
  prepare()
  print_parameters()
  build_lpwip_loc_time_zone()
  build_lpwip_loc_cell_duration()
  build_lpwip_loc_cell_duration_sum()
  build_lpwip_loc_twifi_duration_sum()
  build_lpwip_loc_tmap_duration_sum()
  build_lpwip_p1_location_poi_set_daily()
  build_lpwip_p1_location_transport_poi_daily()
  build_lpwip_p1_tmap_routehistory_daily()
  build_lpwip_p1_pay_set_offline_daily()

  System.exit_with_ok()
