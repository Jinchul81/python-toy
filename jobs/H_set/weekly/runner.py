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
formatted_start_dt = Date.get_start_date_of_two_weeks_ago(formatted_end_dt)
end_weekofyear = Date.get_calendar_week(formatted_end_dt)
start_weekofyear = Date.get_calendar_week(formatted_start_dt)
w2sdt = Date.get_str_format_YYYYMMDD(Date.get_start_date_of_two_weeks_ago(formatted_end_dt))
w2edt = end_dt
dbapi = DBAPI(target_database)


@decorator.duration()
def print_parameters():
    print TextColor.magenta("""
  ==== parameters ====
- Database name          : {0}
- End dt                 : {1}
- Start dt of 2 weeks ago: {2}
- End dt of 2 weeks ago  : {3}
- Start week of year     : {4}
- End week of year       : {5}
"""
                            .format(target_database, end_dt, w2sdt, w2edt, start_weekofyear, end_weekofyear))


@decorator.duration()
def check():
    if False == Date.is_sunday(formatted_end_dt):
        raise RuntimeError("end week of year must be Sunday: {0}".format(formatted_end_dt))


@decorator.duration()
def prepare():
    for setting in query.settings:
        dbapi.execute(setting)


@decorator.duration()
def build_lpwip_prof_home():
    dbapi.execute(query.create_table_lpwip_prof_home)
    dbapi.execute(query.truncate_table_lpwip_prof_home)
    dbapi.execute(query.insert_table_lpwip_prof_home.format(end_dt=end_dt))
    dbapi.execute(query.analyze_table_lpwip_prof_home)


@decorator.duration()
def build_lpwip_house_cell_poi():
    dbapi.execute(query.create_table_lpwip_house_cell_poi)
    dbapi.execute(query.truncate_table_lpwip_house_cell_poi)
    dbapi.execute(query.insert_table_lpwip_house_cell_poi)
    dbapi.execute(query.analyze_table_lpwip_house_cell_poi)


@decorator.duration()
def build_lpwip_p1_loc_prof_home_poi():
    dbapi.execute(query.create_table_lpwip_p1_loc_prof_home_poi)
    dbapi.execute(query.drop_partition_lpwip_p1_loc_prof_home_poi.format(end_dt=end_dt))
    for insert_query in query.insert_table_lpwip_p1_loc_prof_home_poi:
        dbapi.execute(insert_query.format(end_dt=end_dt))
    dbapi.execute(query.analyze_table_lpwip_p1_loc_prof_home_poi)


@decorator.duration()
def build_lpwip_offline_set_activity_radius():
    dbapi.execute(query.create_table_lpwip_offline_set_activity_radius)
    dbapi.execute(query.truncate_table_lpwip_offline_set_activity_radius)
    dbapi.execute(query.insert_table_lpwip_offline_set_activity_radius.format(w2sdt=w2sdt, w2edt=w2edt))
    dbapi.execute(query.analyze_table_lpwip_offline_set_activity_radius)


@decorator.duration()
def build_lpwip_offline_set_transport():
    dbapi.execute(query.create_table_lpwip_offline_set_transport)
    dbapi.execute(query.truncate_table_lpwip_offline_set_transport)
    dbapi.execute(query.insert_table_lpwip_offline_set_transport.format(w2sdt=w2sdt, w2edt=w2edt))
    dbapi.execute(query.analyze_table_lpwip_offline_set_transport)


@decorator.duration()
def build_offline_set_001():
    dbapi.execute(query.create_table_offline_set_001)
    dbapi.execute(query.drop_partition_offline_set_001.format(start_weekofyear=start_weekofyear))
    dbapi.execute(query.insert_table_offline_set_001.format(w2sdt=w2sdt, w2edt=w2edt))
    dbapi.execute(query.analyze_table_offline_set_001)


if __name__ == '__main__':
    check()
    print_parameters()
    prepare()
    build_lpwip_prof_home()
    build_lpwip_house_cell_poi()
    build_lpwip_p1_loc_prof_home_poi()
    build_lpwip_offline_set_activity_radius()
    build_lpwip_offline_set_transport()
    build_offline_set_001()

    System.exit_with_ok()
