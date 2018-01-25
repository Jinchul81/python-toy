import decorator
import query
from dbapi import DBAPI
from utility import Env, Shell, System, TextColor

target_database = Env.check_and_get_env('TARGET_DATABASE')
home_job = Env.check_and_get_env('HOME_JOB')
dbapi = DBAPI(target_database)

@decorator.duration()
def print_parameters():
  print TextColor.magenta("""
  ==== parameters ====
- Target database: {0}
- Home job       : {1}
"""
.format(target_database, home_job))

@decorator.duration()
def insert_lpmgt_tmap_meta():
  dbapi.execute(query.insert_query_lpmgt_tmap_meta)

@decorator.duration()
def insert_lpmgt_dmap_tmap_poi_mapp():
  dbapi.execute(query.insert_query_lpmgt_dmap_tmap_poi_mapp)

@decorator.duration()
def build_lpmgt_cell_meta():
  Shell.execute_r_command("{home_job}/R/build_lpmgt_cell_meta.R".format(home_job=home_job), target_database)

@decorator.duration()
def insert_lpmgt_cell_meta_subway():
  dbapi.execute(query.insert_query_lpmgt_cell_meta_subway)

@decorator.duration()
def insert_lpmgt_dmap_cell_poi_mapp_by_cate():
  dbapi.execute(query.insert_query_lpmgt_dmap_cell_poi_mapp_by_cate)

@decorator.duration()
def build_lpmgt_cell_build_meta():
  Shell.execute_r_command("{home_job}/R/build_lpmgt_cell_build_meta.R".format(home_job=home_job), target_database)

@decorator.duration()
def insert_lpmgt_tmap_poi_build_meta():
  dbapi.execute(query.insert_query_lpmgt_tmap_poi_build_meta)

@decorator.duration()
def insert_lpmgt_tmap_cell_build_match_apt():
  dbapi.execute(query.insert_query_lpmgt_tmap_cell_build_match_apt)

@decorator.duration()
def insert_lpmgt_tmap_cell_meta_apt_list():
  dbapi.execute(query.insert_query_lpmgt_tmap_cell_meta_apt_list)

@decorator.duration()
def insert_lpmgt_tmap_cell_keyword_match_kw_list():
  dbapi.execute(query.insert_query_lpmgt_tmap_cell_keyword_match_kw_list)

@decorator.duration()
def insert_lpmgt_tmap_cell_build_match_inbldg_list():
  dbapi.execute(query.insert_query_lpmgt_tmap_cell_build_match_inbldg_list)

@decorator.duration()
def insert_lpmgt_tmap_cell_build_match_area_list():
  dbapi.execute(query.insert_query_lpmgt_tmap_cell_build_match_area_list)

@decorator.duration()
def insert_lpmgt_dmap_cell_poi_mapp():
  dbapi.execute(query.insert_query_lpmgt_dmap_cell_poi_mapp)

@decorator.duration()
def insert_lpmgt_dmap_twifi_poi_mapp():
  dbapi.execute(query.insert_query_lpmgt_dmap_twifi_poi_mapp)

@decorator.duration()
def insert_lpmgt_dmap_poi_meta():
  dbapi.execute(query.insert_query_lpmgt_dmap_poi_meta)

@decorator.duration()
def drop_tables():
  for tablename in query.tablenames:
    dbapi.execute_no_throw("drop table {tablename} purge".format(tablename=tablename))

@decorator.duration()
def create_tables():
  for create_query in query.create_queries:
    dbapi.execute(create_query)

@decorator.duration()
def analyze_tables():
  for tablename in query.tablenames:
    dbapi.execute("analyze table {tablename} compute statistics".format(tablename=tablename))

if __name__ == '__main__':
  print_parameters()

  drop_tables()
  create_tables()

  insert_lpmgt_tmap_meta()
  insert_lpmgt_dmap_tmap_poi_mapp()
  build_lpmgt_cell_meta()
  insert_lpmgt_cell_meta_subway()
  insert_lpmgt_dmap_cell_poi_mapp_by_cate()
  build_lpmgt_cell_build_meta()
  insert_lpmgt_tmap_poi_build_meta()
  insert_lpmgt_tmap_cell_build_match_apt()
  insert_lpmgt_tmap_cell_meta_apt_list()
  insert_lpmgt_tmap_cell_keyword_match_kw_list()
  insert_lpmgt_tmap_cell_build_match_inbldg_list()
  insert_lpmgt_tmap_cell_build_match_area_list()
  insert_lpmgt_dmap_cell_poi_mapp()
  insert_lpmgt_dmap_twifi_poi_mapp()
  insert_lpmgt_dmap_poi_meta()

  analyze_tables()

  System.exit_with_ok()
