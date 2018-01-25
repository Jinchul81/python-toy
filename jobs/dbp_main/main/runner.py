import decorator
import query
from dbapi import DBAPI
from utility import Env, System, TextColor

wind_database = Env.check_and_get_env('WIND_DATABASE')
location_database = Env.check_and_get_env('LOCATION_DATABASE')
target_database = Env.check_and_get_env('TARGET_DATABASE')
dbapi = DBAPI(target_database)


@decorator.duration()
def print_parameters():
    print TextColor.magenta("""
  ==== parameters ====
- Target database  : {0}
- Wind database    : {1}
- Location database: {2}
"""
                            .format(target_database, wind_database, location_database))


@decorator.duration()
def build_dbp_main():
    dbapi.execute(query.create_table_dbp_main.format(
        wind_database=wind_database,
        location_database=location_database,
        target_database=target_database))
    dbapi.execute(query.truncate_table_dbp_main)


if __name__ == '__main__':
    print_parameters()
    build_dbp_main()

    System.exit_with_ok()
