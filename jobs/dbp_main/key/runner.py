import time
import os, sys
import decorator
from termcolor import colored
from dbapi import DBAPI
from hdfs import HDFSHelper
from utility import Env, System, File, Shell, TextColor

target_database = Env.check_and_get_env('TARGET_DATABASE')
home_job = Env.check_and_get_env('HOME_JOB')
dbapi = DBAPI(target_database)
hdfs = HDFSHelper().get_client()

path_home_address_refiner = '{0}/address_refiner'.format(home_job)
path_home_wsdl_invoker_client = '{0}/wsdl_invoker_client'.format(home_job)
integ_txt_addr = '{db}.td_zngm_integ_txt_addr'.format(db='src')
integ_txt_addr_key = '{db}.cwip_td_zngm_integ_txt_addr_key'.format(db=target_database)
tmp_table_name = 'cwip_dbp_new_addr'
tmp_table = '{db}.{table}'.format(db=target_database, table=tmp_table_name)
tmp_path = "{0}/tmp".format(home_job)
tmp_input_path = "{0}/input".format(tmp_path)
tmp_first_succeeded_file_path = "{0}/succeeded.1".format(tmp_path)
tmp_failed_file_path = "{0}/failed".format(tmp_path)
tmp_second_succeeded_file_path= "{0}/succeeded.2".format(tmp_path)

@decorator.duration()
def print_parameters():
  print TextColor.magenta("""
==== parameters ====
- Target database: {0}
- Home job       : {1}
"""
.format(target_database, home_job))

@decorator.duration()
def check():
  File.throw_error_if_invalid_directory(tmp_path)
  File.throw_error_if_invalid_directory(tmp_input_path)

@decorator.duration()
def create_key_table():
  dbapi.execute('drop table if exists {0}'.format(tmp_table))
  dbapi.execute('create table if not exists {0}(integ_txt_addr_id string, key string)'
      .format(integ_txt_addr_key))

@decorator.duration()
def create_temp_table_to_collect_delta():
  dbapi.execute('''
    create table {tmp_table} as
    with q1 as (select integ_txt_addr_id, bas_addr, dtl_addr from {integ_txt_addr}),
    q2 as (select integ_txt_addr_id from {integ_txt_addr_key})
    select q1.integ_txt_addr_id, q1.bas_addr, q1.dtl_addr from q1 left outer join q2
    on q1.integ_txt_addr_id = q2.integ_txt_addr_id
    where q2.integ_txt_addr_id is null
    '''.format(tmp_table=tmp_table, integ_txt_addr=integ_txt_addr, integ_txt_addr_key=integ_txt_addr_key))

@decorator.duration()
def check_src_addr_table():
  return dbapi.exists_table(integ_txt_addr)

@decorator.duration()
def create_temp_directories():
  Shell.execute_command("rm -rf {0}".format(tmp_path))
  Shell.execute_command("mkdir -p {0}".format(tmp_input_path))

@decorator.duration()
def copy_to_local_for_temp_dbp_new_addr():
  hdfs_path_delta = "/apps/hive/warehouse/{0}.db/{1}".format(target_database, tmp_table_name)
  set_du = hdfs.du(paths=[hdfs_path_delta], include_toplevel=True, include_children=False)
  result_du = next(iter(set_du))['length']
  if 0 == result_du:
    print colored("Stop the procedure because of No more delta data.", "green")
    return False

  command="hdfs dfs -copyToLocal {0}/* {1}".format(hdfs_path_delta, tmp_input_path)
  Shell.execute_command(command)

@decorator.duration(detailed = True)
def execute_on_address_refiner(command):
  Shell.execute_command(command, path_home_address_refiner)

@decorator.duration(detailed = True)
def execute_on_wsdl_invoker_client(command):
  Shell.execute_command(command, path_home_wsdl_invoker_client)

@decorator.duration()
def update_address_refiner_metadata():
  path_data = "{0}/{1}".format(path_home_address_refiner, "data")
  Shell.execute_command('./update.sh', path_data)

@decorator.duration()
def run_address_refiner(path):
  execute_on_address_refiner('cmake .')
  execute_on_address_refiner('make')
  update_address_refiner_metadata()
  cmd_address_refiner = "./src/address_refiner {0} {1} {2}".format(
    tmp_input_path,
    tmp_first_succeeded_file_path,
    tmp_failed_file_path)
  execute_on_address_refiner(cmd_address_refiner)

@decorator.duration()
def apply_proxy_to_wsdl_invoker_clientClient(chdir_path):
  proxy_option="-Dhttp.proxyHost=dbp-nn01 -Dhttp.proxyPort=3128 -Dhttp.nonProxyhosts=localhost"
  cmd_applying_proxy="sed -i \"s/DEFAULT_JVM_OPTS=\\\"\\\"/DEFAULT_JVM_OPTS=\\\"{0}\\\"/g\" wsdl_invoker_client".format(proxy_option)
  Shell.execute_command(cmd_applying_proxy, chdir_path)

@decorator.duration()
def run_wsdl_invoker_client(path):
  target_name='wsdl_invoker_client-1.0-SNAPSHOT'
  target_archive='{0}.tar'.format(target_name)
  File.throw_error_if_invalid_file("{0}/{1}".format(path_home_wsdl_invoker_client, 'build.gradle'))
  execute_on_wsdl_invoker_client('gradle build')
  path_distributions="{0}/build/distributions".format(path_home_wsdl_invoker_client)
  File.throw_error_if_invalid_file("{0}/{1}".format(path_distributions, target_archive))
  Shell.execute_command('tar -xvf {0}'.format(target_archive), path_distributions)
  path_bin="{0}/{1}/bin".format(path_distributions, target_name)
  inputFilepath=tmp_failed_file_path
  outputFilepath=tmp_second_succeeded_file_path
  # FIXME: a workaround to avoid firewall issue until the approval
  #apply_proxy_to_wsdl_invoker_clientClient(path_bin)
  cmd_wsdl_invoker_client="./wsdl_invoker_client {0} {1}".format(inputFilepath, outputFilepath)
  Shell.execute_command(cmd_wsdl_invoker_client, path_bin)

@decorator.duration()
def load_delta_into_key_table(local_path):
  hdfs_path_tmp="/tmp"
  filename = os.path.basename(local_path)
  hdfs_path="{0}/{1}".format(hdfs_path_tmp, filename)
  if hdfs.test(hdfs_path, exists=True):
    Shell.execute_command_no_throw("hdfs dfs -rm {0}".format(hdfs_path))
  Shell.execute_command("hdfs dfs -copyFromLocal {0} {1}".format(local_path, hdfs_path_tmp))
  query = "load data inpath '{0}' into table {1}".format(hdfs_path, integ_txt_addr_key)
  dbapi.execute(query)

@decorator.duration()
def prepare_delta():
  if False == check_src_addr_table():
    print "Failed: Not found {0} table".format(integ_txt_addr)
    System.exit_with_faile()
  create_key_table()
  create_temp_table_to_collect_delta()
  create_temp_directories()
  if False == copy_to_local_for_temp_dbp_new_addr():
    System.exit_with_ok()
  Shell.uncompress_snappy_files(tmp_input_path)

@decorator.duration()
def process_delta():
  run_address_refiner(path_home_address_refiner)
  run_wsdl_invoker_client(path_home_wsdl_invoker_client)

@decorator.duration()
def update_delta():
  load_delta_into_key_table(tmp_first_succeeded_file_path)
  load_delta_into_key_table(tmp_second_succeeded_file_path)

if __name__ == '__main__':
  check()
  print_parameters()
  prepare_delta()
  process_delta()
  update_delta()

  System.exit_with_ok()
