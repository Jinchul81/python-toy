import setting
from impala.dbapi import connect
from conf import HiveConf
from termcolor import colored

class DBAPI:
  def __init__(self, database=None, host=HiveConf.hostname
             , port=HiveConf.port
             , user=HiveConf.user
             , password=HiveConf.password):
    self.cursor = self.issue_cursor(host=host, port=port, database=database
                                 , user=user, password=password)

  def prepare(self, cursor):
    for global_config in setting.global_configs:
      cursor.execute(global_config)

  def issue_cursor(self, host, port, user=None, password=None, database=None):
    conn = connect(host=host, port=port, user=user, password=password,
                   database=database, auth_mechanism=HiveConf.auth_mechanism)
    cursor = conn.cursor()
    self.prepare(cursor)
    return cursor

  def get_cursor(slef):
    return self.cursor

  def fetchall(self):
    return self.cursor.fetchall()

  def get_first_record(self):
    rows = self.fetchall()
    assert(None != rows)
    assert(len(rows) > 0)
    for row in rows:
      return row

  def get_first_column_of_first_record(self):
    row = self.get_first_record()
    assert(None != row)
    assert(len(row) > 0)
    for col in row:
      return col

  def execute(self, query):
    try:
      self.cursor.execute(query)
    except Exception as e:
      print colored("* Query:\n{0}".format(query), "yellow")
      raise e

  def execute_no_throw(self, query):
    try:
      self.execute(query)
    except Exception as e:
      pass

  def exists_table(self, tablename):
    try:
      self.execute('desc {0}'.format(tablename))
    except Exception as e:
      if str(e).find("Table not found"):
        return False
    else:
      return True
