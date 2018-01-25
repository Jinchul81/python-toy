from pyhive import hive

hive2_server = 'bdp-dmap-nn01.dmap.skt.com'
hive2_port = 10000
username = 'hdfs'
password = 'hdfs'


def execute(sql):
    connection = hive.connect(host=hive2_server, port=hive2_port, username=username, password=password,
                              configuation={'hive.server2.session.check.interval': 3600000})
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.close()
