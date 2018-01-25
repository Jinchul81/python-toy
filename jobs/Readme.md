# DBP Jobs

## DBP main

### Job dependencies
```
1. Source data
|--- Wind

2. Intermediate data (required source data)
|--- DBP key (Job: dbp_main/key)

3. Final data (required source and intermediate data)
|-- DBM main (Job: dbp_main/main)
```

### Run
```
python runner.py
```
* Example
  * Please refer to 'test.sh' in each job
  * The shell script is self-executable
* The script returns either 0 or 1 to the parent process
  * 0 means finished successfully
  * 1 means finished wrongly

## LP/H Set

### Job dependencies
```
1. Source data
|--- Wind
|--- Location from Geovision
|--- LP/H meta (Job: LP_set/metadata) 
|--- Pay weekly run result

2. Intermediate data (required source data)
|--- LP daily run result  (Job: LP_set/daily)

3. Final data (required source and intermediate data)
|-- LP weekly run result (Job: LP_set/weekly)
|-- H weekly run result (Job: H_set/weekly)
```

## How to use
1. Prerequisite
* Setup Python DB API 2.0 for Hive
  * Install of Python DB API 2.0 client
  ```
  yum install python-pip
  pip install --upgrade pip
  yum install cryus-sasl-devel
  pip install thrift_sasl
  pip install setuptools --upgrade
  pip install impyla
  pip install enum34
  pip install termcolor
  ```
  * Trouble shooting: AttributeError: 'TBufferedTransport' object has no attribute 'trans'
  ```
  pip install thrift==0.9.3
  ```
  * Example
  ```
  from impala.dbapi import connect
  conn = connect(host='nn01', port=10000, auth_mechanism='PLAIN')
  cursor = conn.cursor()
  cursor.execute('show databases')
  print (cursor.fetchall())
  ```
* Setup HDFS client(a.k.a. snakebite)
```
pip install snakebite
```
* Modify or fill out controller/conf.py to connect HiveServer2 and HDFS
* Install Snappy uncompressor
  * see package_installer/install_snappy.sh
* Install Gradle
  * see package_installer/install_gradle.sh
* Set environment variables which should be used in runner.py in each job

### Run
```
python runner.py
```
* Example
  * Please refer to 'test.sh' in each job
  * The shell script is self-executable
* The script returns either 0 or 1 to the parent process
  * 0 means finished successfully
  * 1 means finished wrongly
