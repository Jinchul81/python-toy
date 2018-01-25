global_configs = [
  "SET hive.server2.idle.session.timeout=86400000", # 1 day
  "SET hive.server2.session.check.interval=3600000", # 1 hour
  "SET hive.server2.idle.operation.timeout=7200000", # 2 hours

  "SET hive.exec.compress.output=true",
  "SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec",
  "SET mapred.output.compression.type=BLOCK",
  # FIXME: org.apache.avro.AvroRuntimeException: Unrecognized codec: SNAPPY
  # "SET parquet.compression=SNAPPY",
]
