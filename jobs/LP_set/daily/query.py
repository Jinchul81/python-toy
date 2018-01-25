#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

settings = [
  "set io.sort.mb=10000",
  "set hive.exec.dynamic.partition.mode=nonstrict",
]

drop_table_lpwip_loc_time_zone = "drop table if exists lpwip_loc_time_zone"
create_table_lpwip_loc_time_zone = """
    create table if not exists lpwip_loc_time_zone
    (
        zone_ind  tinyint
         ,s_secs int
         ,e_secs int
    )
    clustered by ( s_secs, e_secs ) into 16 buckets
    stored as orc
  """
insert_table_lpwip_loc_time_zone = [
  # Description of hard-coded values
  #
  # 06 : 21600
  # 09 : 32400
  # 10 : 36000
  # 16 : 57600
  # 17 : 61200
  # 20 : 72000
  # 22 : 79200
  # 24 : 86400
  "insert into lpwip_loc_time_zone values (1,0,21600)",
  "insert into lpwip_loc_time_zone values (2,21600,32400)",
  "insert into lpwip_loc_time_zone values (3,32400,36000)",
  "insert into lpwip_loc_time_zone values (4,36000,57600)",
  "insert into lpwip_loc_time_zone values (5,57600,61200)",
  "insert into lpwip_loc_time_zone values (6,61200,72000)",
  "insert into lpwip_loc_time_zone values (7,72000,79200)",
  "insert into lpwip_loc_time_zone values (8,79200,86400)",
]

create_table_lpwip_loc_cell_duration = """
    create table if not exists lpwip_loc_cell_duration
    (
         svc_mgmt_num string
        ,c_uid   string
        ,sector_id string
        ,start_secs   int
        ,end_secs     int
    )
    partitioned by (dt string)
    clustered by ( svc_mgmt_num, c_uid, sector_id ) into 80 buckets
    stored as orc
  """
drop_partition_lpwip_loc_cell_duration = "ALTER TABLE lpwip_loc_cell_duration DROP IF EXISTS PARTITION(dt={end_dt}) PURGE"
insert_table_lpwip_loc_cell_duration = """
    insert into lpwip_loc_cell_duration partition(dt)
     SELECT svc_mgmt_num
      ,c_uid
      ,sector_id
      ,start_secs
      ,end_secs
      ,dt
     FROM
     (SELECT  svc_mgmt_num
      ,c_uid
      ,sector_id
      ,second_s start_secs
      ,type
      ,nvl( lead(second_s) over ( partition by svc_mgmt_num order by second_s,c_uid,sector_id ),86400) end_secs
      ,dt
      FROM  ( select svc_mgmt_num, c_uid, sector_id, second_s, type, dt
       from src.location_preproc
       where dt =  {end_dt}
       distribute by dt
       sort by svc_mgmt_num, second_s, c_uid, sector_id
       ) v
     ) w
     where type like '4g%'
   """
analyze_table_lpwip_loc_cell_duration = """
    analyze table lpwip_loc_cell_duration partition(dt) compute statistics
  """

create_table_lpwip_loc_cell_duration_sum = """
    create table if not exists lpwip_loc_cell_duration_sum(
       svc_mgmt_num string
      ,dmap_poi_id string
      ,dmap_poi_category_id string
      ,hh0006 double
      ,hh0609 double
      ,hh0910 double
      ,hh1016 double
      ,hh1617 double
      ,hh1720 double
      ,hh2022 double
      ,hh2224 double
      ,transport_yn boolean
    )
    partitioned by (dt string)
    clustered by ( svc_mgmt_num,dmap_poi_id ) into 80 buckets
    stored as orc
  """
drop_partition_lpwip_loc_cell_duration_sum = "ALTER TABLE lpwip_loc_cell_duration_sum DROP IF EXISTS PARTITION(dt={end_dt}) PURGE"
insert_table_lpwip_loc_cell_duration_sum = """
    insert into lpwip_loc_cell_duration_sum partition(dt)
    select svc_mgmt_num
           ,dmap_poi_id
           ,max(dmap_poi_category_id) as dmap_poi_category_id
           ,nvl(sum(hh0006),0) as hh0006
           ,nvl(sum(hh0609),0) as hh0609
           ,nvl(sum(hh0910),0) as hh0910
           ,nvl(sum(hh1016),0) as hh1016
           ,nvl(sum(hh1617),0) as hh1617
           ,nvl(sum(hh1720),0) as hh1720
           ,nvl(sum(hh2022),0) as hh2022
           ,nvl(sum(hh2224),0) as hh2224
           ,max(transport_yn) as transport_yn
           ,dt
    from (
    select svc_mgmt_num
           ,dmap_poi_id
           ,dmap_poi_category_id
          ,case when zone_ind = 1 then end_secs - start_secs end as hh0006
          ,case when zone_ind = 2 then end_secs - start_secs end as hh0609
          ,case when zone_ind = 3 then end_secs - start_secs end as hh0910
          ,case when zone_ind = 4 then end_secs - start_secs end as hh1016
          ,case when zone_ind = 5 then end_secs - start_secs end as hh1617
          ,case when zone_ind = 6 then end_secs - start_secs end as hh1720
          ,case when zone_ind = 7 then end_secs - start_secs end as hh2022
          ,case when zone_ind = 8 then end_secs - start_secs end as hh2224
          ,transport_yn
          ,dt
       from (
    select
            svc_mgmt_num
           ,dmap_poi_id
           ,dmap_poi_category_id
           ,zone_ind
           ,greatest(0,v1.start_secs, t2.s_secs ) start_secs
           ,least(86400,v1.end_secs, t2.e_secs ) end_secs
           ,v1.transport_yn
           ,dt
      from (
          select t1.svc_mgmt_num
                ,t1.start_secs, t1.end_secs
                ,t2.dmap_poi_id
                ,t2.dmap_poi_category_id
                ,dt
                ,t2.transport_yn as transport_yn
            from
                  lpwip_loc_cell_duration t1
                , lpmgt_dmap_cell_poi_mapp t2
            where t1.c_uid = t2.c_uid
              and t1.sector_id = t2.sector_id
              and dt={end_dt}
            ) v1
          ,( select zone_ind, s_secs, e_secs from lpwip_loc_time_zone ) t2
     where t2.s_secs <= v1.end_secs
             and t2.e_secs >= v1.start_secs
           )v2
        ) v3
      group by dt,svc_mgmt_num,dmap_poi_id
  """
analyze_table_lpwip_loc_cell_duration_sum= """
    analyze table lpwip_loc_cell_duration_sum partition(dt) compute statistics
  """

create_table_lpwip_loc_twifi_duration_sum = """
    create table if not exists lpwip_loc_twifi_duration_sum
    (
      svc_mgmt_num varchar(10)
      ,dmap_poi_id varchar(39)
      ,dmap_poi_category_id  varchar(15)
      ,hh1016 int
      ,hh1622 int
      ,hh0609 int
      ,hh1720 int
      ,hh0024 int
    )
    partitioned by (dt string)
    clustered by ( svc_mgmt_num,dmap_poi_id  ) into 80 buckets
    stored as orc
  """
drop_partition_lpwip_loc_twifi_duration_sum = "ALTER TABLE lpwip_loc_twifi_duration_sum DROP IF EXISTS PARTITION(dt={end_dt}) PURGE"
insert_table_lpwip_loc_twifi_duration_sum = """
    insert into lpwip_loc_twifi_duration_sum partition(dt)
          select t1.svc_mgmt_num
                 ,t2.dmap_poi_id
                 ,max(t2.dmap_poi_category_id)  as dmap_poi_category_id
                ,sum(case
                     when t1.second_s >= 36000 and t1.second_s <57600
                                      then 1 else 0 end ) as hh1016
                ,sum(case
                     when t1.second_s >= 57600 and t1.second_s <79200
                                      then 1 else 0 end ) as hh1622
                ,sum(case
                     when t1.second_s >= 21600 and t1.second_s <32400  -- 06(21600), 09(32400)
                                      then 1 else 0 end ) as hh0609
                ,sum(case
                     when t1.second_s >= 61200 and t1.second_s <72000  -- 17(61200), 20(72000)
                                      then 1 else 0 end ) as hh1720
                ,sum(1)                    as hh0024
                ,dt
            from  ( select svc_mgmt_num,second_s,type,c_uid,dt
                      from src.location_preproc
                     where dt={end_dt}
                       and type = 'twifi'
                     distribute by dt, svc_mgmt_num
                   )  t1
                 , lpmgt_dmap_twifi_poi_mapp t2
           where
                 t1.c_uid = t2.twifi_mac
      group by t1.dt,t1.svc_mgmt_num, t2.dmap_poi_id
      distribute by dt
  """
analyze_table_lpwip_loc_twifi_duration_sum = """
    analyze table lpwip_loc_twifi_duration_sum partition(dt) compute statistics
  """

create_table_lpwip_loc_tmap_duration_sum = """
    create table if not exists lpwip_loc_tmap_duration_sum
    (
      svc_mgmt_num string
      ,dmap_poi_id string   -- mac
      ,dmap_poi_category_id string
      ,hh1016 double
      ,hh1622 double
      ,hh0024 double
    )
    partitioned by (dt string)
    clustered by ( svc_mgmt_num,dmap_poi_id  ) into 80 buckets
    stored as orc
  """
drop_partition_lpwip_loc_tmap_duration_sum = "ALTER TABLE lpwip_loc_tmap_duration_sum DROP IF EXISTS PARTITION(dt={end_dt}) PURGE"
insert_table_lpwip_loc_tmap_duration_sum = """
  insert into lpwip_loc_tmap_duration_sum partition(dt)
        select t1.svc_mgmt_num
               ,t2.dmap_poi_id
               ,max(t2.dmap_poi_category_id)  as dmap_poi_category_id
              ,sum(case
                   when t1.dest_time >= 10 and t1.dest_time <16
                                    then 1 else 0 end ) as hh1016
              ,sum(case
                   when t1.dest_time >= 16 and t1.dest_time <22
                                    then 1 else 0 end ) as hh1622
              ,sum(1) as hh0024
              ,dt
          from  (
                    select svc_mgmt_num, dest_poiid
                           ,cast( substr(real_estimation_time,9,2) as int ) as dest_time
                           ,dt
                      from loc.lpwip_tmap_routehistory_smn
                     where dt={end_dt}
                   distribute by dt, svc_mgmt_num
                 )  t1
               , lpmgt_dmap_tmap_poi_mapp t2
         where t1.dest_poiid = t2.tmap_poi_id
    group by t1.dt,t1.svc_mgmt_num, t2.dmap_poi_id
    distribute by dt
  """
analyze_table_lpwip_loc_tmap_duration_sum = """
    analyze table lpwip_loc_tmap_duration_sum partition(dt) compute statistics
  """

create_table_lpwip_p1_location_poi_set_daily = """
    create table if not exists lpwip_p1_location_poi_set_daily
    (
         svc_mgmt_num         varchar(10)
        ,profile_id           varchar(50)
        ,dmap_poi_id          array<string>        -- varchar(39)
        ,hh1016_visit_cnt      int
        ,hh1622_visit_cnt        int
        ,hh0024_visit_cnt        int
        ,dayofweek            int
    )
    partitioned by (dt varchar(8) )
    clustered by ( svc_mgmt_num,profile_id ) into 80 buckets
    stored as orc
  """
drop_partition_lpwip_p1_location_poi_set_daily = """
    ALTER TABLE lpwip_p1_location_poi_set_daily DROP IF EXISTS PARTITION(dt={end_dt}) PURGE
  """
insert_table_lpwip_p1_location_poi_set_daily = """
    with dmap_poi_info as
    (
    select svc_mgmt_num
           ,dmap_poi_id
           ,dmap_poi_category_id
           ,case when hh1016 > 1800
                 then 1 else 0 end as hh1016_visit_yn
           ,case when hh1617 + hh1720 + hh2022 > 1800
                 then 1 else 0 end as hh1622_visit_yn
           ,case when hh0006+hh0609+hh0910+hh1016+hh1617+hh1720+hh2022+hh2224 > 1800
                 then 1 else 0 end as hh0024_visit_yn
           ,date_format(concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)),'u') as dayofweek
           ,dt
      from lpwip_loc_cell_duration_sum
     where dt={end_dt}
    union all
    select svc_mgmt_num
           ,dmap_poi_id
           ,dmap_poi_category_id
           ,case when hh1016 > 0
                 then 1 else 0 end as hh1016_visit_yn
           ,case when hh1622 > 0
                 then 1 else 0 end as hh1622_visit_yn
           ,case when hh0024 > 0
                 then 1 else 0 end as hh0024_visit_yn
           ,date_format(concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)),'u') as dayofweek
           ,dt
      from lpwip_loc_twifi_duration_sum
     where dt={end_dt}
    union all
    select svc_mgmt_num
           ,dmap_poi_id
           ,dmap_poi_category_id
           ,case when hh1016 > 0
                 then 1 else 0 end as hh1016_visit_yn
           ,case when hh1622 > 0
                 then 1 else 0 end as hh1622_visit_yn
           ,case when hh0024 > 0
                 then 1 else 0 end as hh0024_visit_yn
           ,date_format(concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)),'u') as dayofweek
           ,dt
      from lpwip_loc_tmap_duration_sum
     where dt={end_dt}
    )
    insert into lpwip_p1_location_poi_set_daily partition(dt)
    select  t1.svc_mgmt_num
           ,t2.pf_id
           ,collect_set(t1.dmap_poi_id )  as dmap_poi_id
           ,sum(t1.hh1016_visit_yn)     as hh1016_visit_cnt
           ,sum(t1.hh1622_visit_yn)     as hh1622_visit_cnt
           ,sum(t1.hh0024_visit_yn)     as hh0024_visit_cnt
           ,max(t1.dayofweek)           as dayofweek
           ,max(t1.dt)                  as dt
      from dmap_poi_info t1
            ,(select * from loc.lpmgt_meta_profile_all_mapp where mp_tp_cd="LP" or mp_tp_cd="HAB") t2
      where t1.dmap_poi_category_id = t2.mp_id
      group by
           t1.svc_mgmt_num
           ,t2.pf_id
    distribute by svc_mgmt_num ,pf_id
  """
analyze_table_lpwip_p1_location_poi_set_daily = """
    analyze table lpwip_p1_location_poi_set_daily partition(dt) compute statistics
  """

create_table_lpwip_p1_location_transport_poi_daily = """
    create table if not exists lpwip_p1_location_transport_poi_daily
    (
         svc_mgmt_num varchar(10)
        ,dmap_poi_id varchar(39)
        ,dmap_poi_category_id varchar(15)
        ,hh0609      bigint
        ,hh1720      bigint
        ,hh0024      bigint
    )
    partitioned by (dt varchar(8), source varchar(10))
    clustered by ( svc_mgmt_num,dmap_poi_id ) into 80 buckets
    stored as orc
  """
# cell, twifi, tmap
drop_partition_lpwip_p1_location_transport_poi_daily = """
   ALTER TABLE lpwip_p1_location_transport_poi_daily DROP IF EXISTS PARTITION(dt='{end_dt}',source='{source}') PURGE
  """
insert_table_lpwip_p1_location_transport_poi_daily = """
    insert into lpwip_p1_location_transport_poi_daily partition(dt,source)
    select svc_mgmt_num
           ,dmap_poi_id
           ,dmap_poi_category_id
           ,hh0609
           ,hh1720
           ,hh0006+hh0609+hh0910+hh1016+hh1617+hh1720+hh2022+hh2224 as hh0024
           ,dt
           ,'cell' as source
      from lpwip_loc_cell_duration_sum
     where transport_yn = true
       and dt={end_dt}
    union all
    select svc_mgmt_num
           ,dmap_poi_id
           ,dmap_poi_category_id
           ,hh0609
           ,hh1720
           ,hh0024
           ,dt
           ,'twifi' as source
      from lpwip_loc_twifi_duration_sum
     where  dt={end_dt}
    distribute by dt,source,svc_mgmt_num,dmap_poi_id
  """
analyze_table_lpwip_p1_location_transport_poi_daily = """
    analyze table lpwip_p1_location_transport_poi_daily partition(dt,source) compute statistics
  """

create_table_lpwip_p1_tmap_routehistory_daily = unicode("""
    create table if not exists  lpwip_p1_tmap_routehistory_daily(
      svc_mgmt_num                    varchar(10)
      ,commute_owncar_yn              int   -- 출퇴근 시간 자가용 이용 횟수
      ,transport_owncar               int   -- 자가용 이용 횟수
      )
    partitioned by (dt string)  -- 기준날짜
    clustered by (svc_mgmt_num) into 80 buckets
    stored as orc
  """)
drop_partition_lpwip_p1_tmap_routehistory_daily = """
    ALTER TABLE lpwip_p1_tmap_routehistory_daily DROP IF EXISTS PARTITION(dt='{end_dt}') PURGE
  """
insert_table_lpwip_p1_tmap_routehistory_daily = """
    insert into table lpwip_p1_tmap_routehistory_daily partition(dt)
    select
      svc_mgmt_num
      ,max(case when (day>=1 AND day<=5) AND ((req_hh>=06 AND req_hh<09) OR (req_hh>=17 AND req_hh<20)) then 1 else 0 end) as commute_owncar_yn
      ,count(*) as transport_owncar
      ,dt
      from
        (select *
          ,cast(substr(req_time,9,2) as INT) as req_hh
          ,from_unixtime(unix_timestamp(dt,"yyyyMMdd"),"u") as day
          from
            loc.lpwip_tmap_routehistory_smn
          where
            svc_mgmt_num is not null
            and dt={end_dt}
         ) A
      group by svc_mgmt_num, dt
  """
analyze_table_lpwip_p1_tmap_routehistory_daily = """
    analyze table lpwip_p1_tmap_routehistory_daily partition(dt) compute statistics
  """

create_table_lpwip_p1_pay_set_offline_daily = """
    create table if not exists lpwip_p1_pay_set_offline_daily
    (
        svc_mgmt_num varchar(10)
       ,commute_bus_yn int
       ,commute_taxi_yn int
       ,commute_metro_yn int
       ,bus_cnt int
       ,taxi_cnt int
       ,metro_cnt int
    )
    partitioned by (dt string)
    clustered by ( svc_mgmt_num ) into 80 buckets
    stored as orc
  """
drop_partition_lpwip_p1_pay_set_offline_daily = """
    ALTER TABLE lpwip_p1_pay_set_offline_daily DROP IF EXISTS PARTITION(dt='{end_dt}') PURGE
  """
insert_table_lpwip_p1_pay_set_offline_daily = unicode("""
    insert into table lpwip_p1_pay_set_offline_daily partition(dt)
    select
      svc_mgmt_num
      ,max(case when (use_sto_typ="버스") AND (weekday=1) AND ((hh>=06 AND hh<09) OR (hh>=17 AND hh<20)) then 1 else 0 end) as commute_bus_yn
      ,max(case when (use_sto_typ="택시") AND (weekday=1) AND ((hh>=06 AND hh<09) OR (hh>=17 AND hh<20)) then 1 else 0 end) as commute_taxi_yn
      ,max(case when (use_sto_typ="지하철") AND (weekday=1) AND ((hh>=06 AND hh<09) OR (hh>=17 AND hh<20)) then 1 else 0 end) as commute_metro_yn
      ,sum(case when (use_sto_typ="버스") then 1 else 0 end) as bus_cnt
      ,sum(case when (use_sto_typ="택시") then 1 else 0 end) as taxi_cnt
      ,sum(case when (use_sto_typ="지하철") then 1 else 0 end) as metro_cnt
      ,regexp_replace(use_dt,'-','') as dt
      from pay.pay_set_temp_for_LP_set
     where
           dt=concat(substr({end_dt},1,4),'-',substr({end_dt},5,2), '-', substr({end_dt},7,2) ) -- 일 /일
    group by svc_mgmt_num, use_dt
  """)
analyze_table_lpwip_p1_pay_set_offline_daily = """
    analyze table lpwip_p1_pay_set_offline_daily partition(dt) compute statistics
  """
