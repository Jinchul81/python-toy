#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

settings = [
  "set io.sort.mb=10000",
  "set hive.exec.dynamic.partition.mode=nonstrict",
]

create_table_lpwip_prof_home = """
    create table if not exists lpwip_prof_home
    (
       svc_mgmt_num            string
      ,wd_dong_addr               string
      ,wd_latitude                double
      ,wd_longitude               double
      ,we_dong_addr               string
      ,we_latitude                double
      ,we_longitude               double
    )
    partitioned by (dt string )
    clustered by ( wd_dong_addr,wd_latitude,wd_longitude ) into 80 buckets
     stored as orc
  """
truncate_table_lpwip_prof_home = """
    truncate table lpwip_prof_home
  """
insert_table_lpwip_prof_home = """
    insert into lpwip_prof_home partition(dt)
     select svc_mgmt_num
               , weekday_home_dong_addr as wd_dong_addr
               , weekday_home_latitude_nm as wd_latitude
               , weekday_home_longitude_nm as wd_longitude
               , weekend_home_dong_addr as we_dong_addr
               , weekend_home_latitude_nm as we_latitude
               , weekend_home_longitude_nm as we_longitude
               ,dt
       from src.location_profile_wgs
     where dt='{end_dt}'
  """
analyze_table_lpwip_prof_home = """
    analyze table lpwip_prof_home partition(dt) compute statistics
  """

create_table_lpwip_house_cell_poi = """
    create table if not exists lpwip_house_cell_poi
    (
        dmap_poi_id   string
       ,dmap_poi_category_id string
       ,c_uid      string
       ,sector_id  string
       ,depth1     string
       ,depth2     string
       ,dong_name  string
       ,latitude   double
       ,longitude  double
    )
    clustered by ( latitude,longitude ) into 80 buckets
     stored as orc
  """
truncate_table_lpwip_house_cell_poi = """
    truncate table lpwip_house_cell_poi
  """
insert_table_lpwip_house_cell_poi = unicode("""
    insert into lpwip_house_cell_poi
        select  t2.dmap_poi_id
               ,t1.dmap_poi_category_id
               ,t3.c_uid
               ,t3.cell_id as sector_id
               ,t1.depth1
               ,t1.depth2
               ,t3.dong_name
               ,t3.latitude
               ,t3.longitude
          from loc.lpmgt_meta_dmap_poi t1
               ,lpmgt_dmap_cell_poi_mapp t2
               ,lpmgt_cell_meta t3
         where t1.dmap_poi_category_id = t2.dmap_poi_category_id
            and t1.depth1 = '주거지형태'
            and t2.c_uid = t3.c_uid
            and t2.sector_id = t3.cell_id
     distribute by latitude,longitude
  """)
analyze_table_lpwip_house_cell_poi = """
    analyze table lpwip_house_cell_poi compute statistics
  """

create_table_lpwip_p1_loc_prof_home_poi = """
    create table if not exists lpwip_p1_loc_prof_home_poi
    (
      svc_mgmt_num varchar(10)
      ,dmap_poi_id varchar(50)
      ,dmap_poi_categor_id varchar(50)
      ,depth1      varchar(100)
      ,depth2      varchar(100)
      ,c_uid       varchar(50)
      ,sector_id   varchar(50)
      ,distance_km double
      ,type string
    )
    partitioned by (dt string)
    clustered by ( svc_mgmt_num,dmap_poi_id ) into 80 buckets
     stored as orc
  """
drop_partition_lpwip_p1_loc_prof_home_poi = """
    ALTER TABLE lpwip_p1_loc_prof_home_poi DROP IF EXISTS PARTITION(dt='{end_dt}') PURGE
  """
insert_table_lpwip_p1_loc_prof_home_poi = ["""
    insert into lpwip_p1_loc_prof_home_poi partition(dt)
         select svc_mgmt_num
                ,dmap_poi_id
                ,dmap_poi_category_id
                ,depth1, depth2
                ,c_uid, sector_id
                ,(6371*acos(cos(radians(lat1))*cos(radians(lat2))*cos(radians(lon2)-radians(lon1))+sin(radians(lat1))*sin(radians(lat2)))) as distance_km
                            ,"weekday" as type
                ,dt
           from (
               select  svc_mgmt_num
                      ,row_number() over ( partition by svc_mgmt_num order by (diff_lat * diff_lat   + diff_lon * diff_lon ) ) rnum
                      ,lat1, lon1, lat2, lon2
                      ,dmap_poi_id
                      ,dmap_poi_category_id
                      ,depth1, depth2
                      ,c_uid, sector_id
                      ,dt
                 from (
                     select
                            t1.svc_mgmt_num
                           ,t1.wd_latitude as lat1,t1.wd_longitude as lon1
                           ,t2.latitude as lat2,t2.longitude as lon2
                           ,t2.dmap_poi_id
                           ,t2.dmap_poi_category_id
                           ,t2.depth1
                           ,t2.depth2
                           ,t2.c_uid, t2.sector_id
                           ,abs(t2.latitude - t1.wd_latitude) as diff_lat
                           ,abs(t2.longitude - t1.wd_longitude) as diff_lon
                           ,t1.dt
                       from
                            lpwip_prof_home t1
                            ,lpwip_house_cell_poi t2
                        where t1.dt='{end_dt}'
                        and t1.wd_dong_addr = t2.dong_name
                        ) v1
                   ) v2
              where rnum = 1
    distribute by dt
  """,
  """
    insert into lpwip_p1_loc_prof_home_poi partition(dt)
         select svc_mgmt_num
                ,dmap_poi_id
                ,dmap_poi_category_id
                ,depth1, depth2
                ,c_uid, sector_id
                ,(6371*acos(cos(radians(lat1))*cos(radians(lat2))*cos(radians(lon2)-radians(lon1))+sin(radians(lat1))*sin(radians(lat2)))) as distance_km
                            ,"weekend" as type
                ,dt
           from (
               select  svc_mgmt_num
                      ,row_number() over ( partition by svc_mgmt_num order by (diff_lat * diff_lat   + diff_lon * diff_lon ) ) rnum
                      ,lat1, lon1, lat2, lon2
                      ,dmap_poi_id
                      ,dmap_poi_category_id
                      ,depth1, depth2
                      ,c_uid, sector_id
                      ,dt
                 from (
                     select
                            t1.svc_mgmt_num
                           ,t1.we_latitude as lat1,t1.we_longitude as lon1
                           ,t2.latitude as lat2,t2.longitude as lon2
                           ,t2.dmap_poi_id
                           ,t2.dmap_poi_category_id
                           ,t2.depth1
                           ,t2.depth2
                           ,t2.c_uid, t2.sector_id
                           ,abs(t2.latitude - t1.we_latitude) as diff_lat
                           ,abs(t2.longitude - t1.we_longitude) as diff_lon
                           ,t1.dt
                       from
                            lpwip_prof_home t1
                            ,lpwip_house_cell_poi t2
                        where t1.dt='{end_dt}'
                        and t1.we_dong_addr = t2.dong_name
                        ) v1
                   ) v2
              where rnum = 1
    distribute by dt
  """]
analyze_table_lpwip_p1_loc_prof_home_poi = """
    analyze table lpwip_p1_loc_prof_home_poi partition(dt) compute statistics
  """

create_table_lpwip_offline_set_activity_radius = """
    create table if not exists lpwip_offline_set_activity_radius
      (
        svc_mgmt_num      varchar(10)
        ,activity_radius  double
      )
      clustered by (svc_mgmt_num) into 80 buckets
      STORED AS ORC
  """
truncate_table_lpwip_offline_set_activity_radius = """
    truncate table lpwip_offline_set_activity_radius
  """
insert_table_lpwip_offline_set_activity_radius = unicode("""
    --위치 프로파일링 데이터의 추정 주거지의 좌표와 일별 POI 방문지와의 거리 중 최대 값을 계산하여 활동 반경 테이블을 생성한다.
    insert into table lpwip_offline_set_activity_radius
    select
      CA.svc_mgmt_num
        ,max((6371*acos(cos(radians(CA.latitude))*cos(radians(CB.latitude_home))*cos(radians(CB.longitude_home)-radians(CA.longitude))+sin(radians(CA.latitude))*sin(radians(CB.latitude_home))))*1000) as activity_radius
        from
          -- poi 거리 계산
          (select
            CA1.svc_mgmt_num
            ,CA1.dmap_poi_ids
            ,CA2.latitude
            ,CA2.longitude
              from
                (select
                  svc_mgmt_num
                  ,dmap_poi_ids
                  from
                    lpwip_p1_location_poi_set_daily
                    lateral view explode(dmap_poi_id) newtbl as dmap_poi_ids
                    where
                dt >= "{w2sdt}" -- 주단위 수행 / 2주 집계 (w2sdt)
              and dt <= "{w2edt}" -- 주단위 수행 / 2주 집계 (w2edt)
          ) CA1
                , lpmgt_dmap_poi_meta CA2
              where
              CA1.dmap_poi_ids = CA2.dmap_poi_id) CA
          ,(select
            CB1.svc_mgmt_num
            ,cast(CB1.weekday_home_latitude_nm as DOUBLE) latitude_home
            ,cast(CB1.weekday_home_longitude_nm as DOUBLE) longitude_home
            ,CB1.dt
              from
                (select *
              from
                src.location_profile_wgs
                  where
                    dt="{w2edt}"  -- (w2edt)
          ) CB1
            ) CB
        WHERE
          CA.svc_mgmt_num = CB.svc_mgmt_num
        group by CA.svc_mgmt_num
  """)
analyze_table_lpwip_offline_set_activity_radius = """
    analyze table lpwip_offline_set_activity_radius compute statistics
  """

create_table_lpwip_offline_set_transport = unicode("""
    create table if not exists lpwip_offline_set_transport
      (
        svc_mgmt_num          varchar(10) --서비스관리번호
        ,commute_metro_cnt    int         --출퇴근 지하철 이용 일 수
        ,commute_bus_cnt      int         --출퇴근 버스 이용 일 수
        ,commute_train_cnt    int         --출퇴근 기차 이용 일 수
        ,commute_owncar_cnt   int         --출퇴근 자가용 이용 일 수
        ,commute_taxi_cnt     int         --출퇴근 택시 이용 일 수
        ,transport_metro_cnt  int         --지하철 이용 일 수
        ,transport_bus_cnt	  int         --버스 이용 일 수
        ,transport_train_cnt  int         --기차 이용 일 수
        ,transport_owncar_cnt int         --자가용 이용 일 수
        ,transport_taxi_cnt   int         --택시 이용 일 수
      )
        clustered by (svc_mgmt_num) into 80 buckets
        STORED AS ORC
  """)
truncate_table_lpwip_offline_set_transport = """
    truncate table lpwip_offline_set_transport
  """
insert_table_lpwip_offline_set_transport = unicode("""
    with offline_tansport_daily as
    (
      select
        svc_mgmt_num
        ,commute_owncar_yn
        ,cast(NULL as INT) as commute_metro_yn
        ,cast(NULL as INT) as commute_bus_yn
        ,cast(NULL as INT) as commute_train_yn
        ,cast(NULL as INT) as commute_taxi_yn
        ,case when transport_owncar>0 then 1 else 0 end as transport_owncar_yn
        ,cast(NULL as INT) as transport_metro_yn
        ,cast(NULL as INT) as transport_bus_yn
        ,cast(NULL as INT) as transport_train_yn
        ,cast(NULL as INT) as transport_taxi_yn
        ,"tmap" as source
        ,dt
          from lpwip_p1_tmap_routehistory_daily
        where
          dt>="{w2sdt}"-- 주단위 수행 / 2주 집계 (w2sdt)
          AND dt<="{w2edt}"-- 주단위 수행 / 2주 집계 (w2edt)

    union all

    -- pay set
    select
      svc_mgmt_num
      ,cast(NULL as INT) as commute_owncar_yn
      ,commute_metro_yn
      ,commute_bus_yn
      ,cast(NULL as INT) as commute_train_yn
      ,commute_taxi_yn
      ,cast(NULL as INT) as transport_owncar_yn
      ,case when metro_cnt>0 then 1 else 0 end as transport_metro_yn
      ,case when bus_cnt>0 then 1 else 0 end as transport_bus_yn
      ,cast(NULL as INT) as transport_train_yn
      ,case when taxi_cnt>0 then 1 else 0 end as transport_taxi_yn
      ,"pay" as source
      ,dt2 as dt
        from
        (select *
          ,regexp_replace(dt, "-", "") as dt2
            from
            lpwip_p1_pay_set_offline_daily) T
        where
          dt2>="{w2sdt}"-- 주단위 수행 / 2주 집계 (w2sdt)
          AND dt2<="{w2edt}"-- 주단위 수행 / 2주 집계 (w2edt)

    union all

    -- cell&twifi - 지하철
    select
      A.svc_mgmt_num
      ,cast(NULL as INT) as commute_owncar_yn
      ,max(case when (day>=1 AND day<=5) AND (A.gowork + A.gohome)>0 then 1 else 0 end) commute_metro_yn--지하철
      ,cast(NULL as INT) as commute_bus_yn
      ,cast(NULL as INT) as commute_train_yn
      ,cast(NULL as INT) as commute_taxi_yn
      ,cast(NULL as INT) as transport_owncar_yn
      ,max(case when (A.metro>0) then 1 else 0 end) transport_metro_yn--지하철 전체
      ,cast(NULL as INT) as transport_bus_yn
      ,cast(NULL as INT) as transport_train_yn
      ,cast(NULL as INT) as transport_taxi_yn
      ,"cell" as source
      ,dt
        from
          (select *
            ,case when(hh0609>80) then 1 else 0 end as gowork --역 정차시간 20초 / 역 간 이동시간 2분중 반 => 1분 20초
            ,case when(hh1720>80) then 1 else 0 end as gohome
            ,case when(hh0024>80) then 1 else 0 end as metro
            ,from_unixtime(unix_timestamp(dt,"yyyyMMdd"),"u") as day
              from
                lpwip_p1_location_transport_poi_daily
              where
                dt>="{w2sdt}"-- 주단위 수행 / 2주 집계 (w2sdt)
                AND dt<="{w2edt}"-- 주단위 수행 / 2주 집계 (w2edt)
      ) A
          ,(select * from loc.lpmgt_meta_profile_all_mapp where pf_id like "LOC0901%") B
        where
          A.dmap_poi_category_id = B.mp_id
          group by svc_mgmt_num, dt
    )

    INSERT INTO TABLE lpwip_offline_set_transport
    select
      svc_mgmt_num
      ,sum(commute_metro_yn) as commute_metro_cnt
      ,sum(commute_bus_yn) as commute_bus_cnt
      ,sum(commute_train_yn) as commute_train_cnt
      ,sum(commute_owncar_yn) as commute_owncar_cnt
      ,sum(commute_taxi_yn) as commute_taxi_cnt
      ,sum(transport_metro_yn) as transport_metro_cnt
      ,sum(transport_bus_yn) as transport_bus_cnt
      ,sum(transport_train_yn) as transport_train_cnt
      ,sum(transport_owncar_yn) as transport_owncar_cnt
      ,sum(transport_taxi_yn) as transport_taxi_cnt
      from
        offline_tansport_daily
      group by svc_mgmt_num
  """)
analyze_table_lpwip_offline_set_transport = """
    analyze table lpwip_offline_set_transport compute statistics
  """

create_table_offline_set_001 = """
    create table if not exists offline_set_001
        (
            svc_mgmt_num                   string
            ,weekday_home_do_addr          string
            ,WEEKDAY_HOME_SIGU_ADDR        string
            ,WEEKDAY_HOME_DONG_ADDR        string
            ,WEEKDAY_HOME_DURATION_TMS     int
            ,WEEKDAY_WORK_DO_ADDR          string
            ,WEEKDAY_WORK_SIGU_ADDR        string
            ,WEEKDAY_WORK_DONG_ADDR        string
            ,WEEKDAY_WORK_DURATION_TMS     int
            ,WEEKDAY_NIGHT_DO_ADDR         string
            ,WEEKDAY_NIGHT_SIGU_ADDR       string
            ,WEEKDAY_NIGHT_DONG_ADDR       string
            ,WEEKDAY_NIGHT_DURATION_TMS    int
            ,WEEKEND_HOME_DO_ADDR          string
            ,WEEKEND_HOME_SIGU_ADDR        string
            ,WEEKEND_HOME_DONG_ADDR        string
            ,WEEKEND_HOME_DURATION_TMS     int
            ,WEEKEND_WORK_DO_ADDR          string
            ,WEEKEND_WORK_SIGU_ADDR        string
            ,WEEKEND_WORK_DONG_ADDR        string
            ,WEEKEND_WORK_DURATION_TMS     int
            ,WEEKEND_NIGHT_DO_ADDR         string
            ,WEEKEND_NIGHT_SIGU_ADDR       string
            ,WEEKEND_NIGHT_DONG_ADDR       string
            ,WEEKEND_NIGHT_DURATION_TMS    string
            ,weekday_house_type            string
            ,weekend_house_type            string
            ,commute_distance              double
            ,commute_time                  double
            ,commute_metro_cnt             int
            ,commute_bus_cnt               int
            ,commute_train_cnt             int
            ,commute_owncar_cnt            int
            ,commute_taxi_cnt              int
            ,transport_metro_cnt           int
            ,transport_bus_cnt             int
            ,transport_train_cnt           int
            ,transport_owncar_cnt          int
            ,transport_taxi_cnt            int
            ,movement_pattern              string
            ,activity_radius               int
            ,end_weekofyear                int
        )
        PARTITIONED BY
        (
          start_weekofyear int
        )
        clustered by (svc_mgmt_num) into 80 buckets
        STORED AS ORC
  """
drop_partition_offline_set_001 = """
    ALTER TABLE offline_set_001 DROP IF EXISTS PARTITION(start_weekofyear='{start_weekofyear}') PURGE
  """
insert_table_offline_set_001 = unicode("""
    with smn_all as
    (
    select distinct svc_mgmt_num
      from
      (
        select svc_mgmt_num from src.location_profile_wgs
        union all
        select svc_mgmt_num from lpwip_offline_set_transport
        union all
        select svc_mgmt_num from lpwip_offline_set_activity_radius
        union all
        select svc_mgmt_num from lpwip_p1_loc_prof_home_poi
      ) as T
    )
    INSERT INTO TABLE offline_set_001 PARTITION(start_weekofyear)
    select
      /*+ STREAMTABLE(a) */
      X.svc_mgmt_num
      ,A.weekday_home_do_addr
      ,A.WEEKDAY_HOME_SIGU_ADDR
      ,A.WEEKDAY_HOME_DONG_ADDR
      ,A.WEEKDAY_HOME_DURATION_TMS
      ,A.WEEKDAY_WORK_DO_ADDR
      ,A.WEEKDAY_WORK_SIGU_ADDR
      ,A.WEEKDAY_WORK_DONG_ADDR
      ,A.WEEKDAY_WORK_DURATION_TMS
      ,A.WEEKDAY_NIGHT_DO_ADDR
      ,A.WEEKDAY_NIGHT_SIGU_ADDR
      ,A.WEEKDAY_NIGHT_DONG_ADDR
      ,A.WEEKDAY_NIGHT_DURATION_TMS
      ,A.WEEKEND_HOME_DO_ADDR
      ,A.WEEKEND_HOME_SIGU_ADDR
      ,A.WEEKEND_HOME_DONG_ADDR
      ,A.WEEKEND_HOME_DURATION_TMS
      ,A.WEEKEND_WORK_DO_ADDR
      ,A.WEEKEND_WORK_SIGU_ADDR
      ,A.WEEKEND_WORK_DONG_ADDR
      ,A.WEEKEND_WORK_DURATION_TMS
      ,A.WEEKEND_NIGHT_DO_ADDR
      ,A.WEEKEND_NIGHT_SIGU_ADDR
      ,A.WEEKEND_NIGHT_DONG_ADDR
      ,A.WEEKEND_NIGHT_DURATION_TMS
      ,D.depth2 as weekday_house_type
      ,E.depth2 as weekend_house_type
      ,A.commute_dist as commute_distance
      ,A.commute_dist/40 as commute_time
      ,B.commute_metro_cnt
      ,B.commute_bus_cnt
      ,B.commute_train_cnt
      ,B.commute_owncar_cnt
      ,B.commute_taxi_cnt
      ,B.transport_metro_cnt
      ,B.transport_bus_cnt
      ,B.transport_train_cnt
      ,B.transport_owncar_cnt
      ,B.transport_taxi_cnt
      ,case when(A.commute_dist > 30) then "long" else "short" end as movement_pattern
      ,C.activity_radius
      ,cast(WEEKOFYEAR(FROM_UNIXTIME(UNIX_TIMESTAMP("{w2edt}","yyyyMMdd"))) as int) as end_weekofyear -- 주단위 수행 / 2주 집계 (w2edt)
      ,cast(WEEKOFYEAR(FROM_UNIXTIME(UNIX_TIMESTAMP("{w2sdt}","yyyyMMdd"))) as int) as start_weekofyear -- 주단위 수행 / 2주 집계 (w2sdt)
        from
          (select svc_mgmt_num from smn_all) X
          left join
            (select *
              ,(6371*acos(cos(radians(weekday_work_latitude_nm))*
              cos(radians(weekday_home_latitude_nm))*
              cos(radians(weekday_home_longitude_nm)-
              radians(weekday_work_longitude_nm))+
              sin(radians(weekday_work_latitude_nm))*
              sin(radians(weekday_home_latitude_nm)))) as commute_dist
                from src.location_profile_wgs
                where dt="{w2edt}") A -- 주단위 수행 / 2주 집계 (w2edt)
            on (X.svc_mgmt_num=A.svc_mgmt_num)
          left join
            lpwip_offline_set_transport B
            on (X.svc_mgmt_num=B.svc_mgmt_num)
          left join
            lpwip_offline_set_activity_radius C
              on (X.svc_mgmt_num=C.svc_mgmt_num)
          left join
            (select *
              from lpwip_p1_loc_prof_home_poi
                where
                  dt="{w2edt}" -- 주단위 수행 / 2주 집계 (w2edt)
                  and Depth1="주거지형태"
                              and type="weekday"
                  and distance_km <0.3) D
            on (X.svc_mgmt_num=D.svc_mgmt_num)
          left join
            (select *
              from lpwip_p1_loc_prof_home_poi
                where
                  dt="{w2edt}" -- 주단위 수행 / 2주 집계 (w2edt)
                  and Depth1="주거지형태"
                              and type="weekend"
                  and distance_km <0.3) E
            on (X.svc_mgmt_num=E.svc_mgmt_num)
  """)
analyze_table_offline_set_001 = """
    analyze table offline_set_001 PARTITION(start_weekofyear) compute statistics
  """
