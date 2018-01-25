#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

settings = [
  "set io.sort.mb=10000",
  "set hive.tez.container.size=10240",
  "set hive.tez.java.opts=-Xmx8192m",
  "set hive.exec.dynamic.partition.mode=nonstrict",
]

create_table_location_poi_set_001 = unicode("""
    create table if not exists location_poi_set_001
        (
            svc_mgmt_num                  string,	-- 서비스관리번호
            profile_id                    string,	-- 프로파일ID
            visit_day_cnt                 int,		-- 방문 일 수
            weekday_visit_day_cnt         int,		-- 주중 방문 일 수
            weekend_visit_day_cnt         int,		-- 주말 방문 일 수
            weekday_visit_hh1016_cnt      int,		-- 주중 10-16시 방문 일 수
            weekday_visit_hh1622_cnt      int,		-- 주중 16-22시 방문 일 수
            weekend_visit_hh1016_cnt      int,		-- 주말 10-16시 방문 일 수
            weekend_visit_hh1622_cnt      int,		-- 주말 16-22시 방문 일 수
            mon_visit_cnt                 int,		-- 월요일 방문 일 수
            tue_visit_cnt                 int,		-- 화요일 방문 일 수
            wed_visit_cnt                 int,		-- 수요일 방문 일 수
            thu_visit_cnt                 int,		-- 목요일 방문 일 수
            fri_visit_cnt                 int,		-- 금요일 방문 일 수
            sat_visit_cnt                 int,		-- 토요일 방문 일 수
            sun_visit_cnt                 int,		-- 일요일 방문 일 수
            every_week_visit_yn           int,		-- 매주 방문 여부
            every_week_same_day_visit_yn  int,		-- 매주 같은 요일 방문 여부
            weekday_visit_poi_cnt_max     int,      -- 주중 일별 방문 POI 수 최대값
            weekday_visit_poi_cnt_min     int,      -- 주중 일별 방문 POI 수 최소값
            weekend_visit_poi_cnt_max     int,      -- 주말 일별 방문 POI 수 최대값
            weekend_visit_poi_cnt_min     int,      -- 주중 일별 방문 POI 수 최소값
            end_weekofyear                int		-- 마지막 기준주
        )
        PARTITIONED BY
        (
          start_weekofyear int						-- 첫 기준주
        )
        clustered by (svc_mgmt_num, profile_id) into 80 buckets
        STORED AS ORC
  """)
drop_partition_location_poi_set_001 = """
    ALTER TABLE location_poi_set_001 DROP IF EXISTS PARTITION(start_weekofyear='{start_weekofyear}') PURGE
  """
insert_table_location_poi_set_001 = unicode("""
    WITH poi_set as (SELECT *
                        ,WEEKOFYEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(dt,"yyyyMMdd")))        AS week
                        ,substr(dt, 1, 6)                                                AS month
                        ,WEEKOFYEAR(FROM_UNIXTIME(UNIX_TIMESTAMP("{w4sdt}","yyyyMMdd"))) as start_weekofyear	-- 기준 첫 주 계산(w4sdt)
                        ,WEEKOFYEAR(FROM_UNIXTIME(UNIX_TIMESTAMP("{w4edt}","yyyyMMdd"))) as end_weekofyear		-- 기준 마지막 주 계산(w4edt)
                    FROM lpwip_p1_location_poi_set_daily
                    where
                      dt>="{w4sdt}" -- 주단위 수행 / 4주 집계 (w4sdt)
                      AND dt<='{w4edt}' -- 주단위 수행 / 4주 집계 (w4edt)
                      AND hh0024_visit_cnt>0)
    INSERT INTO TABLE location_poi_set_001 partition(start_weekofyear)
    SELECT
      svc_mgmt_num
      ,profile_id
      ,visit_day_cnt
      ,weekday_visit_day_cnt
      ,weekend_visit_day_cnt
      ,weekday_visit_hh1016_cnt
      ,weekday_visit_hh1622_cnt
      ,weekend_visit_hh1016_cnt
      ,weekend_visit_hh1622_cnt
      ,mon_visit_cnt
      ,tue_visit_cnt
      ,wed_visit_cnt
      ,thu_visit_cnt
      ,fri_visit_cnt
      ,sat_visit_cnt
      ,sun_visit_cnt
      ,every_week_visit_yn
      ,case when
          mon_visit_cnt>=4 OR
          tue_visit_cnt>=4 OR
          wed_visit_cnt>=4 OR
          thu_visit_cnt>=4 OR
          fri_visit_cnt>=4 OR
          sat_visit_cnt>=4 OR
          sun_visit_cnt>=4 then TRUE
          else FALSE
      end as every_week_same_day_visit_yn -- 각 요일 방문일수가 4 이상인 값이 하나 이상인 경우, 매주 같은 요일 방문으로 입력
      ,weekday_visit_poi_cnt_max
      ,weekday_visit_poi_cnt_min
      ,weekend_visit_poi_cnt_max
      ,weekend_visit_poi_cnt_min
      ,end_weekofyear
      ,start_weekofyear
      FROM
        (SELECT
          svc_mgmt_num
          ,profile_id
          ,sum(case when(hh0024_visit_cnt>0) then 1 else 0 end) as visit_day_cnt
          ,sum(case when(dayofweek>=1 and dayofweek<=5) then 1 else 0 end) as weekday_visit_day_cnt -- 1:월 2:화 ... 7:일
          ,sum(case when(dayofweek>=6 and dayofweek<=7) then 1 else 0 end) as weekend_visit_day_cnt
          ,sum(case when(dayofweek>=1 and dayofweek<=5 and hh1016_visit_cnt>0) then 1 else 0 end) as weekday_visit_hh1016_cnt
          ,sum(case when(dayofweek>=1 and dayofweek<=5 and hh1622_visit_cnt>0) then 1 else 0 end) as weekday_visit_hh1622_cnt
          ,sum(case when(dayofweek>=6 and dayofweek<=7 and hh1016_visit_cnt>0) then 1 else 0 end) as weekend_visit_hh1016_cnt
          ,sum(case when(dayofweek>=6 and dayofweek<=7 and hh1622_visit_cnt>0) then 1 else 0 end) as weekend_visit_hh1622_cnt
          ,sum(case when(dayofweek=1) then 1 else 0 end) as mon_visit_cnt
          ,sum(case when(dayofweek=2) then 1 else 0 end) as tue_visit_cnt
          ,sum(case when(dayofweek=3) then 1 else 0 end) as wed_visit_cnt
          ,sum(case when(dayofweek=4) then 1 else 0 end) as thu_visit_cnt
          ,sum(case when(dayofweek=5) then 1 else 0 end) as fri_visit_cnt
          ,sum(case when(dayofweek=6) then 1 else 0 end) as sat_visit_cnt
          ,sum(case when(dayofweek=7) then 1 else 0 end) as sun_visit_cnt
          ,case when (count(distinct week)>=4) then 1 else 0 end as every_week_visit_yn		-- 주차 값이 4 개 이상인 경우, 매주 방문으로 여김
          ,max(size(dmap_poi_id)) as visit_poi_cnt
          ,max(case when(dayofweek>=1 and dayofweek<=5) then size(dmap_poi_id) else 0 end) as weekday_visit_poi_cnt_max
          ,min(case when(dayofweek>=1 and dayofweek<=5) then size(dmap_poi_id) else 0 end) as weekday_visit_poi_cnt_min
          ,max(case when(dayofweek>=6 and dayofweek<=7) then size(dmap_poi_id) else 0 end) as weekend_visit_poi_cnt_max
          ,min(case when(dayofweek>=6 and dayofweek<=7) then size(dmap_poi_id) else 0 end) as weekend_visit_poi_cnt_min
          ,max(end_weekofyear) as end_weekofyear
          ,min(start_weekofyear) as start_weekofyear
          FROM
             poi_set
          GROUP BY
              svc_mgmt_num
            , profile_id
            ) A
  """)
analyze_table_location_poi_set_001 = """
    analyze table location_poi_set_001 partition(start_weekofyear) compute statistics
  """
