
#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# TODO: DB names have not been consolidated into our naming rule.

create_query_lpmgt_tmap_meta = """
  create table if not exists lpmgt_tmap_meta
  (
    poi_id	string
    ,name_org	string
    ,center_wgs84_lat	double
    ,center_wgs84_lon	double
    ,hcode	string
    ,large_cd	string
    ,middle_cd	string
    ,l_small_cd	string
    ,l_detail_cd	string
    ,a_small_cd	string
    ,zip_cd	string
    ,lcd_name	string
    ,mcd_name	string
    ,l_scd_name	string
    ,l_dcd_name	string
    ,a_scd_name	string
    ,class_a	tinyint
    ,class_b	tinyint
    ,class_c	tinyint
    ,class_d	tinyint
    ,class_nm_a	string
    ,class_nm_b	string
    ,class_nm_c	string
    ,class_nm_d	string
    ,class_nm	string
    ,name_data	array<string>
  )
  stored as orc
"""
insert_query_lpmgt_tmap_meta = """
  with tmp_tmap_meta as
  (
    select
      poi_id
      ,name_org
      ,avg(center_wgs84_lat) as center_wgs84_lat
      ,avg(center_wgs84_lon) as center_wgs84_lon
      ,hcode
      ,large_cd
      ,middle_cd
      ,l_small_cd
      ,l_detail_cd
      ,a_small_cd
      ,zip_cd
      ,lcd_name
      ,mcd_name
      ,l_scd_name
      ,l_dcd_name
      ,a_scd_name
      ,class_a
      ,class_b
      ,class_c
      ,class_d
      ,class_nm_data
      ,collect_set(name_data_all) as name_data
      from
      (select * ,split(regexp_replace(concat_ws("_",concat_ws("_", name_org, name1, name2, name4) ,concat_ws("_", name_data)), "\\\\)|\\\\(|\\\\[|\\\\]|\\\\,    eval=FALSE}|\\\\{"," "), "_") as name_kw_array_dup
        from src.tmap_poimeta) TMP
        lateral view explode(name_kw_array_dup) x as name_data_all
        group by
        poi_id
        ,name_org
        ,hcode
        ,large_cd
        ,middle_cd
        ,l_small_cd
        ,l_detail_cd
        ,a_small_cd
        ,zip_cd
        ,lcd_name
        ,mcd_name
        ,l_scd_name
        ,l_dcd_name
        ,a_scd_name
        ,class_a
        ,class_b
        ,class_c
        ,class_d
        ,class_nm_data
  )
  ,tmp_tmap_category as
  (
    select distinct
      poi_id
      ,class_nm_data2 as class_nm
      from
        (select
          poi_id
          ,class_nm_data2
            from
              src.tmap_poimeta lateral
                view explode(class_nm_data) new_tb as class_nm_data2) TMP
            where
              substr(reverse(class_nm_data2),1,2) = "1:"
  )
  , tmp_tmap_meta2 as
  (
    select
      A.poi_id
      ,name_org
      ,A.center_wgs84_lat
      ,A.center_wgs84_lon
      ,A.hcode
      ,A.large_cd
      ,A.middle_cd
      ,A.l_small_cd
      ,A.l_detail_cd
      ,A.a_small_cd
      ,A.zip_cd
      ,A.lcd_name
      ,A.mcd_name
      ,A.l_scd_name
      ,A.l_dcd_name
      ,A.a_scd_name
      ,A.class_a
      ,A.class_b
      ,A.class_c
      ,A.class_d
      ,B.class_nm
      ,A.name_data
      from
      tmp_tmap_meta A
        left join
        tmp_tmap_category B
        on
          A.poi_id = B.poi_id
  )

  insert into table lpmgt_tmap_meta
  select
    poi_id
    ,name_org
    ,center_wgs84_lat
    ,center_wgs84_lon
    ,hcode
    ,large_cd
    ,middle_cd
    ,l_small_cd
    ,l_detail_cd
    ,a_small_cd
    ,zip_cd
    ,lcd_name
    ,mcd_name
    ,l_scd_name
    ,l_dcd_name
    ,a_scd_name
    ,class_a
    ,class_b
    ,class_c
    ,class_d
    ,split(class_nm3, ":")[0] as class_nm_a
    ,split(class_nm3, ":")[1] as class_nm_b
    ,split(class_nm3, ":")[2] as class_nm_c
    ,split(class_nm3, ":")[3] as class_nm_d
    ,class_nm3 as class_nm
    ,name_data
      from
        (select *
          ,substr(class_nm2, 5, length(class_nm)-4) as class_nm3
            from
            (select *
              ,substr(class_nm, 1, length(class_nm)-2) as class_nm2
                from tmp_tmap_meta2) T) T2
"""

create_query_lpmgt_dmap_tmap_poi_mapp =  """
  create table if not exists lpmgt_dmap_tmap_poi_mapp
  (
      dmap_poi_id             string
      ,dmap_poi_category_id	string
      ,tmap_poi_id             string
  )
  stored as orc
"""
insert_query_lpmgt_dmap_tmap_poi_mapp =  """
  with lpmgt_dmap_tmap_poi_mapp_by_cate as
  (
  select distinct
    A.dmap_poi_category_id
    ,B.poi_id as tmap_poi_id
    from
      (select distinct
        dmap_poi_category_id
        ,tmapdepth1234
          from loc.lpmgt_xls_dmap_tmap_twifi_cell_category_mapp) A
      ,(select
          poi_id
          ,class_nm
            from lpmgt_tmap_meta) B
      where
        (A.tmapdepth1234 = B.class_nm)
  )

  insert into lpmgt_dmap_tmap_poi_mapp
  select
    concat(A.dmap_poi_category_id
        ,B.large_cd, B.middle_cd, B.l_small_cd, B.l_detail_cd
        , "TM"
        ,repeat("0",(12-length(A.tmap_poi_id)))
        , A.tmap_poi_id) as dmap_poi_id
    ,A.dmap_poi_category_id
    ,A.tmap_poi_id
      from
        lpmgt_dmap_tmap_poi_mapp_by_cate A
        ,lpmgt_tmap_meta B
      where A.tmap_poi_id = B.poi_id
"""

create_query_lpmgt_cell_meta_subway =  """
  create table if not exists lpmgt_cell_meta_subway
  (
  c_uid string,
  cell_id string,
  latitude string,
  longitude string,
  ru_name string,
  meta_train string,
  c_uid_joined string,
  subway_seq string,
  subway_code string,
  city string,
  subway_line string,
  station	string,
  st_basement_flag string,
  dist double
  )
  stored as orc
"""
insert_query_lpmgt_cell_meta_subway =  unicode("""
  with tmp_cl_enb_subway_meta as
  (
  select
      A.subway_seq
      ,A.c_uid
      ,A.cellnum
      ,B.subway_code
      ,B.city
      ,B.subway_line
      ,B.station
      ,B.st_basement_flag
      ,B.update_date
      ,B.latitude_norm
      ,B.longitude_norm
      from
        (select
          subway_seq
          ,c_uid
          ,cellnum
            from src.cl_enb_subway) A
        left join
          (select
            subway_seq
            ,subway_code
            ,city
            ,subway_line
            ,station
            ,st_basement_flag
            ,update_date
            ,latitude_norm
            ,longitude_norm
              from src.cb_subway) B
          on A.subway_seq = B.subway_seq
   )
  ,tmp_cell_meta_subway_all as
  (
  select * from (
   select
    A.c_uid
    ,A.cell_id
    ,A.latitude
    ,A.longitude
    ,A.ru_name
    ,A.meta_train
    ,B.c_uid as c_uid_joined
    ,B.subway_seq
    ,B.subway_code
    ,B.city
    ,B.subway_line
    ,B.station
    ,B.st_basement_flag
    ,sqrt(power(B.latitude_norm-A.latitude,2)+power(B.longitude_norm-A.longitude,2))*100000 as dist
    ,row_number() over( partition by (B.c_uid, B.cellnum)
    order by (power(B.latitude_norm-A.latitude,2)+power(B.longitude_norm-A.longitude,2)) ) rn
     -- cl_enb_subway table matches several stations to a cell, thus we select the closest station
    from
      lpmgt_cell_meta A
      left join
        tmp_cl_enb_subway_meta B
        on A.c_uid = B.c_uid AND A.cell_id = B.cellnum) T1
      where T1.rn=1
  )
  insert into table lpmgt_cell_meta_subway
  select distinct c_uid, cell_id, latitude, longitude, ru_name, meta_train, c_uid_joined
    , subway_seq, subway_code, city, subway_line, station, st_basement_flag, dist
    from tmp_cell_meta_subway_all
  where
    c_uid_joined is not null
    and st_basement_flag = 0
    and ru_name rlike "\\\\(지\\\\)"
""")

create_query_lpmgt_dmap_cell_poi_mapp_by_cate =  """
  create table if not exists lpmgt_dmap_cell_poi_mapp_by_cate
  (
    dmap_poi_category_id	string		,c_uid	string
    ,sector_id	string
  )
  stored as orc
"""
insert_query_lpmgt_dmap_cell_poi_mapp_by_cate =  """
  insert into table lpmgt_dmap_cell_poi_mapp_by_cate
  select distinct
    C.dmap_poi_category_id
    ,B.c_uid
    ,B.cell_id as sector_id
      from
      lpmgt_cell_meta_subway as B
    join loc.lpmgt_xls_subway_poi_category_mapping C
    on B.city=C.city and B.subway_line=C.line
"""

create_query_lpmgt_tmap_poi_build_meta =  """
  create table if not exists lpmgt_tmap_poi_build_meta
  (
    poi_id	string
    ,poi_id2	string
    ,class_a	string
    ,class_b	string
    ,class_c	string
    ,class_d	string
    ,poi_name1	string
    ,poi_name2	string
    ,ca_name	string
    ,cb_name	string
    ,cc_name	string
    ,cd_name	string
    ,join_key	string
    ,used	string
    ,name1	string
    ,name2	string
    ,distanceki	double
    ,mapping	double
    ,apt	double
    ,bldng	double
    ,area	double
    ,tmap_kw	string
    ,tmap_kw_array array<string>
  )
  stored as orc
"""
insert_query_lpmgt_tmap_poi_build_meta = unicode("""
  with tmp_tmap_poi_build_meta as
  (
  select
    A.poi_id
    ,A.class_a
    ,A.class_b
    ,A.class_c
    ,A.class_d
    ,A.poi_name1
    ,A.poi_name2
    ,A.ca_name
    ,A.cb_name
    ,A.cc_name
    ,A.cd_name
    ,A.join_key
    ,A.used
    ,A.name1
    ,A.name2
    ,A.distanceki
    ,B.mapping
    ,B.apt
    ,B.bldng
    ,B.area
      from
        (select *
            ,concat_ws(":", ca_name, cb_name, cc_name, cd_name) as c_name
                from src.tmap_poi_build) A
          left join
            loc.lpmgt_xls_tmap_category_mapping_used B
            on (A.c_name = B.c_name)
  )
  ,tmp_tmap_poi_build_meta_matching_kw as
  (
  select
    poi_id
    ,collect_set(name_kw) as name_data
      from
      (select
        poi_id
        ,name_kw
        ,lcd_name, mcd_name, l_scd_name, l_dcd_name, a_scd_name
        ,substr(lcd_name, 1, length(lcd_name)-1) as lcd_name_s
        ,substr(mcd_name, 1, length(mcd_name)-1) as mcd_name_s
        ,substr(l_scd_name, 1, length(l_scd_name)-1) as l_scd_name_s
        ,substr(l_dcd_name, 1, length(l_dcd_name)-1) as l_dcd_name_s
        ,substr(a_scd_name, 1, length(a_scd_name)-1) as a_scd_name_s
        ,name_data
        from
          lpmgt_tmap_meta
          lateral view explode(name_data) new_tb as name_kw) TMP
        where
          name_kw not like lcd_name
          AND name_kw not like mcd_name
          AND name_kw not like l_scd_name
          AND name_kw not like l_dcd_name
          AND name_kw not like a_scd_name
          AND name_kw not like regexp_replace(lcd_name, "[0-9]+", "")
          AND name_kw not like regexp_replace(mcd_name, "[0-9]+", "")
          AND name_kw not like regexp_replace(l_scd_name, "[0-9]+", "")
          AND name_kw not like regexp_replace(l_dcd_name, "[0-9]+", "")
          AND name_kw not like regexp_replace(a_scd_name, "[0-9]+", "")
          AND name_kw not like concat(lcd_name, "점")
          AND name_kw not like concat(mcd_name, "점")
          AND name_kw not like concat(l_scd_name, "점")
          AND name_kw not like concat(l_dcd_name, "점")
          AND name_kw not like concat(a_scd_name, "점")
          AND name_kw not like lcd_name_s
          AND name_kw not like mcd_name_s
          AND name_kw not like l_scd_name_s
          AND name_kw not like l_dcd_name_s
          AND name_kw not like a_scd_name_s
          AND name_kw not like regexp_replace(lcd_name_s, "[0-9]+", "")
          AND name_kw not like regexp_replace(mcd_name_s, "[0-9]+", "")
          AND name_kw not like regexp_replace(l_scd_name_s, "[0-9]+", "")
          AND name_kw not like regexp_replace(l_dcd_name_s, "[0-9]+", "")
          AND name_kw not like regexp_replace(a_scd_name_s, "[0-9]+", "")
          AND name_kw not like concat(lcd_name_s, "점")
          AND name_kw not like concat(mcd_name_s, "점")
          AND name_kw not like concat(l_scd_name_s, "점")
          AND name_kw not like concat(l_dcd_name_s, "점")
          AND name_kw not like concat(a_scd_name_s, "점")
          AND name_kw not like "[0-9]+"
          AND name_kw not like "[.]"
        group by poi_id
  )

  insert into table lpmgt_tmap_poi_build_meta
  select
    A.poi_id
    ,B.poi_id as poi_id2
    ,A.class_a
    ,A.class_b
    ,A.class_c
    ,A.class_d
    ,A.poi_name1
    ,A.poi_name2
    ,A.ca_name
    ,A.cb_name
    ,A.cc_name
    ,A.cd_name
    ,A.join_key
    ,A.used
    ,A.name1
    ,A.name2
    ,A.distanceki
    ,A.mapping
    ,A.apt
    ,A.bldng
    ,A.area
    ,concat_ws("_", B.name_data) as tmap_kw
    ,B.name_data as tmap_kw_array
      from
      tmp_tmap_poi_build_meta A
        left join
        tmp_tmap_poi_build_meta_matching_kw B
        on (A.poi_id = B.poi_id)
""")

create_query_lpmgt_tmap_cell_build_match_apt =  """
  create table if not exists lpmgt_tmap_cell_build_match_apt
  (
    c_uid	string
    ,sector_id	string
    ,id	string
    ,ru_name	string
    ,class_nm_a	string
    ,class_nm_b	string
    ,class_nm_c	string
    ,class_nm_d	string
    ,exact_ratio	double
    ,exact_num	int
    ,exact_matched_kw	array<string>
    ,tmap_kw	string
    ,partial_ratio	double
    ,partial_num	int
    ,partial_matched_term	array<string>
    ,poi_id	string
    ,cell_inbldg	string
    ,dmap_poi_category_id	string
  )
  stored as orc
"""
insert_query_lpmgt_tmap_cell_build_match_apt =  unicode("""
  with tmp_cell_build_meta_apt as
  (
    select * from lpmgt_cell_build_meta where ru_name not rlike "\\\\(지\\\\)"
  )
  , tmp_tmap_cell_build_join as
  (
    select distinct
      A.poi_id
      ,A.join_key
      ,A.used
      ,A.name1
      ,A.name2
      ,A.tmap_kw
      ,A.tmap_kw_array
      ,A.mapping
      ,A.apt
      ,A.bldng as bldg
      ,A.area
      ,B.distanceki
      ,B.c_uid
      ,B.sector_id
      ,B.city_name
      ,B.gu_name
      ,B.dong_name
      ,B.ru_name
      ,B.cell_name
      ,B.cell_termlist
      ,B.meta_train
      ,B.meta_apt
      ,B.meta_prkg
      ,B.meta_inbldg
      ,C.dmap_poi_category_id
      from
      (select *, concat_ws(":", ca_name, cb_name, cc_name, cd_name) as c_name from lpmgt_tmap_poi_build_meta) A,
      (select * from tmp_cell_build_meta_apt where c_uid is not null) B,
      loc.lpmgt_xls_dmap_tmap_twifi_cell_category_mapp C
      where
      A.join_key= B.join_key
      AND A.c_name = C.tmapdepth1234
  )
  ,tmp_tmap_cell_build_exact_match as
  (
    select
    poi_id
    ,join_key
    ,c_uid
    ,sector_id
    ,distanceki
    ,collect_set(tmap_kws) as tmap_kw_matched
    ,cast(count(*) as int) as num_tmap_kw_matched
      from
      (select * from tmp_tmap_cell_build_join
      lateral view explode(tmap_kw_array) new_tb as tmap_kws) as TMP
      where (cell_name rlike tmap_kws)
      group by
      poi_id
      ,join_key
      ,c_uid
      ,sector_id
      ,distanceki
  )
  ,tmp_tmap_cell_build_partial_match as
  (
    select
    poi_id
    ,join_key
    ,c_uid
    ,sector_id
    ,distanceki
    ,collect_set(term) as cell_2term_matched
    ,cast(count(*) as int) as num_cell_2term_matched
      from
      (select * from tmp_tmap_cell_build_join
      lateral view explode(cell_termlist) new_tb as term) as TMP
      where (tmap_kw rlike term)
      group by
      poi_id
      ,join_key
      ,c_uid
      ,sector_id
      ,distanceki
  )
  , tmap_cell_build_match_view as
  (
    select
    A.c_uid
    ,A.sector_id
    ,A.poi_id
    ,A.distanceki
    ,A.num_tmap_kw_matched/size(A.tmap_kw_array) as exact_ratio
    ,A.num_tmap_kw_matched as exact_num
    ,B.num_cell_2term_matched/size(A.cell_termlist) as partial_ratio
    ,B.num_cell_2term_matched as partial_num
    ,A.join_key
    ,A.tmap_kw_matched as exact_matched_kw
    ,B.cell_2term_matched as partial_matched_term
    ,A.tmap_kw, tmap_kw_array
    ,A.cell_name, cell_termlist
    ,A.meta_train as cell_train
    ,A.meta_apt as cell_apt
    ,A.meta_prkg as cell_prkg
    ,A.meta_inbldg as cell_inbldg
    ,A.apt as tmap_apt
    ,A.bldg as tmap_bldg
    ,A.area as tmap_area
    ,A.dmap_poi_category_id
      from
      (select
        poi_id
        ,join_key
        ,used
        ,name1
        ,name2
        ,tmap_kw
        ,tmap_kw_array
        ,distanceki
        ,c_uid
        ,sector_id
        ,city_name
        ,gu_name
        ,dong_name
        ,ru_name
        ,cell_name
        ,cell_termlist
        ,tmap_kw_matched
        ,num_tmap_kw_matched
        ,key1
        ,meta_train
        ,meta_apt
        ,meta_prkg
        ,meta_inbldg
        ,apt
        ,bldg
        ,area
        ,dmap_poi_category_id
        from
        (select *
          ,concat(poi_id, join_key, c_uid, sector_id, distanceki) as key1
          from tmp_tmap_cell_build_join) as A1
        left join
        (select
          concat(poi_id, join_key, c_uid, sector_id, distanceki) as key2
          ,tmap_kw_matched
          ,num_tmap_kw_matched
          from tmp_tmap_cell_build_exact_match) as A2
        on (A1.key1 = A2.key2)) as A
        left join
        (select
          concat(poi_id, join_key, c_uid, sector_id, distanceki) as key2
          ,cell_2term_matched
          ,num_cell_2term_matched
          from tmp_tmap_cell_build_partial_match) as B
        on (A.key1 = B.key2)
  )

  insert into table lpmgt_tmap_cell_build_match_apt
  select distinct
    A.c_uid
    ,A.sector_id
    ,concat(A.c_uid, "_", A.sector_id) as id
    ,B.ru_name
    ,C.class_nm_a
    ,C.class_nm_b
    ,C.class_nm_c
    ,C.class_nm_d
    ,A.exact_ratio
    ,A.exact_num
    ,A.exact_matched_kw
    ,A.tmap_kw
    ,A.partial_ratio
    ,A.partial_num
    ,A.partial_matched_term
    ,A.poi_id
    ,A.cell_inbldg
    ,A.dmap_poi_category_id
      from
        tmap_cell_build_match_view A
        ,lpmgt_cell_meta B
        ,lpmgt_tmap_meta C
      where
        A.tmap_apt = 1 and
        (A.exact_num > 0 or A.partial_num > 1) and
        A.c_uid = B.c_uid and
        A.sector_id = B.cell_id and
        A.poi_id = C.poi_id
      order by
        c_uid
        ,sector_id
        ,A.exact_ratio desc
        ,C.class_nm_a
        ,C.class_nm_b
        ,C.class_nm_c
        ,C.class_nm_d
""")

create_query_lpmgt_tmap_cell_meta_apt_list =  """
  create table if not exists lpmgt_tmap_cell_meta_apt_list
  (
    id	string
    ,c_uid	string
    ,sector_id	string
    ,ru_name	string
    ,dmap_poi_category_id	string
  )
  stored as orc
"""
insert_query_lpmgt_tmap_cell_meta_apt_list =  unicode("""
  insert into table lpmgt_tmap_cell_meta_apt_list
  select
    B.id
    ,B.c_uid
    ,B.cell_id as sector_id
    ,B.ru_name
    ,"N02040200010000" as dmap_poi_category_id --주거지형태>아파트>아파트단지>기타
    from
      lpmgt_cell_meta B
    where
      ru_name rlike "\\\\(아\\\\)"
      AND id not in (select id from lpmgt_tmap_cell_build_match_apt)
""")

create_query_lpmgt_tmap_cell_keyword_match_kw_list =  """
  create table if not exists lpmgt_tmap_cell_keyword_match_kw_list
  (
    dmap_poi_category_id	string
    ,c_uid	string
    ,sector_id	string
    ,cell_name	string
    ,ru_name	string
    ,keyword	string
    ,keyword_type	string
    ,id	string
  )
"""
insert_query_lpmgt_tmap_cell_keyword_match_kw_list =  unicode("""
  with tmp_cell_meta as
  (
    select *
      ,regexp_extract(cell_name, "(터미널현우빌라|컨테이너터미널|화물터미널|트럭터미널|공항로|대학로|치킨대학|대학동|전주공단|울주공암마을|진주공교사|전주공고|진주공교사항의전대|무주공예테마파크|광주공병대|뒷길|세빛섬|가빛섬|삼거리|사거리|오거리|산곡중앙하이츠|음성무극중|신북온천)", 0) as del_keyword
      ,regexp_extract(cell_name, "(고시텔|기숙사|빌라|아파트|오피스텔|터미널|원주공항|군산공항|광주공항|여수공항|사천공항|울산공항|포항공항|인천국제공항|인천공항|김포국제공항|김포공항|김해국제공항|김해공항|제주국제공항|제주공항|청주국제공항|청주공항|대구국제공항|대구공항|무안국제공항|무안공항|양양국제공항|양양공항|휴게소|씨네마|시네마|극장|쇼핑몰|백화점|아울렛|공원|롯데어드벤처|롯데매직아일랜드어드벤처|경기장|수영장|스키장|운동장|야구장|축구장|호텔|병원)", 0) as add_keyword1
      ,regexp_extract(cell_name, "(대학|주공)", 0) as add_keyword2
      ,regexp_extract(cell_name, "(메세나폴리스|아이파크몰|디큐브시티|스타필드|타임스퀘어|롯데*어드벤처|롯데월드|가든파이브|세종시청사|세종정부청사|세종청사|IFC몰|킨텍스|코엑스|예술의전당|용산전자랜드|하나로마트|빅마트|굿모닝마트|홈마트)", 0) as poi_keyword
       ,regexp_extract(cell_name, "(스탠다드차타드은행출장소|FRESHMARKET|AK플라자|애경백화점|KEB하나은행출장소|한국수출입은행출장소|스탠다드차타드은행|한국씨티은행출장소|홈플러스익스프레스|대림이편한세상|신동아파밀리에|이수브라운스톤|현대힐스테이트|현대산업아이파크|농협중앙회출장소|이마트에브리데이|갤러리아팰리스|경남아너스빌|대우푸르지오|동문굿모닝힐|동부센트레빌|신성미소지움|월드메르디앙|주택휴먼시아|코오롱하늘채|한신휴플러스|한화꿈에그린|지역농협출장소|KEB하나은행|한국수출입은행|경남은행출장소|광주은행출장소|국민은행출장소|기업은행출장소|대구은행출장소|부산은행출장소|신한은행출장소|우리은행출장소|전북은행출장소|제주은행출장소|건영옴니백화점|아이파크백화점|농협파머스마켓|2001아울렛|E마트|이마트|코스트코홀세일|현대산업개발|금호어울림|벽산블루밍|삼성래미안|성원상떼빌|주택뜨란채|포스코더샵|풍림아이원|한라비발디|현대홈타운|한국씨티은행|그랜드백화점|신세계백화점|디자이너클럽|GS슈퍼마켓|농협수퍼마켓|전자랜드21|뉴코아아울렛|GS자이|두산위브|우림필유|농협중앙회|수협출장소|임협출장소|롯데시네마|NC백화점|대구백화점|대우백화점|동아백화점|롯데백화점|태평백화점|행복한세상|현대백화점|테크노마트|하이프라자|그랜드마트|하나로클럽|벽산건설|주택공사|현대건설|롯데캐슬|낙농농협|양돈농협|원예농협|인삼농협|지역농협|축산농협|HSBC|경남은행|광주은행|국민은행|기업은행|대구은행|부산은행|산업은행|신한은행|외국은행|우리은행|전북은행|제주은행|한국은행|메가박스|갤러리아|두산타워|밀리오레|엘리시움|롯데슈퍼|킴스마트|하이마트|롯데마트|메가마트|세이브존|홈플러스|SK뷰|CGV|탑마트|수협|임협|뉴존|메사)", 0) as brand
      from lpmgt_cell_meta
      where
      ru_name not rlike "\\\\(지\\\\)"
      AND id not in
        (select id from lpmgt_tmap_cell_build_match_apt
        union all
        select id from lpmgt_tmap_cell_meta_apt_list)
  )
  ,tmp_cell_keyword_match_kw1 as
  (
    select
      B.dmap_poi_category_id
      ,A.c_uid
      ,A.sector_id
      ,A.cell_name
      ,A.ru_name
      ,A.brand as keyword
      ,"brand" as keyword_type
        from
        (select distinct
          c_uid
          ,cell_id as sector_id
          ,cell_name
          ,brand
          ,ru_name
          ,del_keyword
          from tmp_cell_meta) A
        ,(select distinct
          dmap_poi_category_id
          ,cell_brand_keyword
          ,keywords
          from loc.lpmgt_xls_dmap_tmap_twifi_cell_category_mapp
            lateral view explode(split(cell_brand_keyword, "\\\\|")) new_tb as keywords
          where
          cell_brand_keyword not like "NA") B
        where
        A.del_keyword like ""
        AND A.brand not like ""
        AND A.brand  = B.keywords
  )
  ,tmp_cell_keyword_match_kw2 as
  (
  select
    B.dmap_poi_category_id
    ,A.c_uid
    ,A.sector_id
    ,A.cell_name
    ,A.ru_name
    ,A.add_keyword1 as keyword
    ,"category" as keyword_type
        from
          (select distinct
            c_uid
            ,cell_id as sector_id
            ,cell_name
            ,ru_name
            ,add_keyword1
            ,del_keyword
            ,brand from tmp_cell_meta) A
          ,(select distinct
              dmap_poi_category_id
              ,cell_cate_keyword1
              ,keywords
              from loc.lpmgt_xls_dmap_tmap_twifi_cell_category_mapp
                lateral view explode(split(cell_cate_keyword1, "\\\\|")) new_tb as keywords
            where
              cell_cate_keyword1 not like "NA") B
        where
          del_keyword like ""
          AND brand like ""
          AND add_keyword1 not like ""
          AND A.add_keyword1  = B.keywords
  )
  ,tmp_cell_keyword_match_kw3 as
  (
  select
    dmap_poi_category_id
    ,c_uid
    ,sector_id
    ,cell_name
    ,ru_name
    ,add_keyword1 as keyword
    ,"category" as keyword_type
        from
          (select distinct
            c_uid
            ,cell_id as sector_id
            ,cell_name
            ,ru_name
            ,add_keyword1
            ,add_keyword2
            ,del_keyword
            ,brand from tmp_cell_meta) A
          ,(select distinct
              dmap_poi_category_id
              ,cell_cate_keyword2
              ,keywords
                from loc.lpmgt_xls_dmap_tmap_twifi_cell_category_mapp
                  lateral view explode(split(cell_cate_keyword2, "\\\\|")) new_tb as keywords
            where
              cell_cate_keyword2 not like "NA") B
        where
          del_keyword like ""
          AND brand like ""
          AND add_keyword1 like ""
          AND add_keyword2 not like ""
          AND A.add_keyword2  = B.keywords
  )
  ,tmp_cell_keyword_match_kw4 as
  (
    select
      dmap_poi_category_id
      ,c_uid
      ,sector_id
      ,cell_name
      ,ru_name
      ,poi_keyword as keyword
      ,"poi" as keyword_type
        from
        (select distinct
          c_uid
          ,cell_id as sector_id
          ,cell_name
          ,ru_name
          ,add_keyword1
          ,add_keyword2
          ,del_keyword
          ,brand
          ,poi_keyword
          from tmp_cell_meta) A
        ,(select distinct
          dmap_poi_category_id
          ,cell_poi_keyword
          ,keywords
            from loc.lpmgt_xls_dmap_tmap_twifi_cell_category_mapp
            lateral view explode(split(cell_poi_keyword, "\\\\|")) new_tb as keywords
          where cell_poi_keyword not like "NA") B
        where
        del_keyword like ""
        AND brand like ""
        AND add_keyword1 like ""
        AND add_keyword2 like ""
        AND poi_keyword not like ""
        AND A.poi_keyword  = B.keywords
  )

  insert into table lpmgt_tmap_cell_keyword_match_kw_list
    select *, concat(c_uid, sector_id) as id from
    (select * from tmp_cell_keyword_match_kw1
    union all
    select * from tmp_cell_keyword_match_kw2
    union all
    select * from tmp_cell_keyword_match_kw3
    union all
    select * from tmp_cell_keyword_match_kw4) T
""")

create_query_lpmgt_tmap_cell_build_match_inbldg_list =  """
  create table if not exists lpmgt_tmap_cell_build_match_inbldg_list
  (
    c_uid	string
    ,sector_id	string
    ,ru_name	string
    ,class_nm_a	string
    ,class_nm_b	string
    ,class_nm_c	string
    ,class_nm_d	string
    ,exact_ratio	double
    ,exact_num	int
    ,exact_matched_kw	array<string>
    ,tmap_kw	string
    ,partial_ratio	double
    ,partial_num	int
    ,partial_matched_term	array<string>
    ,poi_id	string
    ,cell_inbldg	string
    ,dmap_poi_category_id	string
  )
  stored as orc
"""
insert_query_lpmgt_tmap_cell_build_match_inbldg_list =  unicode("""
  with tmp_cell_build_meta_ib_area as
  (
    select *
      from
      (select *
        ,concat(c_uid, sector_id) as c_sector_id
        from lpmgt_cell_build_meta) T
      where
      ru_name not rlike "\\\\(지\\\\)"
      AND c_sector_id not in
        (select id from lpmgt_tmap_cell_build_match_apt
        union all
        select id from lpmgt_tmap_cell_meta_apt_list
        union all
        select id from lpmgt_tmap_cell_keyword_match_kw_list)
  )
  ,tmp_tmap_cell_build_join as
  (
    select distinct
      A.poi_id
      ,A.join_key
      ,A.used
      ,A.name1
      ,A.name2
      ,A.tmap_kw
      ,A.tmap_kw_array
      ,A.mapping
      ,A.apt
      ,A.bldng as bldg
      ,A.area
      ,B.distanceki
      ,B.c_uid
      ,B.sector_id
      ,B.city_name
      ,B.gu_name
      ,B.dong_name
      ,B.ru_name
      ,B.cell_name
      ,B.cell_termlist
      ,B.meta_train
      ,B.meta_apt
      ,B.meta_prkg
      ,B.meta_inbldg
      ,C.dmap_poi_category_id
      from
      (select *
        ,concat_ws(":", ca_name, cb_name, cc_name, cd_name) as c_name
        from lpmgt_tmap_poi_build_meta) A
      ,(select *
        from tmp_cell_build_meta_ib_area
        where c_uid is not null) B
      ,loc.lpmgt_xls_dmap_tmap_twifi_cell_category_mapp C
      where
      A.join_key= B.join_key
      AND A.c_name = C.tmapdepth1234
  )
  ,tmp_tmap_cell_build_exact_match as
  (
    select
      poi_id
      ,join_key
      ,c_uid
      ,sector_id
      ,distanceki
      ,collect_set(tmap_kws) as tmap_kw_matched
      ,cast(count(*) as int) as num_tmap_kw_matched
      ,tmap_name_matched
      from
        (select *
        ,case when cell_name rlike tmap_kw_array[0] then 1 else 0 end as tmap_name_matched
          from tmp_tmap_cell_build_join
          lateral view explode(tmap_kw_array) new_tb as tmap_kws) as TMP
      where
        cell_name rlike tmap_kws
      group by
        poi_id
        ,join_key
        ,c_uid
        ,sector_id
        ,distanceki
        ,tmap_name_matched
  )
  ,tmp_tmap_cell_build_partial_match as
  (
    select
      poi_id
      ,join_key
      ,c_uid
      ,sector_id
      ,distanceki
      ,collect_set(term) as cell_2term_matched
      ,size(collect_set(term)) as num_cell_2term_matched
      ,collect_set(tmap_kws) as tmapkw_2term_matched
      ,size(collect_set(tmap_kws)) as num_tmapkw_2term_matched
      from
        (select * from
        (select * from
          tmp_tmap_cell_build_join
          lateral view explode(cell_termlist) new_tb as term) as A
            lateral view explode(tmap_kw_array) new_tb2 as tmap_kws) as B
      where
        tmap_kws rlike term
      group by
        poi_id
        ,join_key
        ,c_uid
        ,sector_id
        ,distanceki
  )
  ,tmap_cell_build_match_view as
  (
    select
      A.c_uid
      ,A.sector_id
      ,A.poi_id
      ,A.distanceki
      ,A.num_tmap_kw_matched/size(A.tmap_kw_array) as exact_ratio
      ,A.num_tmap_kw_matched as exact_num
      ,A.tmap_name_matched as name_matched_yn
      ,B.num_cell_2term_matched/size(A.cell_termlist) as partial_ratio
      ,B.num_cell_2term_matched as partial_num
      ,B.tmapkw_2term_matched as partial_kwnum
      ,B.num_tmapkw_2term_matched as partial_matched_kw
      ,A.join_key
      ,A.tmap_kw_matched as exact_matched_kw
      ,B.cell_2term_matched as partial_matched_term
      ,A.tmap_kw, tmap_kw_array
      ,A.cell_name, cell_termlist
      ,A.meta_train as cell_train
      ,A.meta_apt as cell_apt
      ,A.meta_prkg as cell_prkg
      ,A.meta_inbldg as cell_inbldg
      ,A.apt as tmap_apt
      ,A.bldg as tmap_bldg
      ,A.area as tmap_area
      ,A.dmap_poi_category_id
      from
        (select
        poi_id
        ,join_key
        ,used
        ,name1
        ,name2
        ,tmap_kw
        ,tmap_kw_array
        ,distanceki
        ,c_uid
        ,sector_id
        ,city_name
        ,gu_name
        ,dong_name
        ,ru_name
        ,cell_name
        ,cell_termlist
        ,tmap_name_matched
        ,tmap_kw_matched
        ,num_tmap_kw_matched
        ,key1
        ,meta_train
        ,meta_apt
        ,meta_prkg
        ,meta_inbldg
        ,apt
        ,bldg
        ,area
        ,dmap_poi_category_id
          from
          (select *
            ,concat(poi_id, join_key, c_uid, sector_id, distanceki) as key1
            from
              tmp_tmap_cell_build_join) as A1
            left join
            (select
              concat(poi_id, join_key, c_uid, sector_id, distanceki) as key2
              ,tmap_name_matched
              ,tmap_kw_matched
              ,num_tmap_kw_matched
              from tmp_tmap_cell_build_exact_match) as A2
            on A1.key1 = A2.key2) as A
          left join
          (select
            concat(poi_id, join_key, c_uid, sector_id, distanceki) as key2
            ,cell_2term_matched
            ,num_cell_2term_matched
            ,tmapkw_2term_matched
            ,num_tmapkw_2term_matched
            from
              tmp_tmap_cell_build_partial_match) as B
          on A.key1 = B.key2
  )

  insert into table lpmgt_tmap_cell_build_match_inbldg_list
  select distinct
    A.c_uid,
    A.sector_id,
    B.ru_name,
    C.class_nm_a,
    C.class_nm_b,
    C.class_nm_c,
    C.class_nm_d,
    A.exact_ratio,
    A.exact_num,
    A.exact_matched_kw,
    A.tmap_kw,
    A.partial_ratio,
    A.partial_num,
    A.partial_matched_term,
    A.poi_id,
    A.cell_inbldg,
    A.dmap_poi_category_id
      from
        (select *
          ,row_number() over
            (partition by
              c_uid
              ,sector_id
              order by
                name_matched_yn desc
                ,exact_num desc
                ,exact_ratio desc
                ,partial_num desc
                ,partial_kwnum desc) as rnk
          from
            tmap_cell_build_match_view) A
            ,lpmgt_cell_meta B
            ,lpmgt_tmap_meta C
      where
        A.tmap_bldg = 1 and
        C.class_nm_c not like "주차장" and
        (A.exact_ratio > 0 or partial_ratio > 2) and
        A.c_uid = B.c_uid and
        A.sector_id = B.cell_id and
        A.poi_id = C.poi_id and
        A.rnk=1
      order by
        c_uid, sector_id
        ,A.exact_ratio desc
        ,C.class_nm_a
        ,C.class_nm_b
        ,C.class_nm_c
        ,C.class_nm_d
""")

create_query_lpmgt_tmap_cell_build_match_area_list =  """
  create table if not exists lpmgt_tmap_cell_build_match_area_list
  (
    c_uid	string
    ,sector_id	string
    ,ru_name	string
    ,class_nm_a	string
    ,class_nm_b	string
    ,class_nm_c	string
    ,class_nm_d	string
    ,exact_ratio	double
    ,exact_num	int
    ,exact_matched_kw	array<string>
    ,tmap_kw	string
    ,partial_ratio	double
    ,partial_num	int
    ,partial_matched_term	array<string>
    ,poi_id	string
    ,cell_inbldg	string
    ,dmap_poi_category_id	string
  )
  stored as orc
"""
insert_query_lpmgt_tmap_cell_build_match_area_list =  unicode("""
  with
  tmp_cell_build_meta_ib_area as
  (
  select *
    from
      (select *
        ,concat(c_uid, sector_id) as c_sector_id
          from lpmgt_cell_build_meta) T
    where
      ru_name not rlike "\\\\(지\\\\)"
      AND c_sector_id not in
        (select id from lpmgt_tmap_cell_build_match_apt
          union all
          select id from lpmgt_tmap_cell_meta_apt_list
          union all
          select id from lpmgt_tmap_cell_keyword_match_kw_list)
  )
  ,tmp_tmap_cell_build_join as
  (
    select distinct
      A.poi_id
      ,A.join_key
      ,A.used
      ,A.name1
      ,A.name2
      ,A.tmap_kw
      ,A.tmap_kw_array
      ,A.mapping
      ,A.apt
      ,A.bldng as bldg
      ,A.area
      ,B.distanceki
      ,B.c_uid
      ,B.sector_id
      ,B.city_name
      ,B.gu_name
      ,B.dong_name
      ,B.ru_name
      ,B.cell_name
      ,B.cell_termlist
      ,B.meta_train
      ,B.meta_apt
      ,B.meta_prkg
      ,B.meta_inbldg
      ,C.dmap_poi_category_id
      from
      (select *
        ,concat_ws(":", ca_name, cb_name, cc_name, cd_name) as c_name
        from lpmgt_tmap_poi_build_meta) A
      ,(select *
        from tmp_cell_build_meta_ib_area
        where c_uid is not null) B
      ,loc.lpmgt_xls_dmap_tmap_twifi_cell_category_mapp C
      where
      A.join_key= B.join_key
      AND A.c_name = C.tmapdepth1234
  )
  ,tmp_tmap_cell_build_exact_match as
  (
    select
      poi_id
      ,join_key
      ,c_uid
      ,sector_id
      ,distanceki
      ,collect_set(tmap_kws) as tmap_kw_matched
      ,cast(count(*) as int) as num_tmap_kw_matched
      ,tmap_name_matched
      from
        (select *
        ,case when cell_name rlike tmap_kw_array[0] then 1 else 0 end as tmap_name_matched
          from tmp_tmap_cell_build_join
          lateral view explode(tmap_kw_array) new_tb as tmap_kws) as TMP
      where
        cell_name rlike tmap_kws
      group by
        poi_id
        ,join_key
        ,c_uid
        ,sector_id
        ,distanceki
        ,tmap_name_matched
  )
  ,tmp_tmap_cell_build_partial_match as
  (
    select
      poi_id
      ,join_key
      ,c_uid
      ,sector_id
      ,distanceki
      ,collect_set(term) as cell_2term_matched
      ,size(collect_set(term)) as num_cell_2term_matched
      ,collect_set(tmap_kws) as tmapkw_2term_matched
      ,size(collect_set(tmap_kws)) as num_tmapkw_2term_matched
      from
        (select * from
        (select * from
          tmp_tmap_cell_build_join
          lateral view explode(cell_termlist) new_tb as term) as A
            lateral view explode(tmap_kw_array) new_tb2 as tmap_kws) as B
      where
        tmap_kws rlike term
      group by
        poi_id
        ,join_key
        ,c_uid
        ,sector_id
        ,distanceki
  )
  ,tmap_cell_build_match_view as
  (
    select
      A.c_uid
      ,A.sector_id
      ,A.poi_id
      ,A.distanceki
      ,A.num_tmap_kw_matched/size(A.tmap_kw_array) as exact_ratio
      ,A.num_tmap_kw_matched as exact_num
      ,A.tmap_name_matched as name_matched_yn
      ,B.num_cell_2term_matched/size(A.cell_termlist) as partial_ratio
      ,B.num_cell_2term_matched as partial_num
      ,B.tmapkw_2term_matched as partial_kwnum
      ,B.num_tmapkw_2term_matched as partial_matched_kw
      ,A.join_key
      ,A.tmap_kw_matched as exact_matched_kw
      ,B.cell_2term_matched as partial_matched_term
      ,A.tmap_kw, tmap_kw_array
      ,A.cell_name, cell_termlist
      ,A.meta_train as cell_train
      ,A.meta_apt as cell_apt
      ,A.meta_prkg as cell_prkg
      ,A.meta_inbldg as cell_inbldg
      ,A.apt as tmap_apt
      ,A.bldg as tmap_bldg
      ,A.area as tmap_area
      ,A.dmap_poi_category_id
      from
        (select
        poi_id
        ,join_key
        ,used
        ,name1
        ,name2
        ,tmap_kw
        ,tmap_kw_array
        ,distanceki
        ,c_uid
        ,sector_id
        ,city_name
        ,gu_name
        ,dong_name
        ,ru_name
        ,cell_name
        ,cell_termlist
        ,tmap_name_matched
        ,tmap_kw_matched
        ,num_tmap_kw_matched
        ,key1
        ,meta_train
        ,meta_apt
        ,meta_prkg
        ,meta_inbldg
        ,apt
        ,bldg
        ,area
        ,dmap_poi_category_id
          from
          (select *
            ,concat(poi_id, join_key, c_uid, sector_id, distanceki) as key1
            from
              tmp_tmap_cell_build_join) as A1
            left join
            (select
              concat(poi_id, join_key, c_uid, sector_id, distanceki) as key2
              ,tmap_name_matched
              ,tmap_kw_matched
              ,num_tmap_kw_matched
              from tmp_tmap_cell_build_exact_match) as A2
            on A1.key1 = A2.key2) as A
          left join
          (select
            concat(poi_id, join_key, c_uid, sector_id, distanceki) as key2
            ,cell_2term_matched
            ,num_cell_2term_matched
            ,tmapkw_2term_matched
            ,num_tmapkw_2term_matched
            from
              tmp_tmap_cell_build_partial_match) as B
          on A.key1 = B.key2
  )

  insert into table lpmgt_tmap_cell_build_match_area_list
  select distinct
    A.c_uid
    ,A.sector_id
    ,B.ru_name
    ,C.class_nm_a
    ,C.class_nm_b
    ,C.class_nm_c
    ,C.class_nm_d
    ,A.exact_ratio
    ,A.exact_num
    ,A.exact_matched_kw
    ,A.tmap_kw
    ,A.partial_ratio
    ,A.partial_num
    ,A.partial_matched_term
    ,A.poi_id
    ,A.cell_inbldg
    ,A.dmap_poi_category_id
      from
        (select *
          ,row_number() over
            (partition by c_uid, sector_id
              order by
                name_matched_yn desc
                ,exact_num desc
                ,exact_ratio desc
                ,partial_num desc
                ,partial_kwnum desc) as rnk
          from
          tmap_cell_build_match_view) A
          ,lpmgt_cell_meta B
          ,lpmgt_tmap_meta C
      where
        A.tmap_area = 1 and
        (A.exact_ratio > 0 or partial_ratio > 2) and
        A.c_uid = B.c_uid and
        A.sector_id = B.cell_id and
        A.poi_id = C.poi_id and
        A.rnk=1
      order by
        c_uid, sector_id
        ,A.exact_ratio desc
        ,C.class_nm_a
        ,C.class_nm_b
        ,C.class_nm_c
        ,C.class_nm_d
""")

create_query_lpmgt_dmap_cell_poi_mapp =  """
  create table if not exists lpmgt_dmap_cell_poi_mapp
  (
    dmap_poi_id	string
    ,dmap_poi_category_id	string
    ,c_uid	string
    ,sector_id	string
    ,transport_yn	boolean
  )
  stored as orc
"""
insert_query_lpmgt_dmap_cell_poi_mapp =  unicode("""
  insert into table lpmgt_dmap_cell_poi_mapp
  select
    B.dmap_poi_id,
    A.dmap_poi_category_id,
    A.c_uid,
    A.sector_id,
    FALSE as transport_yn
    from
      (select dmap_poi_category_id, c_uid, sector_id, poi_id from lpmgt_tmap_cell_build_match_apt
      union all
      select dmap_poi_category_id, c_uid, sector_id, poi_id from lpmgt_tmap_cell_build_match_area_list
      union all
      select dmap_poi_category_id, c_uid, sector_id, poi_id from lpmgt_tmap_cell_build_match_inbldg_list) A,
      lpmgt_dmap_tmap_poi_mapp B
  where A.poi_id = B.tmap_poi_id

  union all

  -- tmap poi_id 매치 안된 기지국
  select
    concat(A.dmap_poi_category_id,
    B.dong_id, "00","CE",
    repeat("0",(10-length(A.c_uid))), A.c_uid,
    repeat("0",(2-length(A.sector_id))), A.sector_id) as dmap_poi_id,
    A.dmap_poi_category_id,
    A.c_uid,
    A.sector_id,
    A.transport_yn
    from
        (select dmap_poi_category_id, c_uid, sector_id, TRUE as transport_yn from lpmgt_dmap_cell_poi_mapp_by_cate
        union all
        select dmap_poi_category_id, c_uid, sector_id, FALSE as transport_yn from lpmgt_tmap_cell_meta_apt_list
        union all
        select dmap_poi_category_id, c_uid, sector_id, FALSE as transport_yn from lpmgt_tmap_cell_keyword_match_kw_list) A,
        lpmgt_cell_meta B
    where
        A.c_uid = B.c_uid
        AND A.sector_id = B.cell_id
""")

create_query_lpmgt_dmap_twifi_poi_mapp =  """
  create table if not exists lpmgt_dmap_twifi_poi_mapp
  (
   dmap_poi_id string
   ,dmap_poi_category_id string
   ,twifi_mac string
  )
  stored as orc
"""
insert_query_lpmgt_dmap_twifi_poi_mapp =  """
  with
  lpmgt_dmap_twifi_poi_mapp_by_cate as
  (
    select
      A.dmap_poi_category_id
      ,B.mac as twifi_mac
      from
      (select distinct
        dmap_poi_category_id
        ,twifidepth12
        from
          loc.lpmgt_xls_dmap_tmap_twifi_cell_category_mapp) A
      ,(select
        mac
        ,concat(ca,":",cb) as category
          from src.xls_twifi_meta) B
      where
      (A.twifidepth12 = B.category)
  )
  insert into table lpmgt_dmap_twifi_poi_mapp
    select
      concat(A.dmap_poi_category_id,
      "0000000000",
      "TW", repeat("0",(12-length(A.twifi_mac))), A.twifi_mac) as dmap_poi_id,
      dmap_poi_category_id,
      twifi_mac
    from
      lpmgt_dmap_twifi_poi_mapp_by_cate A
"""

create_query_lpmgt_dmap_poi_meta =  """
  create table if not exists lpmgt_dmap_poi_meta
  (
    dmap_poi_id	string			,dmap_poi_category_id	string			,name	string			,poi_source	string			,type	string			,depth1	string			,depth2	string			,depth3	string			,depth4	string			,depth5	string			,latitude	double			,longitude	double
  )
  stored as orc
"""
insert_query_lpmgt_dmap_poi_meta =  """
  with tmp_dmap_poi_mapp_all
  as
  (
    select
      A.dmap_poi_id
      ,A.dmap_poi_category_id
      ,cast(NULL as STRING) as tmap_poi_id
      ,A.c_uid
      ,A.sector_id
      ,cast(NULL as STRING) as twifi_mac
      ,A.transport_yn
            ,"cell" as poi_source
      from lpmgt_dmap_cell_poi_mapp A
    union all
    select
      B.dmap_poi_id
      ,B.dmap_poi_category_id
      ,B.tmap_poi_id
      ,cast(NULL as STRING) as c_uid
      ,cast(NULL as STRING) as sector_id
      ,cast(NULL as STRING) as twifi_mac
      ,cast(NULL as BOOLEAN) as transport_yn
            ,"tmap" as poi_source
      from lpmgt_dmap_tmap_poi_mapp B
    union all
    select
      C.dmap_poi_id
      ,C.dmap_poi_category_id
      ,cast(NULL as STRING) as tmap_poi_id
      ,cast(NULL as STRING) as c_uid
      ,cast(NULL as STRING) as sector_id
      ,C.twifi_mac
      ,cast(NULL as BOOLEAN) as transport_yn
            ,"twifi" as poi_source
      from lpmgt_dmap_twifi_poi_mapp C
  )
  insert into lpmgt_dmap_poi_meta
  select distinct
    A.dmap_poi_id
    ,A.dmap_poi_category_id
    ,B.name_org as name
    ,"tmap" as poi_source
    ,A.type
    ,A.depth1
    ,A.depth2
    ,A.depth3
    ,A.depth4
    ,A.depth5
    ,B.center_wgs84_lat as latitude
    ,B.center_wgs84_lon as longitude
      from
        (select
          A1.dmap_poi_id
          ,A1.dmap_poi_category_id
          ,A1.tmap_poi_id
          ,A1.c_uid
          ,A1.sector_id
          ,A1.twifi_mac
          ,A1.transport_yn
          ,A2.type
          ,A2.depth1, A2.depth2, A2.depth3, A2.depth4, A2.depth5
          from
          (select *
              from tmp_dmap_poi_mapp_all
              where poi_source="tmap") A1
          , loc.lpmgt_meta_dmap_poi A2
          where A1.dmap_poi_category_id=A2.dmap_poi_category_id) A
        , lpmgt_tmap_meta B
      where
        A.tmap_poi_id = B.poi_id

  union all

  select distinct
    A.dmap_poi_id
    ,A.dmap_poi_category_id
    ,B.ru_name as name
    ,"cell" as poi_source
    ,A.type
    ,A.depth1
    ,A.depth2
    ,A.depth3
    ,A.depth4
    ,A.depth5
    ,B.latitude
    ,B.longitude
      from
        (select
          A1.dmap_poi_id
          ,A1.dmap_poi_category_id
          ,A1.tmap_poi_id
          ,A1.c_uid
          ,A1.sector_id
          ,A1.twifi_mac
          ,A1.transport_yn
          ,A2.type
          ,A2.depth1, A2.depth2, A2.depth3, A2.depth4, A2.depth5
          from
          (select *
              from tmp_dmap_poi_mapp_all
              where poi_source="cell") A1
          , loc.lpmgt_meta_dmap_poi A2
          where A1.dmap_poi_category_id=A2.dmap_poi_category_id) A
        , lpmgt_cell_meta B
      where
        A.c_uid = B.c_uid
        AND A.sector_id = B.cell_id

  union all

  select distinct
    A.dmap_poi_id
    ,A.dmap_poi_category_id
    ,B.name as name
    ,"twifi" as poi_source
    ,A.type
    ,A.depth1
    ,A.depth2
    ,A.depth3
    ,A.depth4
    ,A.depth5
    ,B.latitude
    ,B.longitude
      from
        (select
          A1.dmap_poi_id
          ,A1.dmap_poi_category_id
          ,A1.tmap_poi_id
          ,A1.c_uid
          ,A1.sector_id
          ,A1.twifi_mac
          ,A1.transport_yn
          ,A2.type
          ,A2.depth1, A2.depth2, A2.depth3, A2.depth4, A2.depth5
          from
          (select *
              from tmp_dmap_poi_mapp_all
              where poi_source="twifi") A1
          , loc.lpmgt_meta_dmap_poi A2
          where A1.dmap_poi_category_id=A2.dmap_poi_category_id) A
        ,src.xls_twifi_meta B
      where
        A.twifi_mac = B.mac
"""

tablenames = [
  "lpmgt_tmap_meta",
  "lpmgt_dmap_tmap_poi_mapp",
  "lpmgt_cell_meta_subway",
  "lpmgt_dmap_cell_poi_mapp_by_cate",
  "lpmgt_tmap_poi_build_meta",
  "lpmgt_tmap_cell_build_match_apt",
  "lpmgt_tmap_cell_meta_apt_list",
  "lpmgt_tmap_cell_keyword_match_kw_list",
  "lpmgt_tmap_cell_build_match_inbldg_list",
  "lpmgt_tmap_cell_build_match_area_list",
  "lpmgt_dmap_cell_poi_mapp",
  "lpmgt_dmap_twifi_poi_mapp",
  "lpmgt_dmap_poi_meta",
]

create_queries = [
  create_query_lpmgt_tmap_meta,
  create_query_lpmgt_dmap_tmap_poi_mapp,
  create_query_lpmgt_cell_meta_subway,
  create_query_lpmgt_dmap_cell_poi_mapp_by_cate,
  create_query_lpmgt_tmap_poi_build_meta,
  create_query_lpmgt_tmap_cell_build_match_apt,
  create_query_lpmgt_tmap_cell_meta_apt_list,
  create_query_lpmgt_tmap_cell_keyword_match_kw_list,
  create_query_lpmgt_tmap_cell_build_match_inbldg_list,
  create_query_lpmgt_tmap_cell_build_match_area_list,
  create_query_lpmgt_dmap_cell_poi_mapp,
  create_query_lpmgt_dmap_twifi_poi_mapp,
  create_query_lpmgt_dmap_poi_meta,
]

