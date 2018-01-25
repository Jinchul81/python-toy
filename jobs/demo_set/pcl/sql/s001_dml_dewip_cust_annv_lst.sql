---------------------- demo.dewip_cust_annv_lst
INSERT OVERWRITE TABLE demo.dewip_cust_annv_lst -- s001_dml_dewip_cust_annv_lst.sql
SELECT cust_num  -- 고객번호
       ,CASE WHEN LENGTH(MAX(TRIM(wedd_annv))) = 8 OR LENGTH(MAX(TRIM(spouse_birth))) = 8 THEN 'Y' ELSE '#' END AS wedd_yn  -- 결혼여부
       ,MAX(wedd_annv)              AS wedd_annv              -- 결혼기념일
       ,MAX(wedd_annv_slnl_cd)      AS wedd_annv_slnl_cd      -- 결혼기념일 양음구분코드
       ,MAX(spouse_birth)           AS spouse_birth           -- 배우자생일
       ,MAX(spouse_birth_slnl_cd)   AS spouse_birth_slnl_cd   -- 배우자생일 양음구분코드
       ,CASE WHEN LENGTH(MAX(TRIM(rep_chld_birth))) = 8 THEN 'Y' ELSE '#'END AS chld_exist_yn  -- 자녀존재유무
       ,MAX(rep_chld_birth)         AS rep_chld_birth         -- 대표자녀생일
       ,MAX(rep_chld_birth_slnl_cd) AS rep_chld_birth_slnl_cd -- 대표자녀생일 양음구분코드
       ,FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyyMMddHHmmss') AS oper_dtm -- 작업일시
  FROM (
        SELECT cust_num
               ,CASE WHEN annv_cl_cd = '01' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_dt END      AS wedd_annv            -- 결혼기념일
               ,CASE WHEN annv_cl_cd = '01' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_slnl_cd END AS wedd_annv_slnl_cd    -- 결혼기념일양음구분코드
               ,CASE WHEN annv_cl_cd = '03' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_dt END      AS spouse_birth         -- 배우자생일
               ,CASE WHEN annv_cl_cd = '03' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_slnl_cd END AS spouse_birth_slnl_cd -- 배우자생일양음구분코드
               ,CASE
                     WHEN annv_cl_cd = '08' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_dt       -- 첫째자녀생일
                     WHEN annv_cl_cd = '09' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_dt       -- 둘째자녀생일
                     WHEN annv_cl_cd = '10' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_dt       -- 셋째자녀생일
                     WHEN annv_cl_cd = '11' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_dt       -- 넷째자녀생일
                     WHEN annv_cl_cd = '12' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_dt       -- 다자녀감면생일
                 END AS rep_chld_birth    -- 대표자녀생일
               ,CASE
                     WHEN annv_cl_cd = '08' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_slnl_cd  -- 첫째자녀생일 양음구분
                     WHEN annv_cl_cd = '09' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_slnl_cd  -- 둘째자녀생일 양음구분
                     WHEN annv_cl_cd = '10' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_slnl_cd  -- 셋째자녀생일 양음구분
                     WHEN annv_cl_cd = '11' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_slnl_cd  -- 넷째자녀생일 양음구분
                     WHEN annv_cl_cd = '12' AND LENGTH(TRIM(annv_dt)) = 8 THEN annv_slnl_cd  -- 다자녀감면생일 양음구분
                 END AS rep_chld_birth_slnl_cd  -- 대표자녀생일 양음구분코드
          FROM wind.td_zord_cust_annv_lst
         WHERE annv_cl_cd IN ('12','02','22','01','03','08','09','10','11')
       ) V
  GROUP BY cust_num
;