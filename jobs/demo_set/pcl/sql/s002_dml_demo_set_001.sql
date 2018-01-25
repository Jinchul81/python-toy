SET hive.exec.dynamic.partition.mode=nonstrict;

---------------------- demo.demo_set_001
INSERT OVERWRITE TABLE demo.demo_set_001 PARTITION(strd_ym) -- s002_dml_demo_set_001.sql
SELECT t1.svc_mgmt_num          AS svc_mgmt_num -- 서비스관리번호
       ,t1.sex_cd               AS sex_cd       -- 성별코드
       ,t1.age                  AS age          -- 연령
       ,t1.job_cd               AS job_cd       -- 직업코드
       ,CASE WHEN t2.wedd_yn='Y'       THEN 'Y'
             WHEN t3.wedd_yn='Y'       THEN 'Y'
             WHEN t2.chld_exist_yn='Y' THEN 'Y'
             WHEN t3.chld_exist_yn='Y' THEN 'Y'
             ELSE '#'
        END AS wedd_yn  -- 결혼여부
       ,CASE WHEN t2.chld_exist_yn='Y' THEN 'Y'
             WHEN t3.chld_exist_yn='Y' THEN 'Y'
             ELSE '#'
        END AS chld_exist_yn  -- 자녀존재유무
       ,t1.scrb_yr_cnt          AS scrb_yr_cnt          -- 가입년수
       ,t1.data_use_qty         AS data_use_qty         -- 데이터사용량(bytes)
       ,t1.dom_vcall_use_tms    AS dom_vcall_use_tms    -- 국내음성통화사용시간
       ,t1.pay_bank_card_co_cd  AS pay_bank_card_co_cd  -- 요금납부은행카드사코드
       ,t1.eqp_mfact_cd         AS eqp_mfact_d          -- 단말생산업체코드
       ,from_unixtime(unix_timestamp(),'yyyyMMddHHmmss')  AS oper_dtm -- 작업일시
       ,t1.strd_ym              AS strd_ym              -- 기준년월
  FROM demo.dewip_svc t1
       LEFT OUTER JOIN demo.dewip_svc_fmly_rel t2  ON t1.svc_mgmt_num = t2.svc_mgmt_num
       LEFT OUTER JOIN demo.dewip_cust_annv_lst t3 ON t1.cust_num = t3.cust_num
 WHERE t1.svc_st_cd IN ('AC','SP')   -- 서비스상태코드가 '사용중' '정지'
;
