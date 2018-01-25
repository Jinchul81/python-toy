---------------------- demo.dewip_svc
INSERT OVERWRITE TABLE demo.dewip_svc -- s001_dml_dewip_svc.sql
SELECT t1.strd_ym                 AS strd_ym             -- 기준년월
       ,t1.svc_mgmt_num           AS svc_mgmt_num        -- 서비스관리번호
       ,t1.nm_cust_num            AS cust_num            -- 고객번호
       ,t1.sex_cd                 AS sex_cd              -- 성별코드
       ,t1.mth_age                AS age                 -- 연령(원천은 월령이나 실데이터값이 연령임)
       ,t1.job_cd                 AS job_cd              -- 직업코드
       ,t2.svc_st_cd              AS svc_st_cd           -- 서비스상태코드
       ,t2.ltrm_dc_strd_dt        AS ltrm_dc_strd_dt     -- 장기할인기준일자
       ,CASE WHEN LENGTH(TRIM(t2.ltrm_dc_strd_dt)) = 8
             THEN FLOOR(DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())), TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(t2.ltrm_dc_strd_dt,'yyyyMMdd'))))/365)
             ELSE 0                                      -- 장기할인일자가 비정상적이면 가입년수를 0으로 처리
        END AS scrb_yr_cnt  -- 가입년수
       ,(t1.dom_pckt_cnt + t1.dom_pckt_lte_cnt + t1.dom_pckt_tot_web_cnt) * 512 AS data_use_qty -- 데이터통화사용량(1패킷=512byte)
       ,t1.dom_cir_voice_use_tms  AS dom_vcall_use_tms   -- 국내음성통화사용시간
       ,t1.bank_card_co_cd        AS pay_bank_card_co_cd -- 납부은행카드사코드
       ,t1.eqp_mfact_cd           AS eqp_mfact_cd        -- 단말생산업체코드
       ,FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyyMMddHHmmss') AS oper_dtm -- 작업일시
  FROM wind.mmkt_svc_bas_f t1 LEFT OUTER JOIN wind.td_zord_svc t2 ON t1.svc_mgmt_num = t2.svc_mgmt_num
;