---------------------- demo.dewip_svc_fmly_rel
INSERT OVERWRITE TABLE demo.dewip_svc_fmly_rel -- s001_dml_dewip_svc_fmly_rel.sql
SELECT svc_mgmt_num AS svc_mgmt_num  -- 서비스관리번호
       ,CASE WHEN max(wedd_yn) = 1       THEN 'Y' ELSE '#' END AS wedd_yn        -- 결혼여부
       ,CASE WHEN max(chld_exist_yn) = 1 THEN 'Y' ELSE '#' END AS chld_exist_yn  -- 자녀유무
       ,FROM_UNIXTIME(unix_timestamp(),'yyyyMMddHHmmss')       AS oper_dtm       -- 작업일시
  FROM (
        -- 카드수납 (가입자와의관계코드)
        SELECT rep_svc_mgmt_num AS svc_mgmt_num
               ,CASE scrbr_rel_cd WHEN '031' THEN 1 ELSE 0 END AS wedd_yn
               ,CASE scrbr_rel_cd WHEN '050' THEN 1 ELSE 0 END AS chld_exist_yn
          FROM wind.td_zpay_card_pay
         WHERE scrbr_rel_cd IN ('031','050')  -- 가입자와의관계코드 : 031(배우자), 050(자녀)
        UNION ALL

        -- 계정변경이력 (신청인관계코드)
        SELECT t2.rep_svc_mgmt_num AS svc_mgmt_num
               ,CASE t1.reqr_rel_cd WHEN '031' THEN 1 ELSE 0 END AS wedd_yn        -- 배우자
               ,CASE t1.reqr_rel_cd WHEN '050' THEN 1 ELSE 0 END AS chld_exist_yn  -- 자녀
          FROM wind.td_zord_acnt_chg_hst t1
               ,wind.td_zord_acnt t2
         WHERE t1.reqr_rel_cd IN ('031','050')  -- 신청인관계코드 : 031(배우자), 050(자녀)
               AND t1.acnt_num = t2.acnt_num
        UNION ALL

        -- 자동납부신청명세  (예금주관계코드/대리인관계코드)
         SELECT t2.rep_svc_mgmt_num AS svc_mgmt_num
                ,CASE WHEN t1.dpstr_rel_cd = '031' OR t1.agnt_rel_cd = '031' THEN 1 ELSE 0 END AS wedd_yn
                ,CASE WHEN t1.dpstr_rel_cd = '050' OR t1.agnt_rel_cd = '050' THEN 1 ELSE 0 END AS chld_exist_yn
          FROM wind.td_zpay_ap_req_spc t1
               ,wind.td_zord_acnt t2
         WHERE (t1.dpstr_rel_cd IN ('031','050') OR t1.agnt_rel_cd IN ('031','050'))
           AND t1.acnt_num = t2.acnt_num
         union ALL

        -- 멤버십카드 (서비스명의자와의관계코드)
        SELECT svc_mgmt_num AS svc_mgmt_num
               ,CASE WHEN svc_nominal_rel_cd = '031' AND LENGTH(TRIM(mbr_card_num1)) = 10 THEN 1 ELSE 0 END AS wedd_yn
               ,CASE WHEN svc_nominal_rel_cd = '050' AND LENGTH(TRIM(mbr_card_num1)) = 10 THEN 1 ELSE 0 END AS chld_exist_yn
          FROM wind.td_zmbr_mbr_card
         WHERE svc_nominal_rel_cd IN ('031','050')  -- 서비스명의자와의관계코드 : 031(배우자), 050(자녀)
       ) v
  GROUP BY svc_mgmt_num
;