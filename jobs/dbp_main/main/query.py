#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

create_table_dbp_main = """
    CREATE TABLE IF NOT EXISTS dbp_main AS
    WITH q1 AS (
        SELECT
                *
            FROM
                {wind_database}.td_zord_svc
            WHERE
                svc_st_cd = 'AC'
                AND co_cl_cd IN (
                    'T'
                    ,'A'
                )
                AND svc_cd IN (
                    'A'
                    ,'C'
                    ,'I'
                    ,'P'
                )
                AND substr( svc_mgmt_num ,10 ,1 ) IN (
                    '0'
                    ,'1'
                    ,'2'
                    ,'3'
                    ,'4'
                )
    )
    ,q2 AS (
     SELECT
                cntrct_mgmt_num
                ,nm_cust_num
                ,acnt_num
            FROM
                {wind_database}.td_zord_cntrct
            WHERE
                use_cntrct_st_cd = 'AC'
                AND co_cl_cd IN (
                    'T'
                    ,'A'
                )
    )
    ,q3 AS (
        SELECT
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,b.nm_cust_num
                ,b.acnt_num
            FROM
                q1 a LEFT JOIN q2 b
                    ON a.cntrct_mgmt_num = b.cntrct_mgmt_num
            WHERE
                svc_st_cd = 'AC'
                AND co_cl_cd IN (
                    'T'
                    ,'A'
                )
    )
    ,q4 AS (
     SELECT
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,MAX (b.email04) AS email04
                ,MAX (b.email05) AS email05
                ,MAX (b.email08) AS email08
            FROM
                q3 a LEFT JOIN (
                    SELECT
                            cust_num
                            ,CASE
                                WHEN cntc_plc_cl_cd = '04'
                                THEN cntc_plc_cl_cd
                            END AS email04
                            ,CASE
                                WHEN cntc_plc_cl_cd = '05'
                                THEN cntc_plc_cl_cd
                            END AS email05
                            ,CASE
                                WHEN cntc_plc_cl_cd = '08'
                                THEN cntc_plc_cl_cd
                            END AS email08 --,
     --case when cntc_plc_cl_cd='12' then cntc_plc_cl_cd end as email12,
     --case when cntc_plc_cl_cd='13' then cntc_plc_cl_cd end as email13
                        FROM
                            {wind_database}.td_zord_cust_cntc_plc
                        WHERE
                            cntc_rel_cd = '01'
                            AND co_cl_cd IN (
                                'T'
                                ,'A'
                            )
                            AND cntc_plc_cl_cd IN (
                                '04'
                                ,'05'
                                ,'08'
                                ,'12'
                                ,'13'
                            )
                ) b
                    ON a.nm_cust_num = b.cust_num
            GROUP BY
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
    )
    ,q5 AS (
     SELECT
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,a.email04
                ,a.email05
                ,a.email08
                ,MAX (b.inv_email_addr) AS inv_email_addr
                ,MAX (b.inv_addr) AS inv_integ_txt_addr
            FROM
                q4 a LEFT JOIN (
                    SELECT
                            acnt_num
                            ,CASE
                                WHEN bill_isue_typ_cd IN (
                                    'I'
                                    ,'K'
                                    ,'A'
                                    ,'0'
                                    ,'G'
                                    ,'5'
                                    ,'E'
                                )
                                THEN inv_email_addr
                            END AS inv_email_addr
                            ,CASE
                                WHEN bill_isue_typ_cd IN (
                                    '1'
                                    ,'4'
                                    ,'C'
                                    ,'8'
                                )
                                THEN integ_txt_addr_id
                            END AS inv_addr
                        FROM
                            {wind_database}.td_zord_acnt
                        WHERE
                            co_cl_cd = 'T'
                            AND bill_isue_typ_cd IN (
                                'I'
                                ,'K'
                                ,'A'
                                ,'0'
                                ,'G'
                                ,'5'
                                ,'E'
                                ,'1'
                                ,'4'
                                ,'C'
                                ,'8'
                            )
                ) b
                    ON a.acnt_num = b.acnt_num
            GROUP BY
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,a.email04
                ,a.email05
                ,a.email08
    )
    ,q6 AS (
     SELECT
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,a.email04
                ,a.email05
                ,a.email08
                ,a.inv_email_addr
                ,a.inv_integ_txt_addr
                ,b.integ_txt_addr_id AS join_integ_txt_addr
            FROM
                q5 a LEFT JOIN (
                    SELECT
                            cust_num
                            ,integ_txt_addr_id
                        FROM
                            {wind_database}.td_zord_cust
                ) b
                    ON a.nm_cust_num = b.cust_num
    )
    ,q7 AS (
     SELECT
                cust_num
                ,CASE
                    WHEN addr_cd = '01'
                    THEN integ_txt_addr_id
                END AS etc_integ_txt_addr01
                ,CASE
                    WHEN addr_cd = '03'
                    THEN integ_txt_addr_id
                END AS etc_integ_txt_addr03
            FROM
                {wind_database}.td_zord_cust_addr
            WHERE
                co_cl_cd IN (
                    'T'
                    ,'A'
                )
                AND cntc_rel_cd = '01'
                AND addr_cd IN (
                    '01'
                    ,'03'
                )
    )
    ,q8 AS (
     SELECT
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,a.email04
                ,a.email05
                ,a.email08
                ,a.inv_email_addr
                ,a.inv_integ_txt_addr
                ,a.join_integ_txt_addr
                ,MAX (b.etc_integ_txt_addr01) AS etc_integ_txt_addr01
                ,MAX (b.etc_integ_txt_addr03) AS etc_integ_txt_addr03
            FROM
                q6 a LEFT JOIN q7 b
                    ON a.nm_cust_num = b.cust_num
            GROUP BY
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,a.email04
                ,a.email05
                ,a.email08
                ,a.inv_email_addr
                ,a.inv_integ_txt_addr
                ,a.join_integ_txt_addr
    )
    ,q9 AS (
     SELECT
                a.svc_mgmt_num
                ,a.estm_hcode
            FROM
                (
                    SELECT
                            svc_mgmt_num
                            ,weekday_night_hcode_cd AS estm_hcode
                            ,dt
                            ,MAX (dt) over (
                                partition BY svc_mgmt_num
                            ) AS last_dt
                        FROM
                            {location_database}.location_profile_wgs
                ) a
            WHERE
                dt = last_dt
    )
    ,q10 AS (
     SELECT
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,a.email04
                ,a.email05
                ,a.email08
                ,a.inv_email_addr
                ,a.inv_integ_txt_addr
                ,a.join_integ_txt_addr
                ,a.etc_integ_txt_addr01
                ,a.etc_integ_txt_addr03
                ,b.estm_hcode
            FROM
                q8 a LEFT JOIN q9 b
                    ON a.svc_mgmt_num = b.svc_mgmt_num
    )
    ,q11 AS (
     SELECT
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,b.eqp_mdl_cd
                ,b.eqp_ser_num
                ,a.email04
                ,a.email05
                ,a.email08
                ,a.inv_email_addr
                ,a.inv_integ_txt_addr
                ,a.join_integ_txt_addr
                ,a.etc_integ_txt_addr01
                ,a.etc_integ_txt_addr03
                ,a.estm_hcode
            FROM
                q10 a LEFT JOIN (
                    SELECT
                            svc_mgmt_num
                            ,eqp_mdl_cd
                            ,eqp_ser_num
                        FROM
                            {wind_database}.td_zord_wless_svc
                ) b
                    ON a.svc_mgmt_num = b.svc_mgmt_num
    )
    ,q12 AS (
     SELECT
                a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,b.eqp_mdl_cd
                ,b.eqp_ser_num
                ,b.mac_addr_val
                ,reflect (
                    'org.apache.commons.codec.digest.DigestUtils'
                    ,'sha256Hex'
                    ,mac_addr_val
                ) AS uuid
                ,a.email04
                ,a.email05
                ,a.email08
                ,a.inv_email_addr
                ,a.inv_integ_txt_addr
                ,a.join_integ_txt_addr
                ,a.etc_integ_txt_addr01
                ,a.etc_integ_txt_addr03
                ,a.estm_hcode
            FROM
                q11 a LEFT JOIN (
                    SELECT
                            eqp_mdl_cd
                            ,eqp_ser_num
                            ,mac_addr_val
                        FROM
                            {wind_database}.td_zeqp_eqp_orgl_book
                ) b
                    ON a.eqp_ser_num = b.eqp_ser_num
                AND a.eqp_mdl_cd = b.eqp_mdl_cd
    )
    ,q13 AS (
     SELECT
                a. *
                ,CASE
                    WHEN inv_integ_txt_addr IS NOT NULL
                    THEN '1' WHEN join_integ_txt_addr IS NOT NULL
                    THEN '2' WHEN estm_hcode IS NOT NULL
                    THEN '3' WHEN etc_integ_txt_addr01 IS NOT NULL
                    THEN '4' WHEN etc_integ_txt_addr03 IS NOT NULL
                    THEN '4'
                    ELSE '5'
                END AS top_addr_gubun
                ,COALESCE (
                    inv_integ_txt_addr
                    ,join_integ_txt_addr
                    ,etc_integ_txt_addr01
                    ,etc_integ_txt_addr03
                ) AS top_integ_txt_addr
            FROM
                q12 a
    )
    ,q14 AS (
        SELECT
                concat( '1' ,a.svc_mgmt_num ) AS personal_id
                ,b.key AS addr_id
                ,a.svc_cd
                ,a.svc_mgmt_num
                ,a.svc_num
                ,a.cntrct_mgmt_num
                ,a.nm_cust_num
                ,a.acnt_num
                ,a.eqp_mdl_cd
                ,a.eqp_ser_num
                ,a.mac_addr_val
                ,a.email04
                ,a.email05
                ,a.email08
                ,a.inv_email_addr
                ,a.inv_integ_txt_addr
                ,a.join_integ_txt_addr
                ,a.etc_integ_txt_addr01
                ,a.etc_integ_txt_addr03
                ,a.estm_hcode
            FROM
                q13 a LEFT JOIN {target_database}.cwip_td_zngm_integ_txt_addr_key b
                    ON a.top_integ_txt_addr = b.integ_txt_addr_id
    ) SELECT
            *
        FROM
            q14
    """
truncate_table_dbp_main = """
    truncate table dbp_main
  """
