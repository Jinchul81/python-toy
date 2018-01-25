# -*- coding: utf-8 -*-

import abc


class AbstractSet:
    __metaclass__ = abc.ABCMeta

    @staticmethod
    @abc.abstractmethod
    def add_partition():
        pass

    @staticmethod
    @abc.abstractmethod
    def insert():
        pass

    @staticmethod
    @abc.abstractmethod
    def create():
        pass


class P1PaySet(AbstractSet):
    @staticmethod
    def add_partition(dt):
        return "ALTER TABLE PAY.P1_PAY_SET ADD IF NOT EXISTS PARTITION (strd_dt = '%s')" \
               % dt.strftime("%Y%m%d")

    @staticmethod
    def insert(dt):
        return '''
INSERT OVERWRITE TABLE PAY.P1_PAY_SET PARTITION(strd_dt='%(dt)s')
SELECT  T1.pay_hr
, T1.svc_mgmt_num
, T1.pay_src_cl
, T1.pay_brand
, T2.pay_id
, T1.pay_prod_shop_cd
, T1.pay_prod_shop_nm
, CAST(REGEXP_REPLACE(T1.pay_amt,',','') AS INT )
FROM
(
SELECT REGEXP_REPLACE(use_dt ,"-", "") AS strd_dt
, SUBSTR(use_tms, 1, 2) AS pay_hr
, svc_mgmt_num AS svc_mgmt_num
, 'TPAY' AS pay_src_cl
, use_sto_typ AS pay_brand
, use_sto_cd AS pay_prod_shop_cd
, use_sto_nm AS pay_prod_shop_nm
, use_amt AS pay_amt
FROM   TPAY.TPAY_USE
WHERE  stat_typ = '정상'
AND  use_dt = '%(dashed_dt)s'

UNION ALL

SELECT REGEXP_REPLACE(use_dt, '-', '') AS strd_dt
, SUBSTR(use_tms, 1, 2) AS pay_hr
, svc_mgmt_num AS svc_mgmt_num
, 'TMONEY' AS pay_src_cl
, CASE WHEN sto_poi_nm IN ('택시','버스','지하철') THEN sto_poi_nm
WHEN sto_poi_nm LIKE 'GS25%%'            THEN 'GS25'
WHEN sto_poi_nm LIKE 'CU%%'              THEN 'CU'
WHEN sto_poi_nm LIKE '홈플러스%%'          THEN '홈플러스'
WHEN sto_poi_nm LIKE '위드미%%'            THEN '위드미'
WHEN sto_poi_nm LIKE '미니스톱%%'          THEN '미니스톱'
WHEN sto_poi_nm LIKE '바이더웨이%%'         THEN '바이더웨이'
WHEN sto_poi_nm LIKE '파리바게뜨%%'         THEN '파리바게뜨'
WHEN sto_poi_nm LIKE '스토리웨이%%'         THEN '스토리웨이'
WHEN sto_poi_nm LIKE '스타벅스%%'          THEN '스타벅스'
WHEN sto_poi_nm LIKE '세븐일레븐%%'         THEN '세븐일레븐'
ELSE '#' END AS pay_brand
, '#' AS pay_prod_shop_cd
, sto_poi_nm AS pay_prod_shop_nm
, use_amt AS pay_amt
FROM   TMONEY.TMONEY_USE
WHERE  stat_typ = '정상'
AND  use_dt = '%(dashed_dt)s'

UNION ALL

SELECT A.deal_dt  AS strd_dt
, SUBSTR(A.DEAL_TM, 1, 2) AS pay_hr
, A.svc_mgmt_num  AS svc_mgmt_num
, 'MEMBERSHIP' AS pay_src_cl
, B.org_cd AS pay_brand
, A.join_co_org_id AS pay_prod_shop_cd
, '#'  AS pay_prod_shop_nm
, A.join_co_aprv_amt AS pay_amt
FROM   WIND.TD_ZPRM_JOIN_CO_SETTL_HST AS A
LEFT OUTER JOIN WIND.TD_ZNGM_ORG AS B
ON  A.join_co_org_id = B.org_id
WHERE  A.cncl_rsn_cd           = '00'
AND  A.join_co_aprv_amt != '0'
AND  A.deal_dt = '%(dt)s'

UNION ALL

SELECT A.dt  AS strd_dt
, SUBSTR(A.time, 1, 2)  AS pay_hr
, B.svc_mgmt_num AS svc_mgmt_num
, 'GGL' AS pay_src_cl
, '구글소액결제' AS pay_brand
, A.pid AS pay_prod_shop_cd
, C.pname AS pay_prod_shop_nm
, A.amount AS pay_amt
FROM   GGL_DCB.USAGE AS A
LEFT JOIN 
( SELECT svc_mgmt_num, MAX(svc_num) as svc_num 
FROM WIND.TD_ZORD_SVC_NUM_RSC 
-- WHERE dt = '%(dt)s' 
GROUP BY svc_mgmt_num
) AS B
ON A.mdn = B.svc_num
LEFT JOIN
( SELECT pid
, MAX(pname) AS pname
FROM   GGL_DCB.PRODUCT_INFO
GROUP BY pid
) AS C
ON A.pid = C.pid
WHERE  A.dt = '%(dt)s'
) T1
, PAY.META_PAY T2
WHERE  T1.pay_brand = T2.pay_brand
''' % {'dt': dt.strftime("%Y%m%d")
            , 'dashed_dt': dt.strftime("%Y-%m-%d")}

    @staticmethod
    def create():
        return '''
CREATE TABLE IF NOT EXISTS
  PAY.P1_PAY_SET
  (
    pay_hr INT,
    svc_mgmt_num VARCHAR(10),
    pay_src_cl VARCHAR(20),
    pay_brand VARCHAR(50),
    pay_id VARCHAR(20),
    pay_prod_shop_cd VARCHAR(50),
    pay_prod_shop_nm VARCHAR(50),
    pay_amt INT    
  )
PARTITIONED BY (strd_dt VARCHAR(8))
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
      WITH SERDEPROPERTIES("serialization.encoding"='UTF-8')
  STORED AS ORC
  LOCATION '/dataset/pay/p1_pay_set' 
'''


class P2PaySetDaily(AbstractSet):
    @staticmethod
    def add_partition(dt):
        return "ALTER TABLE PAY.P2_PAY_SET_DAILY ADD IF NOT EXISTS PARTITION (strd_dt = '%s')" \
               % dt.strftime("%Y%m%d")

    @staticmethod
    def insert(dt):
        return '''
INSERT OVERWRITE TABLE PAY.P2_PAY_SET_DAILY PARTITION(strd_dt='%(dt)s')
SELECT
  T2.pf_id 
, T2.pf_lvl_full_nm
, T1.svc_mgmt_num
, COUNT(*)
, SUM(T1.pay_amt)
, SUM(CASE WHEN T1.pay_hr BETWEEN 0 AND 3 THEN 1 ELSE 0 END)
, SUM(CASE WHEN T1.pay_hr BETWEEN 0 AND 3 THEN T1.pay_amt ELSE 0 END)
, SUM(CASE WHEN T1.pay_hr BETWEEN 4 AND 9 THEN 1 ELSE 0 END)
, SUM(CASE WHEN T1.pay_hr BETWEEN 4 AND 9 THEN T1.pay_amt ELSE 0 END)
, SUM(CASE WHEN T1.pay_hr BETWEEN 10 AND 15 THEN 1 ELSE 0 END)
, SUM(CASE WHEN T1.pay_hr BETWEEN 10 AND 15 THEN T1.pay_amt ELSE 0 END)
, SUM(CASE WHEN T1.pay_hr BETWEEN 16 AND 21 THEN 1 ELSE 0 END)
, SUM(CASE WHEN T1.pay_hr BETWEEN 16 AND 21 THEN T1.pay_amt ELSE 0 END)
, SUM(CASE WHEN T1.pay_hr BETWEEN 22 AND 24 THEN 1 ELSE 0 END)
, SUM(CASE WHEN T1.pay_hr BETWEEN 22 AND 24 THEN T1.pay_amt ELSE 0 END)
, MAX(CASE WHEN from_unixtime(unix_timestamp(T1.strd_dt), 'u') < 6 THEN 'Y' ELSE 'N' END) -- WEEKDAY 
FROM  PAY.P1_PAY_SET AS T1
    , PAY.META_PROFILE_ALL_MAPP AS T2
WHERE T1.pay_id = T2.mp_id
AND   T1.strd_dt = '%(dt)s'
GROUP BY
  strd_dt
, pf_id
, pf_lvl_full_nm
, svc_mgmt_num
''' % {'dt': dt.strftime("%Y%m%d")}


    @staticmethod
    def create():
        return '''
CREATE TABLE IF NOT EXISTS 
  PAY.P2_PAY_SET_DAILY
  (
    pf_id VARCHAR(20),
    pf_lvl_full_nm VARCHAR(200),
    svc_mgmt_num VARCHAR(10),
    day_pay_cnt INT,
    day_pay_amt INT,
    hr0004_pay_cnt INT,
    hr0004_pay_amt INT,
    hr0410_pay_cnt INT,
    hr0410_pay_amt INT,
    hr1016_pay_cnt INT,
    hr1016_pay_amt INT,
    hr1622_pay_cnt INT,
    hr1622_pay_amt INT,
    hr2224_pay_cnt INT,
    hr2224_pay_amt INT,
    wkday_yn VARCHAR(1)
  )
  PARTITIONED BY (strd_dt VARCHAR(8))
  STORED AS ORC
  LOCATION '/dataset/pay/p2_pay_set_daily' '''


class P2PaySetMonthly(AbstractSet):
    @staticmethod
    def add_partition(dt):
        return "ALTER TABLE PAY.P2_PAY_SET_MONTHLY ADD IF NOT EXISTS PARTITION (strd_ym = '%s')" \
               % dt.strftime("%Y%m")

    @staticmethod
    def insert(dt):
        return '''
INSERT OVERWRITE TABLE PAY.P2_PAY_SET_MONTHLY PARTITION(strd_ym='%(ym)s')
SELECT  
  pf_id                  
, pf_lvl_full_nm         
, svc_mgmt_num           
, SUM(day_pay_cnt)  
, SUM(day_pay_amt)   
, SUM(hr0004_pay_cnt)
, SUM(hr0004_pay_amt)
, SUM(hr0410_pay_cnt)
, SUM(hr0410_pay_amt)
, SUM(hr1016_pay_cnt)
, SUM(hr1016_pay_amt)
, SUM(hr1622_pay_cnt)
, SUM(hr1622_pay_amt)
, SUM(hr2224_pay_cnt)
, SUM(hr2224_pay_amt)
, SUM(CASE WHEN wkday_yn IN('Y') THEN day_pay_cnt ELSE 0 END)
, SUM(CASE WHEN wkday_yn IN('Y') THEN day_pay_amt ELSE 0 END)
, SUM(CASE WHEN wkday_yn IN('N') THEN day_pay_cnt ELSE 0 END)
, SUM(CASE WHEN wkday_yn IN('N') THEN day_pay_amt ELSE 0 END)
FROM
  PAY.P2_PAY_SET_DAILY
WHERE
  strd_dt LIKE '%(ym)s%%'
GROUP BY
  pf_id
, pf_lvl_full_nm 
, svc_mgmt_num
, SUBSTR(strd_dt , 1, 6) ''' % {'ym': dt.strftime("%Y%m")}

    @staticmethod
    def create():
        return '''
CREATE TABLE IF NOT EXISTS
   PAY.P2_PAY_SET_MONTHLY 
(
    pf_id VARCHAR(20),
    pf_lvl_full_nm VARCHAR(200),
    svc_mgmt_num VARCHAR(10),
    tot_pay_cnt INT,
    tot_pay_amt INT,
    hr0004_pay_cnt INT,
    hr0004_pay_amt INT,
    hr0410_pay_cnt INT,
    hr0410_pay_amt INT,
    hr1016_pay_cnt INT,
    hr1016_pay_amt INT,
    hr1622_pay_amt INT,
    hr1622_pay_cnt INT,
    hr2224_pay_cnt INT,
    hr2224_pay_amt INT,
    wkday_pay_cnt INT,
    wkday_pay_amt INT,
    wkend_pay_cnt INT,
    wkend_pay_amt INT
  )
  PARTITIONED BY (strd_ym VARCHAR(8))
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
      WITH SERDEPROPERTIES("serialization.encoding"='UTF-8')
  STORED AS ORC
  LOCATION '/dataset/pay/p2_pay_set_monthly' '''
