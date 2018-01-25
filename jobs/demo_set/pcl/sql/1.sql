
drop table if exists src.td_zord_cust_annv_lst;

CREATE EXTERNAL TABLE `src.td_zord_cust_annv_lst`(
  `cust_num` string,
  `co_cl_cd` string,
  `annv_cl_cd` string,
  `annv_dt` string,
  `audit_id` string,
  `audit_dtm` string,
  `annv_slnl_cd` string,
  `oper_dt_hms` string,
  `del_flag` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0002'
;

INSERT INTO src.td_zord_cust_annv_lst
(cust_num,annv_cl_cd,annv_dt,annv_slnl_cd) VALUES
('111','01','20160101','1'),
('111','03','20150910','1'),
('222','11','20140101','2'),
('333','03','20111111','2');
