drop table ods_fin_third_pay_zt;
CREATE TABLE `ods_fin_third_pay_zt`(
  `trans_time` string, 
  `public_id` string, 
  `buiness_no` string, 
  `trans_id` string, 
  `buiness_push_no` string, 
  `trans_type` string, 
  `pay_type` string, 
  `money` string, 
  `final_money` string, 
  `counter_fee` string,
  `response_no` string,
  `response_info` string
  )
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/ods.db/ods_fin_third_pay_zt';




drop table ods_fin_third_pay_alism;
CREATE TABLE `ods_fin_third_pay_alism`(
  `pay_type` int,
  `system_trans_time` string,
  `order_type` string,
  `currency` string,
  `request_trans_time` string,
  `money` float,
  `trans_id` string,
  `qf_id` string,
  `revoke_point` string,
  `trans_result` string,
  `text` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/ods.db/ods_fin_third_pay_alism';

drop table ods_fin_third_pay_lkl;
CREATE EXTERNAL TABLE `ods_fin_third_pay_lkl`(
  `buiness_no` string,
  `trans_date` string,
  `trans_time` string,
  `settlement_time` string,
  `terminal_no` string,
  `dot_name` string,
  `type` string,
  `pos_no` string,
  `trans_card` string,
  `card_type` string,
  `card_name` string,
  `money` string,
  `counter_fee` string,
  `settlement_money` string,
  `order_id` string,
  `trans_id` string,
  `trans_status` string,
  `text` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/ods.db/ods_fin_third_pay_lkl';


drop table ods_fin_third_pay_wxapp;

CREATE TABLE `ods_fin_third_pay_wxapp`(
  `trans_time` string,
  `public_id` string,
  `buiness_no` string,
  `sub_buiness_no` string,
  `equip_no` string,
  `trans_id` string,
  `buiness_order_id` string,
  `user_mark` string,
  `type` string,
  `trans_status` string,
  `bank` string,
  `currency` string,
  `money` string,
  `red_money` string,
  `sku_name` string,
  `buiness_data_pac` string,
  `counter_fee` string,
  `rate` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/ods.db/ods_fin_third_pay_wxapp';


drop table ods_fin_third_pay_wxgzh;

CREATE TABLE `ods_fin_third_pay_wxgzh`(
  `trans_time` string,
  `public_id` string,
  `buiness_no` string,
  `sub_buiness_no` string,
  `equip_no` string,
  `trans_id` string,
  `buiness_order_id` string,
  `user_mark` string,
  `type` string,
  `trans_status` string,
  `bank` string,
  `currency` string,
  `money` string,
  `red_money` string,
  `sku_name` string,
  `buiness_data_pac` string,
  `counter_fee` string,
  `rate` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/ods.db/ods_fin_third_pay_wxgzh';

drop table ods_fin_third_pay_wxsm;

CREATE TABLE `ods_fin_third_pay_wxsm`(
  `pay_type` int,
  `system_trans_time` string,
  `order_type` string,
  `currency` string,
  `request_trans_time` string,
  `money` float,
  `trans_id` string,
  `qf_id` string,
  `revoke_point` string,
  `trans_result` string,
  `text` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/ods.db/ods_fin_third_pay_wxsm';

drop table ods_fin_third_pay_zfb;

CREATE TABLE `ods_fin_third_pay_zfb`(
  `alpay_trans_id` string,
  `order_id` string,
  `type` string,
  `sku_name` string,
  `create_time` string,
  `end_time` string,
  `store_code` string,
  `store_name` string,
  `operator` string,
  `terminal_no` string,
  `each_acount` string,
  `money` float,
  `actual_money` float,
  `red_money` float,
  `integral_money` float,
  `alpay_dis_money` float,
  `buiness_dis_money` float,
  `voucher_money` float,
  `voucher_name` string,
  `buiness_consu_money` float,
  `card_consu_money` float,
  `revoke_point` string,
  `counter_fee` float,
  `fr` float,
  `text` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/ods.db/ods_fin_third_pay_zfb';

drop table mds_fin_third_pay_bug;
drop table mds_fin_u8_third_pay_dimension;
drop table mds_fin_u8_third_pay_lkl;
drop table mds_fin_u8_third_pay_lkl_bug;
drop table mds_fin_u8_third_pay_no_lkl;
drop table mds_fin_u8_third_pay_no_lkl_bug;
drop table mds_fin_u8_third_pay_result;
drop table mds_fin_u8_third_pay_result_bug;


drop table mds_fin_third_pay;

CREATE EXTERNAL TABLE `mds_fin_third_pay`(
  `name` string COMMENT '',
  `order_id` bigint COMMENT '',
  `receipt_order_id` bigint COMMENT '',
  `waybill_no` string COMMENT '',
  `module` string COMMENT '',
  `money` float COMMENT '',
  `count_fee` float COMMENT '',
  `rate` float COMMENT '',
  `transaction_time` string COMMENT '',
  `lkl_trans_time` string COMMENT '',
  `serial_no` string COMMENT '',
  `trade_id` string COMMENT '',
  `compare_sign` string COMMENT '')
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_third_pay';

drop table mds_fin_third_pay_base;

CREATE EXTERNAL TABLE `mds_fin_third_pay_base`(
  `transaction_id` string COMMENT '',
  `module` string COMMENT '',
  `money` float COMMENT '',
  `count_fee` float COMMENT '',
  `rate` float COMMENT '',
  `transaction_time` string COMMENT '',
  `lkl_trans_time` string COMMENT '')
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_third_pay_base';

drop table mds_fin_third_pay_bug;

CREATE TABLE `mds_fin_third_pay_bug`(
  `name` string,
  `order_id` bigint,
  `receipt_order_id` bigint,
  `waybill_no` string,
  `module` string,
  `money` float,
  `count_fee` float,
  `rate` float,
  `transaction_time` string,
  `lkl_trans_time` string,
  `serial_no` string,
  `trade_id` string,
  `compare_sign` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\t',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_third_pay_bug';

drop table mds_fin_u8_third_pay_dimension;

CREATE EXTERNAL TABLE `mds_fin_u8_third_pay_dimension`(
  `name` string,
  `doc_type` string,
  `dctype` string,
  `gl_account` string,
  `dd_name` string,
  `dd` string,
  `customer` string,
  `project` string,
  `project_cate` string,
  `pn` int,
  `alloc_nmbr` string,
  `tax_rate` string,
  `amtdoccur` string,
  `item_text` string,
  `system` string,
  `area` string,
  `flag` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\t',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_u8_third_pay_dimension';

drop table mds_fin_u8_third_pay_lkl;

CREATE EXTERNAL TABLE `mds_fin_u8_third_pay_lkl`(
  `name` string,
  `money_sxf_bj_lkl` float,
  `money_sxf_tj_lkl` float,
  `money_sxf_hz_lkl` float,
  `money_yszk_bj` float,
  `money_yszk_tj` float,
  `money_yszk_hz` float,
  `money_yszk_cys_bj` float,
  `money_yszk_cys_tj` float,
  `money_yszk_cys_hz` float,
  `money_nbwl_cys_tj` float,
  `money_nbwl_cys_hz` float,
  `system` string,
  `cy_code_q` string,
  `flag` string,
  `module` string,
  `pstng_date` string,
  `lkl_date` string,
  `waybill_no` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_u8_third_pay_lkl';

drop table mds_fin_u8_third_pay_lkl_bug;

CREATE EXTERNAL TABLE `mds_fin_u8_third_pay_lkl_bug`(
  `name` string,
  `money_sxf_bj_lkl` float,
  `money_sxf_tj_lkl` float,
  `money_sxf_hz_lkl` float,
  `money_yszk_bj` float,
  `money_yszk_tj` float,
  `money_yszk_hz` float,
  `money_yszk_cys_bj` float,
  `money_yszk_cys_tj` float,
  `money_yszk_cys_hz` float,
  `money_nbwl_cys_tj` float,
  `money_nbwl_cys_hz` float,
  `system` string,
  `cy_code_q` string,
  `flag` string,
  `module` string,
  `pstng_date` string,
  `lkl_date` string,
  `waybill_no` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_u8_third_pay_lkl_bug';

drop table mds_fin_u8_third_pay_no_lkl;

CREATE EXTERNAL TABLE `mds_fin_u8_third_pay_no_lkl`(
  `name` string,
  `money_sxf_bj` float,
  `money_sxf_tj` float,
  `money_sxf_hz` float,
  `money_yszk` float,
  `money_nbwl_tj` float,
  `money_nbwl_hz` float,
  `system` string,
  `flag` string,
  `module` string,
  `pstng_date` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_u8_third_pay_no_lkl';

drop table mds_fin_u8_third_pay_no_lkl_bug;

CREATE EXTERNAL TABLE `mds_fin_u8_third_pay_no_lkl_bug`(
  `name` string,
  `money_sxf_bj` float,
  `money_sxf_tj` float,
  `money_sxf_hz` float,
  `money_yszk` float,
  `money_nbwl_tj` float,
  `money_nbwl_hz` float,
  `system` string,
  `flag` string,
  `module` string,
  `pstng_date` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_u8_third_pay_no_lkl_bug';

drop table mds_fin_u8_third_pay_result;

CREATE TABLE `mds_fin_u8_third_pay_result`(
  `name` string,
  `waybill_no` string,
  `tax_rate` string,
  `dctype` string,
  `cd_type` string,
  `doc_number` string,
  `dd` string,
  `project` string,
  `project_cate` string,
  `money` string,
  `text` string,
  `pstng_date` string,
  `customer` string,
  `status` int,
  `created_at` int,
  `updated_at` int)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_u8_third_pay_result';

drop table mds_fin_u8_third_pay_result_bug;

CREATE TABLE `mds_fin_u8_third_pay_result_bug`(
  `name` string,
  `waybill_no` string,
  `tax_rate` string,
  `dctype` string,
  `cd_type` string,
  `doc_number` string,
  `dd` string,
  `project` string,
  `project_cate` string,
  `money` string,
  `text` string,
  `pstng_date` string,
  `customer` string,
  `status` int,
  `created_at` int,
  `updated_at` int)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/mds_fin_u8_third_pay_result_bug';