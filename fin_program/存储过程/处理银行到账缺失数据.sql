hive -S -e " load data local inpath '/home/inf/thirdpay/bill/lklpay/201709/12/lkl_bill_20170912.txt' overwrite into table ods.ods_fin_third_pay_lkl partition (dt='2017-09-12')




--补充拉卡拉
hive -e "
insert overwrite table default.mds_fin_third_pay_base partition(dt='2017-09-12')
select trim(trans_id) as transaction_id,
       'LKL' as module,
       cast(money as float)/100 as money,
       cast(counter_fee as float)/100 as counter_fee,
       case when trim(card_type) = '借记卡' then '0.35'
            when trim(card_type) = '贷记卡' then '0.45'
            end as rate,
       concat(substr(trim(trans_date),1,4),'-',substr(trim(trans_date),5,2),'-',substr(trim(trans_date),7,2),' ',trim(trans_time)) as transaction_time,
       concat(trim(dt),' ','00:00:00') as lkl_trans_time
  from ods.ods_fin_third_pay_lkl
 where dt = '2017-09-12'
 "


--写数据
 hive -e "
select name,waybill_no,tax_rate,dctype,cd_type,doc_number,
       dd,project,project_cate,money,text,pstng_date,customer,'' as vendor,
       status,unix_timestamp() created_at,unix_timestamp() updated_at
  from mds_fin_u8_third_pay_result 
 where dt = '2017-09-12'
" > /home/inf/zhaoning/u8/data/201709/br_lkl_0912.txt


mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '/home/inf/zhaoning/u8/data/201709/br_lkl_0912.txt' INTO TABLE lsh_vrm.fin_certificate_bank_result(name,waybill_no,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,status,created_at,updated_at)
"

mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '/home/inf/zhaoning/u8/data/201709/br_lkl_0912.txt' REPLACE INTO TABLE lsh_vrm.fin_certificate_result(name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
"






