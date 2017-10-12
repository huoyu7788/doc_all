#!/bin/sh
#############################################
#                                           
# Author: zhaoning
# Date: 2016-12-06
# Describe: 
#
#############################################
#source /home/inf/.bash_profile
DAGS_HOME=/home/datax/airflow/dags


if [ $1"0" == "0" ] 
then

dateago=`date -d "${date} 1 days ago" +"%Y%m%d"`
datepath=`date -d "${date} 1 days ago" +"%Y/%m/%d"`
day=`date -d "${date} 1 days ago" +"%Y-%m-%d"`

else

dateago=`date -d "$1 0 days ago" +"%Y%m%d"`

datepath=`date -d "$1 0 days ago" +"%Y/%m/%d"`

day=`date -d "$1 0 days ago" +"%Y-%m-%d"`

fi 

echo $datepath

echo $dateago 

# 数据 写入地址   
#datapath=/usr/dw/data/file_no_lkl/${datepath} 
datapath=/home/inf/airflow/dags/thirdpay/data/file_no_lkl/${datepath}

# 删除目录 
rm -rf ${datapath}

#创建目录 
mkdir -p ${datapath}



echo "标记检查开始 ：" `date +"%Y-%m-%d %H:%M:%S"`

CheckTag -d ${day} -l day  -b default.mds_fin_third_pay_no_lkl

echo "标记检查结束 ：" `date +"%Y-%m-%d %H:%M:%S"`


echo "开始计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"` 


sql="
select '北京' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       case when a.system = 'BR_BJ_BJ_SXF' then round(a.money_sxf_bj,2)
            when a.system = 'BR_BJ_TJ_SXF' then round(a.money_sxf_tj,2)
            when a.system = 'BR_BJ_HZ_SXF' then round(a.money_sxf_hz,2)
            when a.system = 'BR_BJ_YFZK' then round(a.money_yszk,2)
            when a.system = 'BR_BJ_TJ_NBWL' then round(a.money_nbwl_tj,2)
            when a.system = 'BR_BJ_HZ_NBWL' then round(a.money_nbwl_hz,2)
       end as money,
       b.dd_name as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
       (
        select pstng_date,
               flag,
               system,
               module,
               sum(money_sxf_bj) as money_sxf_bj,
               sum(money_sxf_tj) as money_sxf_tj,
               sum(money_sxf_hz) as money_sxf_hz,
               sum(money_yszk) as money_yszk,
               sum(money_nbwl_tj) as money_nbwl_tj,
               sum(money_nbwl_hz) as money_nbwl_hz
         from default.mds_fin_third_pay_no_lkl as a
        where dt = '$day'
        group by pstng_date,flag,system,module
       ) as a
       JOIN temp_finance_certificate_info_lm_zhaoning as b
        on (a.flag = b.flag)
union all
select '北京' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       round(a.money,2) as money,
       b.dd_name as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
       (
        select pstng_date,
               module,
               round(sum((money_nbwl_hz + money_nbwl_tj + money_yszk) - (money_sxf_bj + money_sxf_tj + money_sxf_hz)),2) as money
          from default.mds_fin_third_pay_no_lkl
         where dt = '$day'
         group by pstng_date,module
        ) as a
        join temp_finance_certificate_info_lm_zhaoning as b
        on (concat('BR_BJ_YHCK_',a.module) = b.flag)
union all
select '天津' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       round(a.money,2) as money,
       b.dd_name as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             sum(money_nbwl_tj) as money,
             module
        from default.mds_fin_third_pay_no_lkl
       where name = '天津'
         and dt = '$day'
       group by pstng_date,module
      ) as a
      join temp_finance_certificate_info_lm_zhaoning as b
      on (concat('BR_TJ_NBWL_',a.module) = b.flag)
  union all
select '天津' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       round(a.money,2) as money,
       b.dd_name as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             sum(money_nbwl_tj) as money,
             module
        from default.mds_fin_third_pay_no_lkl
       where name = '天津'
         and dt = '$day'
       group by pstng_date,module
      ) as a
      join temp_finance_certificate_info_lm_zhaoning as b
      on (concat('BR_TJ_YFZK_',a.module) = b.flag)
union all
select '杭州' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       round(a.money,2) as money,
       b.dd_name as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             sum(money_nbwl_hz) as money,
             module
        from default.mds_fin_third_pay_no_lkl
       where name = '杭州'
         and dt = '$day'
       group by pstng_date,module
      ) as a
      join temp_finance_certificate_info_lm_zhaoning as b
      on (concat('BR_HZ_NBWL_',a.module) = b.flag)
  union all
select '杭州' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       round(a.money,2) as money,
       b.dd_name as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             sum(money_nbwl_hz) as money,
             module
        from default.mds_fin_third_pay_no_lkl
       where name = '杭州'
         and dt = '$day'
       group by pstng_date,module
      ) as a
      join temp_finance_certificate_info_lm_zhaoning as b
      on (concat('BR_HZ_YFZK_',a.module) = b.flag)
"
 echo $sql

 hive -S -e "$sql" > ${datapath}/file_no_lkl$day 

if [ $? -ne 0 ] 
then
     SendMail -t zhaoning -o "data load is err " -u  "file_no_lkl.sh/default.mds_fin_third_pay_no_lkl CheckTag data is err"
     echo "执行失败:"$? 
   exit 255

else 

  echo "执行成功"

fi
echo "计算结束数据 ：" `date +"%Y-%m-%d %H:%M:%S"`

echo "写入数据库开始 ：" `date +"%Y-%m-%d %H:%M:%S"`
mysql -h192.168.60.49 -uroot -proot123 -P3332 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
delete from dt_db.ofc_account_document where dctype in ('BR1','BR2','BR3','BR4','BR5','BR6','BR7','BR8','BR9','BR10','BR11','BR12') and status =1 and pstng_date = '$day';
"



mysql -h192.168.60.49 -uroot -proot123 -P3332 --local-infile=1 -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '${datapath}/file_no_lkl$day' REPLACE INTO TABLE dt_db.ofc_account_document(name,waybill_no,tax_rate,dctype,cd_type,doc_number,dd,money,text,pstng_date,customer,status,created_at,updated_at)
" 

if [ $? -ne 0 ]
then
     echo "数据库写入失败:"$?
   exit 255

else

  echo "数据库写入成功"

fi
echo "写入数据库完成 ：" `date +"%Y-%m-%d %H:%M:%S"`


