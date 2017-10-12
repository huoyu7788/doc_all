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


echo "开始计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"` 


sql="
insert overwrite table default.mds_fin_third_pay_base partition(dt='$day')
select regexp_replace(alpay_trans_id,'\\\s+','') as transaction_id,
       'ZFB' as module,
       money as money,
       abs(counter_fee) as counter_fee,
       round((abs(counter_fee)/money)*100,2) as rate,
       regexp_replace(create_time,'\\\s+','') as transaction_time,
       concat(trim(dt),' ','00:00:00') as lkl_trans_time
  from ods.ods_fin_third_pay_zfb 
 where dt = '$day'
   and money is not null
union all
select regexp_replace(trim(trans_id),'\`','') as transaction_id,
       'WX_APP' as module,
       cast(regexp_replace(trim(money),'\`','') as float) as money,
       cast(regexp_replace(trim(counter_fee),'\`','') as float) as counter_fee,
       cast(regexp_replace(regexp_replace(trim(rate),'\`',''),'%','') as float) as rate,
       regexp_replace(trim(trans_time),'\`','') as transaction_time,
       concat(trim(dt),' ','00:00:00') as lkl_trans_time
  from ods.ods_fin_third_pay_wxapp
 where dt = '$day'
   and money is not null
   and money <> '总金额'
union all
select regexp_replace(trim(trans_id),'\`','') as transaction_id,
       'WX_GZH' as module,
       cast(regexp_replace(trim(money),'\`','') as float) as money,
       cast(regexp_replace(trim(counter_fee),'\`','') as float) as counter_fee,
       cast(regexp_replace(regexp_replace(trim(rate),'\`',''),'%','') as float) as rate,
       regexp_replace(trim(trans_time),'\`','') as transaction_time,
       concat(trim(dt),' ','00:00:00') as lkl_trans_time
  from ods.ods_fin_third_pay_wxgzh
 where dt = '$day'
   and money is not null
   and money <> '总金额'
union all
select trim(qf_id) as transaction_id,
       'WX_SM' as module,
       money/100 as money,
       round((money*0.38)/10000,2) as counter_fee,
       '0.38' as rate,
       trim(system_trans_time) as transaction_time,
       concat(trim(dt),' ','00:00:00') as lkl_trans_time
  from ods.ods_fin_third_pay_wxsm
 where dt = '$day'
   and money is not null
union all
select trim(qf_id) as transaction_id,
       'ZFB_SM' as module,
       money/100 as money,
       round((money*0.38)/10000,2) as counter_fee,
       '0.38' as rate,
       trim(system_trans_time) as transaction_time,
       concat(trim(dt),' ','00:00:00') as lkl_trans_time
  from ods.ods_fin_third_pay_alism
 where dt = '$day'
   and money is not null
union all
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
 where dt = '$day'

"
#insert overwrite local directory '${datapath}'
 echo $sql

 /usr/bin/hive -S -e "$sql"

if [ $? -ne 0 ] 
then
     
     echo "执行失败:"$? 
   exit 255

else 

  echo "执行成功"

fi

echo "开始计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"`

SetTagStatus -d $day -l day -b default.mds_fin_third_pay_base -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

SetTagStatus -d $day -l day -b default.mds_fin_third_pay_base -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D


if [ $? -ne 0 ]
then

  echo "标记失败:"$?
#SendMessage -i 13720026387,18910320926 -o " data load err  " -a "msql_mds_vrm_vender_main_pay"
SendMail -t zhaoning -o "data load mysql is err " -u  "mds_fin_third_pay_base.sh data is err" 
  exit 255

else

    echo "标记成功"

fi
echo "结束计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"`
