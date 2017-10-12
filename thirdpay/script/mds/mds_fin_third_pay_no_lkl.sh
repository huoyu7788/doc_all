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


echo "标记检查开始 ：" `date +"%Y-%m-%d %H:%M:%S"`

CheckTag -d ${day} -l day  -b default.mds_fin_third_pay

if [ $? -ne 0 ]
then

     SendMail -t zhaoning -o "data load is err " -u  "mds_fin_third_pay_no_lkl.sh CheckTag is err"

     echo "执行失败:"$?

     exit 255

else

  echo "执行成功"

fi

echo "标记检查结束 ：" `date +"%Y-%m-%d %H:%M:%S"`


echo "开始计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"` 


sql="
insert overwrite table mds_fin_u8_third_pay_no_lkl partition(dt='$day')
  --北京手续费
  select '北京' as name,
         round(money,2) as money_sxf_bj,
         0 as money_sxf_tj,
         0 as money_sxf_hz,
         0 as money_yszk,
         0 as money_nbwl_tj,
         0 as money_nbwl_hz,
         'BR_BJ_BJ_SXF' as system,
         b.flag as flag,
         a.module as module,
         a.psting_date as pstng_date
    from
        (
        select module,
               sum(count_fee) as money,
               substr(transaction_time,1,10) as psting_date
          from mds_fin_third_pay
         where name = '北京'
           -- and module not like 'LKL%'
           and module in ('WX_APP','WX_GZH','WX_SM','ZFB','ZFB_SM') 
           and dt = '$day'
         group by substr(transaction_time,1,10),module
        ) as a
          join
        default.mds_fin_u8_third_pay_dimension as b
        on (concat('BR_BJ_BJ_SXF_',a.module) = b.flag)
  union all
  --手续费天津
  select '天津' as name,
         0 as money_sxf_bj,
         round(money,2) as money_sxf_tj,
         0 as money_sxf_hz,
         0 as money_yszk,
         0 as money_nbwl_tj,
         0 as money_nbwl_hz,
         'BR_BJ_TJ_SXF' as system,
         b.flag as flag,
         a.module as module,
         a.psting_date as pstng_date
    from
        (
        select module,
               sum(count_fee) as money,
               substr(transaction_time,1,10) as psting_date
          from mds_fin_third_pay
         where name = '天津'
           -- and module not like 'LKL%'
           and module in ('WX_APP','WX_GZH','WX_SM','ZFB','ZFB_SM')
           and dt = '$day'
         group by substr(transaction_time,1,10),module
        ) as a
          join
        default.mds_fin_u8_third_pay_dimension as b
        on (concat('BR_BJ_TJ_SXF_',a.module) = b.flag)
  union all
  --手续费杭州
  select '杭州' as name,
         0 as money_sxf_bj,
         0 as money_sxf_tj,
         round(money,2) as money_sxf_hz,
         0 as money_yszk,
         0 as money_nbwl_tj,
         0 as money_nbwl_hz,
         'BR_BJ_HZ_SXF' as system,
         b.flag as flag,
         a.module as module,
         a.psting_date as pstng_date
    from
        (
        select module,
               sum(count_fee) as money,
               substr(transaction_time,1,10) as psting_date
          from mds_fin_third_pay
         where name = '杭州'
           -- and module not like 'LKL%'
           and module in ('WX_APP','WX_GZH','WX_SM','ZFB','ZFB_SM')
           and dt = '$day'
         group by substr(transaction_time,1,10),module
        ) as a
          join
        default.mds_fin_u8_third_pay_dimension as b
        on (concat('BR_BJ_HZ_SXF_',a.module) = b.flag)
  union all
  --应收账款
  select '北京' as name,
         0 as money_sxf_bj,
         0 as money_sxf_tj,
         0 as money_sxf_hz,
         round(a.money,2) as money_yszk,
         0 as money_nbwl_tj,
         0 as money_nbwl_hz,
         'BR_BJ_YFZK' as system,
         b.flag as flag,
         a.module as module,
         a.psting_date as pstng_date
    from
        (
        select module,
               sum(money) as money,
               substr(transaction_time,1,10) as psting_date
          from mds_fin_third_pay
         where name = '北京'
           -- and module not like 'LKL%'
           and module in ('WX_APP','WX_GZH','WX_SM','ZFB','ZFB_SM')
           and dt = '$day'
         group by substr(transaction_time,1,10),module
        ) as a
          join
        default.mds_fin_u8_third_pay_dimension as b
        on (concat('BR_BJ_YFZK_',a.module) = b.flag)

  union all
  --内部往来 天津
  select '天津' as name,
         0 as money_sxf_bj,
         0 as money_sxf_tj,
         0 as money_sxf_hz,
         0 as money_yszk,
         round(a.money,2)  as money_nbwl_tj,
         0  as money_nbwl_hz,
         'BR_BJ_TJ_NBWL' as system,
         b.flag as flag,
         a.module as module,
         a.psting_date as pstng_date
    from
        (
        select module,
               sum(money) as money,
               substr(transaction_time,1,10) as psting_date
          from mds_fin_third_pay
         where name = '天津'
           -- and module not like 'LKL%'
           and module in ('WX_APP','WX_GZH','WX_SM','ZFB','ZFB_SM')
           and dt = '$day'
         group by substr(transaction_time,1,10),module
        ) as a
          join
        default.mds_fin_u8_third_pay_dimension as b
        on (concat('BR_BJ_TJ_NBWL_',a.module) = b.flag)

  union all
  --内部往来 杭州
  select '杭州' as name,
         0 as money_sxf_bj,
         0 as money_sxf_tj,
         0 as money_sxf_hz,
         0 as money_yszk,
         0 as money_nbwl_tj,
         round(a.money,2) as money_nbwl_hz,
         'BR_BJ_HZ_NBWL' as system,
         b.flag as flag,
         a.module as module,
         a.psting_date as pstng_date
    from
        (
        select module,
               sum(money) as money,
               substr(transaction_time,1,10) as psting_date
          from mds_fin_third_pay
         where name = '杭州'
           -- and module not like 'LKL%'
           and module in ('WX_APP','WX_GZH','WX_SM','ZFB','ZFB_SM')
           and dt = '$day'
         group by substr(transaction_time,1,10),module
        ) as a
          join
        default.mds_fin_u8_third_pay_dimension as b
        on (concat('BR_BJ_HZ_NBWL_',a.module) = b.flag)
"

 echo $sql

 /usr/bin/hive -S -e "$sql"

if [ $? -ne 0 ] 
then
  
     SendMail -t zhaoning -o "data load is err " -u  "mds_fin_third_pay_no_lkl.sh data load is err" 
  
     echo "执行失败:"$? 
   
     exit 255

else 

  echo "执行成功"

fi

echo "开始标记 ：" `date +"%Y-%m-%d %H:%M:%S"`

#删除标记

SetTagStatus -d $day -l day -b default.mds_fin_third_pay_no_lkl -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

#创建标记

SetTagStatus -d $day -l day -b default.mds_fin_third_pay_no_lkl -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D


if [ $? -ne 0 ]
then

  echo "标记失败:"$?

  SendMail -t zhaoning -o "data load is err " -u  "mds_fin_third_pay_no_lkl.sh/SetTagStatus is err" 

  exit 255

else

    echo "标记成功"

fi
echo "结束标记 ：" `date +"%Y-%m-%d %H:%M:%S"`





