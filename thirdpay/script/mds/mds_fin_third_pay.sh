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

CheckTag -d ${day} -l day  -b default.mds_fin_third_pay_base

echo "标记检查结束 ：" `date +"%Y-%m-%d %H:%M:%S"`


echo "开始计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"` 


sql="
insert overwrite table default.mds_fin_third_pay partition(dt='$day')
select case when nvl(a.zone_id,b.zone_id) = 1000 then '北京'
            when nvl(a.zone_id,b.zone_id) = 1001 then '天津'
            when nvl(a.zone_id,b.zone_id) = 1002 then '杭州'
            end
       as name,
       b.order_id as order_id,
       b.receipt_order_id as receipt_order_id,
       c.waybill_no as waybill_no,
       a.module as module,
       a.money as money,
       a.count_fee as count_fee,
       a.rate as rate,
       a.transaction_time as transaction_time,
       a.lkl_trans_time,
       a.transaction_id as serial_no,
       a.trade_id as trade_id,
       case when b.order_id is not null then 'YES'
            else 'NO' end compare_sign

  FROM
       (select a.transaction_id as transaction_id,
              case when b.trade_module = 'bill' then concat(a.module,'_CASH')
                   else a.module end as module,
              a.money as money,
              a.count_fee as count_fee,
              a.rate as rate,
              a.transaction_time as transaction_time,
              a.lkl_trans_time,
              b.trade_id as trade_id,
              c.zone_id
         from
              (select transaction_id as transaction_id,
                    module,
                    money,
                    count_fee,
                    rate,
                    transaction_time,
                    lkl_trans_time
                from mds_fin_third_pay_base
               where dt = '$day'
              ) as a
              LEFT OUTER JOIN
              (select pay_id,
                      trade_id,
                      channel_transaction,
                      trade_module
                from  ods.ods_lsh_payment_pay_deal
               where dt = '$day'
                 and pay_status = 3
                 and pay_type = 1
               ) as b
               on (a.transaction_id = b.channel_transaction)
               left outer join
               (SELECT waybill_no,
                       zone_id
                  FROM ods.ods_yougong_order_shipping_head
                 WHERE dt='$day'
                   AND to_date(from_unixtime(shipped_at))>='2016-08-30'
                 GROUP BY waybill_no,zone_id
               ) as c
               on (b.trade_id = c.waybill_no)
       ) as a
       left outer join
       (SELECT zone_id,
               receipt_order_id,
               order_id
          FROM ods.ods_yougong_order_receipt_head
         WHERE dt='$day'
           AND is_valid=1
           AND pay_status=2
        union all
        select zone_id,
               receipt_order_id,
               order_id
          from ods.ods_lsh_oms_order_sign_head
         where dt = '$day'
           and tms_id = 0
        ) AS b
        on (a.trade_id = b.receipt_order_id)
        left outer join
        (SELECT waybill_no,
                order_id
           FROM ods.ods_yougong_order_shipping_head
          WHERE dt='$day'
            AND to_date(from_unixtime(shipped_at))>='2016-08-30'
          GROUP BY waybill_no,order_id
         union all
         SELECT '' waybill_no,
                order_id
           FROM ods.ods_lsh_oms_order_shipping_head
          WHERE dt='$day'
            AND tms_id = 0
          GROUP BY waybill_no,order_id
        ) as c
        on (b.order_id = c.order_id);
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

echo "开始标记 ：" `date +"%Y-%m-%d %H:%M:%S"`

SetTagStatus -d $day -l day -b default.mds_fin_third_pay -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

SetTagStatus -d $day -l day -b default.mds_fin_third_pay -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

if [ $? -ne 0 ]
then

  echo "标记失败:"$?
#SendMessage -i 13720026387,18910320926 -o " data load err  " -a "msql_mds_vrm_vender_main_pay"
SendMail -t zhaoning -o "data load is err " -u  "mds_fin_third_pay.sh data is err" 
  exit 255

else

    echo "标记成功"

fi
echo "结束标记 ：" `date +"%Y-%m-%d %H:%M:%S"`
