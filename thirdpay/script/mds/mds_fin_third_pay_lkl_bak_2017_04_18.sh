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

  echo "标记失败:"$?

  SendMail -t zhaoning -o "data load is err " -u  "mds_fin_third_pay_lkl.sh/CheckTag is err"
 
  exit 255

else

    echo "标记成功"

fi


echo "标记检查结束 ：" `date +"%Y-%m-%d %H:%M:%S"`


echo "开始计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"` 


sql="
insert overwrite table mds_fin_u8_third_pay_lkl partition(dt='$day')    
--北京手续费
select '北京' as name,
       round(money,2) as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       0 as money_yszk_tj,
       0 as money_yszk_hz,
       0 as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       0 as money_nbwl_cys_hz,
       'BR_BJ_BJ_SXF' as system,
       '' as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       '' waybill_no
  from
      (
      select substr(module,1,3) as module,
             sum(count_fee) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date
        from mds_fin_third_pay
       where name = '北京'
         and name is not null
         and module like 'LKL%'
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                substr(module,1,3)
      ) as a
        join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_BJ_BJ_SXF_',substr(a.module,1,3)) = b.flag)
union all
--手续费天津
select '链商中国' as name,
       0 as money_sxf_bj_lkl,
       round(money,2) as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       0 as money_yszk_tj,
       0 as money_yszk_hz,
       0 as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       0 as money_nbwl_cys_hz,
       'BR_LSZG_TJ_SXF' as system,
       '' as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       '' waybill_no
  from
      (
      select substr(module,1,3) as module,
             sum(count_fee) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date
        from mds_fin_third_pay
       where name = '天津'
         and name is not null
         and module like 'LKL%'
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                substr(module,1,3)
      ) as a
        join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_LSZG_TJ_SXF_',substr(a.module,1,3)) = b.flag)
union all
--手续费杭州
select '链商中国' as name,
       0 as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       round(money,2) as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       0 as money_yszk_tj,
       0 as money_yszk_hz,
       0 as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       0 as money_nbwl_cys_hz,
       'BR_LSZG_HZ_SXF' as system,
       '' as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       '' waybill_no
  from
      (
      select substr(module,1,3) as module,
             sum(count_fee) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date
        from mds_fin_third_pay
       where name = '杭州'
         and name is not null
         and module like 'LKL%'
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                substr(module,1,3)
      ) as a
        join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_LSZG_HZ_SXF_',substr(a.module,1,3)) = b.flag)
union all
--应收账款-北京
select '北京' as name,
       0 as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       round(money,2) as money_yszk_bj,
       0 as money_yszk_tj,
       0 as money_yszk_hz,
       0 as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       0 as money_nbwl_cys_hz,
       'BR_BJ_YFZK' as system,
       '' as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       '' waybill_no
  from
      (
      select module,
             sum(money) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date
        from mds_fin_third_pay
       where name = '北京'
         and name is not null
         and module = 'LKL'
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                module
      ) as a
        join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_BJ_YFZK_',a.module) = b.flag)
union all
--应收账款-天津
select '天津' as name,
       0 as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       round(money,2) as money_yszk_tj,
       0 as money_yszk_hz,
       0 as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       0 as money_nbwl_cys_hz,
       'BR_TJ_YFZK' as system,
       '' as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       '' waybill_no
  from
      (
      select module,
             sum(money) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date
        from mds_fin_third_pay
       where name = '天津'
         and name is not null
         and module = 'LKL'
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                module
      ) as a
        join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_TJ_YFZK_',a.module) = b.flag)
union all
--应收账款-杭州
select '杭州' as name,
       0 as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       0 as money_yszk_tj,
       round(money,2) as money_yszk_hz,
       0 as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       0 as money_nbwl_cys_hz,
       'BR_HZ_YFZK' as system,
       '' as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       '' waybill_no
  from
      (
      select module,
             sum(money) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date
        from mds_fin_third_pay
       where name = '杭州'
         and name is not null
         and module = 'LKL'
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                module
      ) as a
        join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_HZ_YFZK_',a.module) = b.flag)
union all
--应收账款-承运商-北京
select '北京' as name,
       0 as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       0 as money_yszk_tj,
       0 as money_yszk_hz,
       round(money,2) as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       0 as money_nbwl_cys_hz,
       'BR_BJ_YFZK' as system,
       nvl(e.code_h,'01010013') as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       a.trade_id waybill_no
  from
      (
      select module,
             sum(money) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date,
             trade_id 
        from mds_fin_third_pay
       where name = '北京'
         and module = 'LKL_CASH'
         and name is not null
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                module,
                trade_id
      ) as a
      left outer join
      (select b.waybill_no,
              a.sapcode_q,
              a.code_h
         from (select sapcode_q,
                      code_h,
                      carrier_id,
                      zone_id
                from dim_fin_carrier_info
              ) as a 
              join 
              (
               select waybill_no,
                      wumartcompany_id,
                      zone_id 
                 from ods.ods_tms_order_waybill 
                where dt = '$day'
              ) as b 
              on (b.wumartcompany_id=a.carrier_id and a.zone_id=b.zone_id)
      ) as e
      on (a.trade_id = e.waybill_no)
      join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_BJ_YFZK_',a.module) = b.flag)
      
union all
--应收账款-承运商-天津
select '天津' as name,
       0 as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       0 as money_yszk_tj,
       0 as money_yszk_hz,
       0 as money_yszk_cys_bj,
       round(money,2) as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       0 money_nbwl_cys_hz,
       'BR_TJ_YFZK' as system,
       nvl(e.code_h,'02010003') as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       a.trade_id waybill_no
  from
      (
      select module,
             sum(money) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date,
             trade_id
        from mds_fin_third_pay
       where name = '天津'
         and module = 'LKL_CASH'
         and name is not null
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                module,
                trade_id
      ) as a
      left outer join
      (select b.waybill_no,
              a.sapcode_q,
              a.code_h
         from (select sapcode_q,
                      code_h,
                      carrier_id,
                      zone_id
                from dim_fin_carrier_info
              ) as a 
              join 
              (
               select waybill_no,
                      wumartcompany_id,
                      zone_id 
                 from ods.ods_tms_order_waybill 
                where dt = '$day'
              ) as b 
              on (b.wumartcompany_id=a.carrier_id and a.zone_id=b.zone_id)
      ) as e
      on (a.trade_id = e.waybill_no)
      join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_TJ_YFZK_',a.module) = b.flag)
union all
--应收账款-承运商-杭州
select '杭州' as name,
       0 as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       0 as money_yszk_tj,
       0 as money_yszk_hz,
       0 as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       round(money,2) as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       0 money_nbwl_cys_hz,
       'BR_HZ_YFZK' as system,
       nvl(e.code_h,'03010001') as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       a.trade_id waybill_no
  from
      (
      select module,
             sum(money) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date,
             trade_id
        from mds_fin_third_pay
       where name = '杭州'
         and module = 'LKL_CASH'
         and name is not null
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                module,
                trade_id
      ) as a
      left outer join
      (select b.waybill_no,
              a.sapcode_q,
              a.code_h
         from (select sapcode_q,
                      code_h,
                      carrier_id,
                      zone_id
                from dim_fin_carrier_info
              ) as a 
              join 
              (
               select waybill_no,
                      wumartcompany_id,
                      zone_id 
                 from ods.ods_tms_order_waybill 
                where dt = '$day'
              ) as b 
              on (b.wumartcompany_id=a.carrier_id and a.zone_id=b.zone_id)
      ) as e
      on (a.trade_id = e.waybill_no)
      join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_HZ_YFZK_',a.module) = b.flag)
union all
--内部往来-天津
select '链商中国' as name,
       0 as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       0 as money_yszk_tj,
       0 as money_yszk_hz,
       0 as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       round(money,2) as money_nbwl_cys_tj,
       0 as money_nbwl_cys_hz,
       'BR_LSZG_TJ_NBWL' as system,
       '' as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       '' waybill_no
  from
      (
      select substr(module,1,3) as module,
             sum(money) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date
        from mds_fin_third_pay
       where name = '天津'
         and name is not null
         and module like 'LKL%'
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                module
      ) as a
        join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_LSZG_TJ_NBWL_',a.module) = b.flag)
union all
--内部往来-杭州
select '链商中国' as name,
       0 as money_sxf_bj_lkl,
       0 as money_sxf_tj_lkl,
       0 as money_sxf_hz_lkl,
       0 as money_yszk_bj,
       0 as money_yszk_tj,
       0 as money_yszk_hz,
       0 as money_yszk_cys_bj,
       0 as money_yszk_cys_tj,
       0 as money_yszk_cys_hz,
       0 as money_nbwl_cys_tj,
       round(money,2) money_nbwl_cys_hz,
       'BR_LSZG_HZ_NBWL' as system,
       '' as cy_code_q,
       b.flag as flag,
       a.module as module,
       a.psting_date as pstng_date,
       a.lkl_date as lkl_date,
       '' waybill_no
  from
      (
      select substr(module,1,3) as module,
             sum(money) as money,
             substr(lkl_trans_time,1,10) as psting_date,
             substr(transaction_time,1,10) as lkl_date
        from mds_fin_third_pay
       where name = '杭州'
         and name is not null
         and module like 'LKL%'
         and dt = '$day'
       group by substr(lkl_trans_time,1,10),
                substr(transaction_time,1,10),
                module
      ) as a
        join
      default.mds_fin_u8_third_pay_dimension as b
      on (concat('BR_LSZG_HZ_NBWL_',a.module) = b.flag)
"
$day
 echo $sql

 hive -S -e "$sql"

if [ $? -ne 0 ] 
then
     SendMail -t zhaoning -o "data load is err " -u  "mds_fin_third_pay_lkl.sh data is err"
     echo "执行失败:"$? 
   exit 255

else 

  echo "执行成功"

fi

echo "开始标记 ：" `date +"%Y-%m-%d %H:%M:%S"`

#删除标记

SetTagStatus -d $day -l day -b default.mds_fin_third_pay_lkl -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

#创建标记

SetTagStatus -d $day -l day -b default.mds_fin_third_pay_lkl -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

if [ $? -ne 0 ]
then

  echo "标记失败:"$?

SendMail -t zhaoning -o "data load is err " -u  "mds_fin_third_pay_lkl.sh/SetTagStatus is err" 

  exit 255

else

    echo "标记成功"

fi
echo "结束标记 ：" `date +"%Y-%m-%d %H:%M:%S"`
