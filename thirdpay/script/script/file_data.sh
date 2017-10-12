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
datapath=/home/inf/airflow/dags/thirdpay/data/file_data/${datepath}

# 删除目录 
rm -rf ${datapath}

#创建目录 
mkdir -p ${datapath}



echo "标记检查开始 ：" `date +"%Y-%m-%d %H:%M:%S"`

CheckTag -d ${day} -l day  -b default.mds_fin_third_pay_no_lkl
CheckTag -d ${day} -l day  -b default.mds_fin_third_pay_lkl
echo "标记检查结束 ：" `date +"%Y-%m-%d %H:%M:%S"`


echo "开始计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"` 


sql="
create TEMPORARY table temp_mds_fin_u8_third_pay_result as
select '北京' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       '' as project,
       '' as project_cate,
       case when a.system = 'BR_BJ_BJ_SXF' then a.money_sxf_bj
            when a.system = 'BR_BJ_TJ_SXF' then a.money_sxf_tj
            when a.system = 'BR_BJ_HZ_SXF' then a.money_sxf_hz
            when a.system = 'BR_BJ_YFZK' then a.money_yszk
            when a.system = 'BR_BJ_TJ_NBWL' then a.money_nbwl_tj
            when a.system = 'BR_BJ_HZ_NBWL' then a.money_nbwl_hz
       end as money,
       concat_ws(' ',pstng_date,b.item_text) as text,
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
               round(sum(money_sxf_bj),2) as money_sxf_bj,
               round(sum(money_sxf_tj),2) as money_sxf_tj,
               round(sum(money_sxf_hz),2) as money_sxf_hz,
               round(sum(money_yszk),2) as money_yszk,
               round(sum(money_nbwl_tj),2) as money_nbwl_tj,
               round(sum(money_nbwl_hz),2) as money_nbwl_hz
         from default.mds_fin_u8_third_pay_no_lkl
        where dt = '$day'
        group by pstng_date,flag,system,module
       ) as a
       JOIN default.mds_fin_U8_third_pay_dimension as b
        on (a.flag = b.flag)
union all
select '北京' as name,
       '0102' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       '' as project,
       '' as project_cate,
       a.money as money,
       concat_ws(' ',pstng_date,b.item_text) as text,
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
          from default.mds_fin_u8_third_pay_no_lkl
        where dt = '$day'
         group by pstng_date,module
        ) as a
        join default.mds_fin_U8_third_pay_dimension as b
        on (concat('BR_BJ_YHCK_',a.module) = b.flag)
union all
select '天津' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       '' as project,
       '' as project_cate,
       a.money as money,
       concat_ws(' ',pstng_date,b.item_text) as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             round(sum(money_nbwl_tj),2) as money,
             module
        from default.mds_fin_u8_third_pay_no_lkl
       where name = '天津'
         and dt = '$day'
       group by pstng_date,module
      ) as a
      join default.mds_fin_U8_third_pay_dimension as b
      on (concat('BR_TJ_NBWL_',a.module) = b.flag)
  union all
select '天津' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       '' as project,
       '' as project_cate,
       a.money as money,
       concat_ws(' ',pstng_date,b.item_text) as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             round(sum(money_nbwl_tj),2) as money,
             module
        from default.mds_fin_u8_third_pay_no_lkl
       where name = '天津'
         and dt = '$day'
       group by pstng_date,module
      ) as a
      join default.mds_fin_U8_third_pay_dimension as b
      on (concat('BR_TJ_YFZK_',a.module) = b.flag)
union all
select '杭州' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       '' as project,
       '' as project_cate,
       a.money as money,
       concat_ws(' ',pstng_date,b.item_text) as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             round(sum(money_nbwl_hz),2) as money,
             module
        from default.mds_fin_u8_third_pay_no_lkl
       where name = '杭州'
         and dt = '$day'
       group by pstng_date,module
      ) as a
      join default.mds_fin_U8_third_pay_dimension as b
      on (concat('BR_HZ_NBWL_',a.module) = b.flag)
  union all
select '杭州' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       '' as project,
       '' as project_cate,
       a.money as money,
       concat_ws(' ',pstng_date,b.item_text) as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             round(sum(money_nbwl_hz),2) as money,
             module
        from default.mds_fin_u8_third_pay_no_lkl
       where name = '杭州'
         and dt = '$day'
       group by pstng_date,module
      ) as a
      join default.mds_fin_U8_third_pay_dimension as b
      on (concat('BR_HZ_YFZK_',a.module) = b.flag)
union all
select '北京' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type as doc_type,
       b.dctype as dctype,
       b.gl_account as doc_number,
       b.dd as dd,
       '' as project,
       '' as project_cate,
       case when a.flag = 'BR_BJ_BJ_SXF_LKL' then a.money_sxf_bj_lkl
            when a.flag = 'BR_BJ_LSZG_TJ_SXF_LKL' then a.money_sxf_tj_lkl
            when a.flag = 'BR_BJ_LSZG_HZ_SXF_LKL' then a.money_sxf_hz_lkl
            when a.flag = 'BR_BJ_YFZK_LKL' then a.money_yszk_bj
            when a.flag = 'BR_BJ_YFZK_LKL_CASH' then a.money_yszk_cys_bj
            when a.flag = 'BR_BJ_LSZG_TJ_NBWL_LKL' then a.money_nbwl_cys_tj
            when a.flag = 'BR_BJ_LSZG_HZ_NBWL_LKL' then a.money_nbwl_cys_hz
       end as money,
       concat_ws(' ',lkl_date,a.waybill_no,b.item_text) as text,
       pstng_date as pstng_date,
       case when a.flag = 'BR_BJ_YFZK_LKL_CASH' then a.cy_code_q
            else b.customer end as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             lkl_date,
             name,
             system,
             module,
             flag,
             cy_code_q,
             waybill_no,
             round(sum(money_sxf_bj_lkl),2) as money_sxf_bj_lkl,
             round(sum(money_sxf_tj_lkl),2) as money_sxf_tj_lkl,
             round(sum(money_sxf_hz_lkl),2) as money_sxf_hz_lkl,
             round(sum(money_yszk_bj),2) as money_yszk_bj,
             round(sum(money_yszk_cys_bj),2) as money_yszk_cys_bj,
             round(sum(money_nbwl_cys_tj),2) as money_nbwl_cys_tj,
             round(sum(money_nbwl_cys_hz),2) as money_nbwl_cys_hz 
        from default.mds_fin_u8_third_pay_lkl
       where name = '北京'
         and dt = '$day'
       group by pstng_date,lkl_date,name,system,module,flag,cy_code_q,waybill_no
      ) as a
      JOIN default.mds_fin_U8_third_pay_dimension as b
       on (a.flag = b.flag)
union all
select '北京' as name,
       '0102' as waybill_no,
       '' as tax_rate,
       b.doc_type as doc_type,
       b.dctype as dctype,
       b.gl_account as doc_number,
       b.dd as dd,
       '' as project,
       '' as project_cate,
       round((a.money_yszk_bj + a.money_yszk_cys_bj + a.money_nbwl_cys_tj + a.money_nbwl_cys_hz) - (a.money_sxf_bj_lkl + a.money_sxf_tj_lkl + a.money_sxf_hz_lkl),2) as money,
       concat_ws(' ',lkl_date,b.item_text) as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             lkl_date,
             substr(module,1,3) as module,
             sum(money_sxf_bj_lkl) as money_sxf_bj_lkl,
             sum(money_sxf_tj_lkl) as money_sxf_tj_lkl,
             sum(money_sxf_hz_lkl) as money_sxf_hz_lkl,
             sum(money_yszk_bj) as money_yszk_bj,
             sum(money_yszk_cys_bj) as money_yszk_cys_bj,
             sum(money_nbwl_cys_tj) as money_nbwl_cys_tj,
             sum(money_nbwl_cys_hz) as money_nbwl_cys_hz
        from default.mds_fin_u8_third_pay_lkl
       where name = '北京'
         and dt = '$day'
       group by pstng_date,lkl_date,substr(module,1,3)
      ) as a
      JOIN default.mds_fin_U8_third_pay_dimension as b
       on (concat('BR_BJ_YHCK_',a.module) = b.flag)
union all
select '天津' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type as doc_type,
       b.dctype as dctype,
       b.gl_account as doc_number,
       b.dd as dd,
       '' as project,
       '' as project_cate,
       case when a.flag = 'BR_TJ_YFZK_LKL' then a.money_yszk_tj
            when a.flag = 'BR_TJ_YFZK_LKL_CASH' then a.money_yszk_cys_tj
       end as money,
       concat_ws(' ',lkl_date,a.waybill_no,b.item_text) as text,
       pstng_date as pstng_date,
       case when a.flag = 'BR_TJ_YFZK_LKL_CASH' then a.cy_code_q
            else b.customer end as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             lkl_date,
             name,
             system,
             module,
             flag,
             cy_code_q,
             waybill_no,
             sum(money_yszk_tj) as money_yszk_tj,
             sum(money_yszk_cys_tj) as money_yszk_cys_tj
        from default.mds_fin_u8_third_pay_lkl
       where name = '天津'
         and dt = '$day'
       group by pstng_date,lkl_date,name,system,module,flag,cy_code_q,waybill_no
      ) as a
      JOIN default.mds_fin_U8_third_pay_dimension as b
       on (a.flag = b.flag)
  union all
  select '天津' as name,
         '' as waybill_no,
         '' as tax_rate,
         b.doc_type as doc_type,
         b.dctype as dctype,
         b.gl_account as doc_number,
         b.dd as dd,
         '' as project,
         '' as project_cate,
         round(a.money_yszk_tj,2) as money,
         concat_ws(' ',lkl_date,b.item_text) as text,
         pstng_date as pstng_date,
         '' as customer,
         '1' as status,
         unix_timestamp() as created_at,
         unix_timestamp() as updated_at
    from
        (
        select pstng_date,
               lkl_date,
               name,
               system,
               'LKL' as module,
               sum(money_yszk_tj + money_yszk_cys_tj) as money_yszk_tj
          from default.mds_fin_u8_third_pay_lkl
         where name = '天津'
           and dt = '$day'
         group by pstng_date,lkl_date,name,system
        ) as a
        JOIN default.mds_fin_U8_third_pay_dimension as b
         on (concat('BR_TJ_NBWL_',a.module) = b.flag)
union all
select '杭州' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type as doc_type,
       b.dctype as dctype,
       b.gl_account as doc_number,
       b.dd as dd,
       '' as project,
       '' as project_cate,
       case when a.flag = 'BR_HZ_YFZK_LKL' then a.money_yszk_hz
            when a.flag = 'BR_HZ_YFZK_LKL_CASH' then a.money_yszk_cys_hz
       end as money,
       concat_ws(' ',lkl_date,a.waybill_no,b.item_text) as text,
       pstng_date as pstng_date,
       case when a.flag = 'BR_HZ_YFZK_LKL_CASH' then a.cy_code_q 
            else b.customer end as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from
      (
      select pstng_date,
             lkl_date,
             name,
             system,
             module,
             flag,
             cy_code_q,
             waybill_no,
             round(sum(money_yszk_hz),2) as money_yszk_hz,
             round(sum(money_yszk_cys_hz),2) as money_yszk_cys_hz
        from default.mds_fin_u8_third_pay_lkl
       where name = '杭州'
         and dt = '$day'
       group by pstng_date,lkl_date,name,system,module,flag,cy_code_q,waybill_no
      ) as a
      JOIN default.mds_fin_U8_third_pay_dimension as b
       on (a.flag = b.flag)
  union all
  select '杭州' as name,
         '' as waybill_no,
         '' as tax_rate,
         b.doc_type as doc_type,
         b.dctype as dctype,
         b.gl_account as doc_number,
         b.dd as dd,
         '' as project,
         '' as project_cate,
         round(a.money_yszk_hz,2) as money,
         concat_ws(' ',lkl_date,b.item_text) as text,
         pstng_date as pstng_date,
         '' as customer,
         '1' as status,
         unix_timestamp() as created_at,
         unix_timestamp() as updated_at
    from
        (
        select pstng_date,
               lkl_date,
               name,
               system,
               'LKL' as module,
               sum(money_yszk_hz + money_yszk_cys_hz) as money_yszk_hz
          from default.mds_fin_u8_third_pay_lkl
         where name = '杭州'
           and dt = '$day'
         group by pstng_date,lkl_date,name,system
        ) as a
        JOIN default.mds_fin_U8_third_pay_dimension as b
         on (concat('BR_HZ_NBWL_',a.module) = b.flag);


insert overwrite table mds_fin_u8_third_pay_result partition(dt='$day') 
select a.name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type,
       b.dctype,
       b.gl_account doc_number,
       b.dd,
       '' as project,
       '' as project_cate,
       sub_money as money,
       concat_ws(' ',pstng_date,b.item_text) as text,
       pstng_date as pstng_date,
       '' as customer,
       '1' as status,
       unix_timestamp() as created_at,
       unix_timestamp() as updated_at
  from 
(  
select name,pstng_date,doc_type,flag,sub_money
  from 
(
select name,pstng_date,doc_type,
       case when doc_type = 'BR1' then 'BR_BJ_YFZK_WX_SM'
            when doc_type = 'BR2' then 'BR_BJ_YFZK_WX_APP'
            when doc_type = 'BR3' then 'BR_BJ_YFZK_WX_GZH'
            when doc_type = 'BR4' then 'BR_BJ_YFZK_ZFB'
            when doc_type = 'BR13' then 'BR_BJ_YFZK_LKL'
            when doc_type = 'BR14' then 'BR_TJ_YFZK_LKL'
            when doc_type = 'BR15' then 'BR_HZ_YFZK_LKL'
            when doc_type = 'BR17' then 'BR_BJ_YFZK_ZFB_SM'
            else 'other'
        end flag,
       round(round(sum(case when dctype = 'C' then money end),2) - round(sum(case when dctype = 'D' then money end),2),2) as sub_money
  from temp_mds_fin_u8_third_pay_result
group by name,pstng_date,doc_type
) a 
where sub_money <> 0
) as a
JOIN default.mds_fin_U8_third_pay_dimension as b
on (a.flag = b.flag)
union all
select name, waybill_no, tax_rate, doc_type, dctype, doc_number, dd, project, project_cate, money, text, 
       pstng_date, customer, status, created_at, updated_at
  from temp_mds_fin_u8_third_pay_result;

"
 echo $sql

 /usr/bin/hive -S -e "$sql" 

 SetTagStatus -d $day -l day -b default.mds_fin_third_pay_result -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

 SetTagStatus -d $day -l day -b default.mds_fin_third_pay_result -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D 
 
 CheckTag -d ${day} -l day  -b default.mds_fin_third_pay_result
 
# hive -S -e "  select name,waybill_no,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,'' as vendor,
#                      status,unix_timestamp() created_at,unix_timestamp() updated_at
#                 from mds_fin_u8_third_pay_result 
# where dt = '2017-03-13'
#            "  > ${datapath}/file_data${day} 

#if [ $? -ne 0 ] 
#then
#     SendMail -t zhaoning -o "data load is err " -u  "file_no_lkl.sh/default.mds_fin_third_pay_no_lkl CheckTag data is err"
#     echo "执行失败:"$? 
#   exit 255

#else 

#  echo "执行成功"

#fi
#    echo "计算结束数据 ：" `date +"%Y-%m-%d %H:%M:%S"`

#    echo "写入数据库开始 ：" `date +"%Y-%m-%d %H:%M:%S"`

#   mysql -h192.168.60.49 -uroot -proot123 -P3332 --local-infile=1  -e "
#    SET character_set_database = utf8;
#    SET character_set_server = utf8;
#    delete from dt_db.ofc_account_document where dctype like 'BR%' and status =1 and pstng_date = '$day';
#    "



#   mysql -h192.168.60.49 -uroot -proot123 -P3332 --local-infile=1 -e "
#    SET character_set_database = utf8;
#    SET character_set_server = utf8;
#    LOAD DATA LOCAL INFILE '${datapath}/file_data$day' REPLACE INTO TABLE dt_db.ofc_account_document(name,waybill_no,tax_rate,dctype,cd_type,doc_number,dd,money,text,pstng_date,customer,status,created_at,updated_at)
#    " 

#  if [ $? -ne 0 ]
#  then
#       echo "数据库写入失败:"$?
#     exit 255

#  else

#    echo "数据库写入成功"

#  fi
#  echo "写入数据库完成 ：" `date +"%Y-%m-%d %H:%M:%S"`


