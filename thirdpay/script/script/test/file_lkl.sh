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
datapath=/home/inf/airflow/dags/thirdpay/data/file_lkl/${datepath}

# 删除目录 
rm -rf ${datapath}

#创建目录 
mkdir -p ${datapath}



echo "标记检查开始 ：" `date +"%Y-%m-%d %H:%M:%S"`

CheckTag -d ${day} -l day  -b default.mds_fin_third_pay_lkl

echo "标记检查结束 ：" `date +"%Y-%m-%d %H:%M:%S"`


echo "开始计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"` 

sql="
select '北京' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type as doc_type,
       b.dctype as dctype,
       case when a.flag = 'BR_BJ_YFZK_LKL_CASH' then a.cy_code_q
            else b.gl_account end as doc_number,
       b.dd as dd,
       case when a.flag = 'BR_BJ_BJ_SXF_LKL' then round(a.money_sxf_bj_lkl,2)
            when a.flag = 'BR_BJ_YFZK_LKL' then round(a.money_yszk_bj,2)
            when a.flag = 'BR_BJ_YFZK_LKL_CASH' then round(a.money_yszk_cys_bj,2)
       end as money,
       concat(lkl_date,' ',b.dd_name) as text,
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
             module,
             flag,
             cy_code_q,
             sum(money_sxf_bj_lkl) as money_sxf_bj_lkl,
             sum(money_yszk_bj) as money_yszk_bj,
             sum(money_yszk_cys_bj) as money_yszk_cys_bj
        from default.mds_fin_third_pay_lkl
       where name = '北京'
         and dt = '$day'
       group by pstng_date,lkl_date,name,system,module,flag,cy_code_q
      ) as a
      JOIN temp_finance_certificate_info_lm_zhaoning as b
       on (a.flag = b.flag)
union all
select '北京' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type as doc_type,
       b.dctype as dctype,
       b.gl_account as doc_number,
       b.dd as dd,
       round((a.money_yszk_bj + a.money_yszk_cys_bj) - a.money_sxf_bj_lkl,2) as money,
       concat(lkl_date,' ',b.dd_name) as text,
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
             sum(money_yszk_bj) as money_yszk_bj,
             sum(money_yszk_cys_bj) as money_yszk_cys_bj
        from default.mds_fin_third_pay_lkl
       where name = '北京'
         and dt = '$day'
       group by pstng_date,lkl_date,substr(module,1,3)
      ) as a
      JOIN temp_finance_certificate_info_lm_zhaoning as b
       on (concat('BR_BJ_YHCK_',a.module) = b.flag)
union all
select '天津' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type as doc_type,
       b.dctype as dctype,
       case when a.flag = 'BR_TJ_YFZK_LKL_CASH' then a.cy_code_q
            else b.gl_account end as doc_number,
       b.dd as dd,
       case when a.flag = 'BR_TJ_YFZK_LKL' then round(a.money_yszk_tj,2)
            when a.flag = 'BR_TJ_YFZK_LKL_CASH' then round(a.money_yszk_cys_tj,2)
       end as money,
       concat(lkl_date,' ',b.dd_name) as text,
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
             module,
             flag,
             cy_code_q,
             sum(money_yszk_tj) as money_yszk_tj,
             sum(money_yszk_cys_tj) as money_yszk_cys_tj
        from default.mds_fin_third_pay_lkl
       where name = '天津'
         and dt = '$day'
       group by pstng_date,lkl_date,name,system,module,flag,cy_code_q
      ) as a
      JOIN temp_finance_certificate_info_lm_zhaoning as b
       on (a.flag = b.flag)
  union all
  select '天津' as name,
         '' as waybill_no,
         '' as tax_rate,
         b.doc_type as doc_type,
         b.dctype as dctype,
         b.gl_account as doc_number,
         b.dd as dd,
         round(a.money_yszk_tj,2) as money,
         concat(lkl_date,' ',b.dd_name) as text,
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
          from default.mds_fin_third_pay_lkl
         where name = '天津'
           and dt = '$day'
         group by pstng_date,lkl_date,name,system
        ) as a
        JOIN temp_finance_certificate_info_lm_zhaoning as b
         on (concat('BR_TJ_NBWL_',a.module) = b.flag)
union all
select '杭州' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type as doc_type,
       b.dctype as dctype,
       case when a.flag = 'BR_HZ_YFZK_LKL_CASH' then a.cy_code_q
            else b.gl_account end as doc_number,
       b.dd as dd,
       case when a.flag = 'BR_HZ_YFZK_LKL' then round(a.money_yszk_hz,2)
            when a.flag = 'BR_HZ_YFZK_LKL_CASH' then round(a.money_yszk_cys_hz,2)
       end as money,
       concat(lkl_date,' ',b.dd_name) as text,
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
             module,
             flag,
             cy_code_q,
             sum(money_yszk_hz) as money_yszk_hz,
             sum(money_yszk_cys_hz) as money_yszk_cys_hz
        from default.mds_fin_third_pay_lkl
       where name = '杭州'
         and dt = '$day'
       group by pstng_date,lkl_date,name,system,module,flag,cy_code_q
      ) as a
      JOIN temp_finance_certificate_info_lm_zhaoning as b
       on (a.flag = b.flag)
  union all
  select '杭州' as name,
         '' as waybill_no,
         '' as tax_rate,
         b.doc_type as doc_type,
         b.dctype as dctype,
         b.gl_account as doc_number,
         b.dd as dd,
         round(a.money_yszk_hz,2) as money,
         concat(lkl_date,' ',b.dd_name) as text,
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
          from default.mds_fin_third_pay_lkl
         where name = '杭州'
           and dt = '$day'
         group by pstng_date,lkl_date,name,system
        ) as a
        JOIN temp_finance_certificate_info_lm_zhaoning as b
         on (concat('BR_HZ_NBWL_',a.module) = b.flag)
union all
select '链商中国' as name,
       '' as waybill_no,
       '' as tax_rate,
       b.doc_type as doc_type,
       b.dctype as dctype,
       b.gl_account as doc_number,
       b.dd as dd,
       case when a.system = 'BR_LSZG_TJ_SXF' then round(a.money_sxf_tj_lkl,2)
            when a.system = 'BR_LSZG_HZ_SXF' then round(a.money_sxf_hz_lkl,2)
            when a.system = 'BR_LSZG_TJ_NBWL' then round(a.money_nbwl_cys_tj,2)
            when a.system = 'BR_LSZG_HZ_NBWL' then round(a.money_nbwl_cys_hz,2)
       end as money,
       concat(lkl_date,' ',b.dd_name) as text,
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
             module,
             flag,
             sum(money_sxf_tj_lkl) as money_sxf_tj_lkl,
             sum(money_sxf_hz_lkl) as money_sxf_hz_lkl,
             sum(money_nbwl_cys_tj) as money_nbwl_cys_tj,
             sum(money_nbwl_cys_hz) as money_nbwl_cys_hz
        from default.mds_fin_third_pay_lkl
       where name = '链商中国'
         and dt = '$day'
       group by pstng_date,lkl_date,name,system,module,flag
      ) as a
      JOIN temp_finance_certificate_info_lm_zhaoning as b
       on (a.flag = b.flag)
 union all
 select '链商中国' as name,
        '' as waybill_no,
        '' as tax_rate,
        b.doc_type as doc_type,
        b.dctype as dctype,
        b.gl_account as doc_number,
        b.dd as dd,
        round((a.money_nbwl_cys_tj + a.money_nbwl_cys_hz) - (a.money_sxf_tj_lkl + a.money_sxf_hz_lkl),2) as money,
        concat(a.lkl_date,' ',b.dd_name) as text,
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
              sum(money_sxf_tj_lkl) as money_sxf_tj_lkl,
              sum(money_sxf_hz_lkl) as money_sxf_hz_lkl,
              sum(money_nbwl_cys_tj) as money_nbwl_cys_tj,
              sum(money_nbwl_cys_hz) as money_nbwl_cys_hz
         from default.mds_fin_third_pay_lkl
        where name = '链商中国'
          and dt = '$day'
        group by pstng_date,lkl_date,substr(module,1,3)
       ) as a
       JOIN temp_finance_certificate_info_lm_zhaoning as b
        on (concat('BR_LSZG_YHCK_',a.module) = b.flag)
"
 echo $sql

 hive -S -e "$sql" > ${datapath}/file_lkl$day 

if [ $? -ne 0 ] 
then

     SendMail -t zhaoning -o " data load is err " -u  "file_lkl.sh/default.mds_fin_third_pay_lkl data is err"
     echo "执行失败:"$? 
   exit 255

else 

  echo "执行成功"

fi
echo "结束计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"`

echo "开始写入数据 ：" `date +"%Y-%m-%d %H:%M:%S"`
echo $day
mysql -h192.168.60.49 -uroot -proot123 -P3332 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
delete from dt_db.ofc_account_document where dctype in ('BR13','BR14','BR15','BR16') and status =1 and pstng_date = '$day';
"
echo ${datapath}/file_lkl$day
mysql -h192.168.60.49 -uroot -proot123 -P3332 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8; 
LOAD DATA LOCAL INFILE '${datapath}/file_lkl$day' REPLACE INTO TABLE dt_db.ofc_account_document(name,waybill_no,tax_rate,dctype,cd_type,doc_number,dd,money,text,pstng_date,customer,status,created_at,updated_at)
"

if [ $? -ne 0 ]
then

     echo "写入失败:"$?
   exit 255

else

  echo "写入成功"

fi

echo "结束写入数据 ：" `date +"%Y-%m-%d %H:%M:%S"`


