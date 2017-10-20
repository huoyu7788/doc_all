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
       dd,project,project_cate,money,text,pstng_date,customer,' as vendor,
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



--造数据
insert into bank_credit_bug
(ticket_id, it_amount, pay_amount, final_amount, rate, factorage, type, pay_type, 
date, lkl_date, day, status, gen_evidence, check_mall_data, pay_payment_no, zone_id, 
zone_name, created_at, updated_at)
select 
'0001',0.0000,0.0000,2413.5,35,9.1700,1,8,1400,'2017-10-15 00:00:00','2017-10-15',  
0,0,0,'090850843667','','杭州',1505110193,1505110193



--处理非现金
insert into fin_certificate_no_lkl_detail_bug
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
              select case when pay_type = 1 then 'ZFB'
                          when pay_type = 2 then 'WX_APP'
                          when pay_type = 6 then 'WX_SM'
                          when pay_type = 8 then 'ZFB_SM'
                     end module,
                     sum(factorage) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = '北京'
                 and pay_type in (1,2,6,8)
                 and day = '2017-10-15'
                 and type = 1
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat('BR_BJ_BJ_SXF_',a.module) = b.flag)
        union all
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
              select case when pay_type = 1 then 'ZFB'
                          when pay_type = 2 then 'WX_APP'
                          when pay_type = 6 then 'WX_SM'
                          when pay_type = 8 then 'ZFB_SM'
                     end module,
                     sum(factorage) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = '天津'
                 and pay_type in (1,2,6,8)
                 and day = '2017-10-15'
                 and type = 1
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat('BR_BJ_TJ_SXF_',a.module) = b.flag)
        union all
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
              select case when pay_type = 1 then 'ZFB'
                          when pay_type = 2 then 'WX_APP'
                          when pay_type = 6 then 'WX_SM'
                          when pay_type = 8 then 'ZFB_SM'
                     end module,
                     sum(factorage) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = '杭州'
                 and pay_type in (1,2,6,8)
                 and day = '2017-10-15'
                 and type = 1
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat('BR_BJ_HZ_SXF_',a.module) = b.flag)
        union all
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
              select case when pay_type = 1 then 'ZFB'
                          when pay_type = 2 then 'WX_APP'
                          when pay_type = 6 then 'WX_SM'
                          when pay_type = 8 then 'ZFB_SM'
                     end module,
                     sum(final_amount) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = '北京'
                 and pay_type in (1,2,6,8)
                 and day = '2017-10-15'
                 and type = 1
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat('BR_BJ_YFZK_',a.module) = b.flag)

        union all
        select '天津' as name,
               0 as money_sxf_bj,
               0 as money_sxf_tj,
               0 as money_sxf_hz,
               0 as money_yszk,
               round(a.money,2)  as money_nbwl_tj,
               0 as money_nbwl_hz,
               'BR_BJ_TJ_NBWL' as system,
               b.flag as flag,
               a.module as module,
               a.psting_date as pstng_date
          from
              (
              select case when pay_type = 1 then 'ZFB'
                          when pay_type = 2 then 'WX_APP'
                          when pay_type = 6 then 'WX_SM'
                          when pay_type = 8 then 'ZFB_SM'
                     end module,
                     sum(final_amount) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = '天津'
                 and pay_type in (1,2,6,8)
                 and day = '2017-10-15'
                 and type = 1
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat('BR_BJ_TJ_NBWL_',a.module) = b.flag)

        union all
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
              select case when pay_type = 1 then 'ZFB'
                          when pay_type = 2 then 'WX_APP'
                          when pay_type = 6 then 'WX_SM'
                          when pay_type = 8 then 'ZFB_SM'
                     end module,
                     sum(final_amount) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = '杭州'
                 and pay_type in (1,2,6,8)
                 and day = '2017-10-15'
                 and type = 1
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat('BR_BJ_HZ_NBWL_',a.module) = b.flag);


--处理现金
insert into fin_certificate_lkl_detail_bug
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
            select 'LKL' module,
                   sum(factorage) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit_bug
             where zone_name = '北京'
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat('BR_BJ_BJ_SXF_',a.module) = b.flag)
      union all
      select '北京' as name,
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
             'BR_BJ_LSZG_TJ_SXF' as system,
             '' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '' waybill_no
        from
            (
            select 'LKL' as module,
                   sum(factorage) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit_bug
             where zone_name = '天津'
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat('BR_BJ_LSZG_TJ_SXF_',a.module) = b.flag)
      union all
      select '北京' as name,
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
             'BR_BJ_LSZG_HZ_SXF' as system,
             '' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '' waybill_no
        from
            (
            select 'LKL' as module,
                   sum(factorage) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit_bug
             where zone_name = '杭州'
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat('BR_BJ_LSZG_HZ_SXF_',a.module) = b.flag)
      union all
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
            select 'LKL' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit_bug
             where zone_name = '北京'
               and zone_name is not null
               and pay_type = 5
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat('BR_BJ_YFZK_',a.module) = b.flag)
      union all
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
            select 'LKL' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit_bug
             where zone_name = '天津'
               and zone_name is not null
               and pay_type = 5
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat('BR_TJ_YFZK_',a.module) = b.flag)
      union all
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
            select 'LKL' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit_bug
             where zone_name = '杭州'
               and zone_name is not null
               and pay_type = 5
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat('BR_HZ_YFZK_',a.module) = b.flag)
      union all
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
             ifnull(c.f_code,'01010013') as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             a.trade_id waybill_no
        from
            (
            select 'LKL_CASH'as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date,
                   ticket_id as trade_id
              from bank_credit_bug
             where zone_name = '北京'
               and pay_type in (3,10,11)
               and zone_name is not null
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type,ticket_id
            ) as a
            left outer join
            (select waybill_id as waybill_no, f_code
               from waybill_fin_code
            ) as c
            on (a.trade_id = c.waybill_no)
            join
            dim_third_pay_u8 as b
            on (concat('BR_BJ_YFZK_',a.module) = b.flag)
      union all
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
             ifnull(c.f_code,'02010003') as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             a.trade_id waybill_no
        from
            (
            select 'LKL_CASH' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date,
                   ticket_id as trade_id
              from bank_credit_bug
             where zone_name = '天津'
               and pay_type in (3,10,11)
               and zone_name is not null
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type,ticket_id
            ) as a
            left outer join
            (select waybill_id as waybill_no, f_code
               from waybill_fin_code
            ) as c
            on (a.trade_id = c.waybill_no)
            join
            dim_third_pay_u8 as b
            on (concat('BR_TJ_YFZK_',a.module) = b.flag)
      union all
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
             ifnull(c.f_code,'03010001') as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             a.trade_id waybill_no
        from
            (
            select 'LKL_CASH' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date,
                   ticket_id as trade_id
              from bank_credit_bug
             where zone_name = '杭州'
               and pay_type in (3,10,11)
               and zone_name is not null
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type,ticket_id
            ) as a
            left outer join
            (select waybill_id as waybill_no, f_code
               from waybill_fin_code
            ) as c
            on (a.trade_id = c.waybill_no)
            join
            dim_third_pay_u8 as b
            on (concat('BR_HZ_YFZK_',a.module) = b.flag)
      union all
      select '北京' as name,
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
             'BR_BJ_LSZG_TJ_NBWL' as system,
             '' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '' waybill_no
        from
            (
            select 'LKL' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit_bug
             where zone_name = '天津'
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat('BR_BJ_LSZG_TJ_NBWL_',a.module) = b.flag)
      union all
      select '北京' as name,
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
             'BR_BJ_LSZG_HZ_NBWL' as system,
             '' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '' waybill_no
        from
            (
            select 'LKL' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit_bug
             where zone_name = '杭州'
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day = '2017-10-15'
               and type = 1
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat('BR_BJ_LSZG_HZ_NBWL_',a.module) = b.flag);












