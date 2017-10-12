--------------------------------------------------------------------------------------------
RERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERE
--------------------------------------------------------------------------------------------
drop table fin_certificate_re_detail;
create table fin_certificate_re_detail as
select zone_name,receipt_id,'' as tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,f.dd,a.money,concat(pstng_date,' ',f.item_text) text,pstng_date,a.vendor
  from 
(
--库存商品\物美集采
select zone_name,receipt_id,from_unixtime(cast(date as int),'yyyy-MM-dd') pstng_date,nt_amount as money,'RE_WUMART' flag,'' as vendor
  from mds_pss_wm_receipt_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-30'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
union all
--其他应收款\待摊费用\待抵扣进项税（物美）
select zone_name,receipt_id,from_unixtime(cast(date as int),'yyyy-MM-dd') pstng_date,it_amount - nt_amount as money,'RE_TAX_WUMART' flag,'' as vendor
  from mds_pss_wm_receipt_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-30'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
union all
--应付账款\商品采购款\物美集采
select zone_name,receipt_id,from_unixtime(cast(date as int),'yyyy-MM-dd') pstng_date,it_amount as money,'RE_GY' flag,
       case when zone_id = '1000' then '01010001' 
            when zone_id = '1001' then '02010001'
            when zone_id = '1002' then '03010001'
            end 
       as vendor
  from mds_pss_wm_receipt_head
 where dt = '2017-07-31' 
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-30'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
) as a 
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area;


drop table fin_certificate_re;
create table fin_certificate_re as
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,dd,'' as project,'' as project_cate,money,text,pstng_date,'' as customer ,vendor,'1' status,
    unix_timestamp() created_at,unix_timestamp() updated_at
from (
    select zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,f.dd,money,pstng_date text,pstng_date,'' as vendor
    from (
        SELECT zone_name,receipt_id,pstng_date,
            case when sub_money>0 then 'RE_D' else 'RE_C' end flag,
            abs(sub_money) money
        from (
            select zone_name,receipt_id,pstng_date,
                round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
            from  fin_certificate_re_detail
            group by zone_name,receipt_id,pstng_date
        ) a
        where round(sub_money,2)<>0
    ) a
    join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,dd,money,text,pstng_date,vendor
    from fin_certificate_re_detail
) b ;


insert into fin_certificate_re_result
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
  from fin_certificate_re
 group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
       text,customer,vendor,status,created_at,updated_at

hive -e "
select zone_name,receipt_id,tax_rate, doc_type, dctype, doc_number, dd, project, 
       project_cate, money, text, pstng_date, customer, vendor, status, created_at, updated_at 
  from temp_fin_re
" > /home/inf/zhaoning/u8/data/201707/re_20170730_31.txt
--------------------------------------------------------------------------------------------
RVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRV
--------------------------------------------------------------------------------------------
select * from fin_certificate_rv_detail_1 where sale_id = '2909374480459990979';

select text,money,customer from mds_fin_u8_third_pay_result where dt >= '2017-07-14' and dt <= '2017-07-16' and dctype = 'BR15' and doc_number = '11220202' and name = '杭州';
drop table fin_certificate_rv_detail_1;
create table fin_certificate_rv_detail_1 as
select a.zone_name,a.receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
       case when f.gl_account = '11220202' then a.fin_code else '' end as customer,'' as vendor,f.dd,
       a.money,
       concat_ws(' ',a.pstng_date,f.item_text) text,
       f.project,
       f.project_cate,
       a.pstng_date
  from (
        select zone_name,sale_id as receipt_id,order_id,final_amount as money,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,fin_code,
               case when pay_type=3 then '现金'
                    when pay_type=1 then '支付宝'
                    when pay_type=5 then '拉卡拉'
                    when pay_type=6 then '微信-扫码'
                    when pay_type=2 then '微信-APP'
                    when pay_type=8 then '支付宝-扫码' 
                    when pay_type=9 then '赊销'
                    when pay_type=0 then '现金'
                    end as type
          from mds_pss_mall_sale_head
         where dt = '2017-07-31'
           and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-01'
           and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
           and user_type = 1
        ) as a
        join dim_fin_certificate_info_u8_new f on f.flag=TYPE and a.zone_name=f.area;


drop table fin_certificate_rv_detail_2;
create table fin_certificate_rv_detail_2 as
select  a.zone_name,receipt_id,a.tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,'' as customer,'' as vendor,f.dd,f.project,f.project_cate,money*pn as money,concat_ws(' ',pstng_date,item_text) text,pstng_date
FROM (
    --订单折扣前未税
select zone_name,sale_id as receipt_id,pstng_date,tax as tax_rate,
       round(sum(price*qty/(1+tax/100)),2) money,
       concat('NO',case when type = 0 then 'WUMART' 
                        when type = 1 then 'JISHOU' 
                        end,'_',cast(tax as int)) flag
  from temp_finance_sale_detail
 where type in ('0','1')
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
   and user_type = 1
   and sale_type = 0
 group by zone_name,sale_id,pstng_date,tax,type 
union all
select zone_name,sale_id as receipt_id,pstng_date,tax as tax_rate,
       round(sum(price*qty*(tax/100)/(1+tax/100)),2) money,
       concat(case when type = 0 then 'WUMART' 
                        when type = 1 then 'JISHOU' 
                        end,'_',cast(tax as int)) flag
  from temp_finance_sale_detail
 where type in ('0','1')
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
   and user_type = 1
   and sale_type = 0
 group by zone_name,sale_id,pstng_date,tax,type 
union all
select a.zone_name,b.second_receipt_id as receipt_id,a.pstng_date,b.tax as tax_rate,
       round(sum(sale_price*qty/(1+tax/100)),2) money,
       concat('SH_NO',case when b.type = 0 then 'WUMART' 
                           when b.type = 1 then 'JISHOU' 
                           end,'_',cast(tax as int)) flag
  from 
(
select zone_id,
       zone_name,
       afs_id,  
       from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date
  from mds_pss_mall_sale_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-01'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
   and user_type = 1
   and afs_id <> 0
) as a
join 
(select zone_id,
        zone_name,
        return_id,second_receipt_id,product_code,qty,sale_price,tax,type,vendor
   from temp_finance_return_detail
  where type in ('0','1')
) as b
on (a.zone_id = b.zone_id and a.afs_id = b.return_id)
group by a.zone_name,b.second_receipt_id,a.pstng_date,b.tax,b.type
union all
select a.zone_name,b.second_receipt_id as receipt_id,pstng_date,b.tax as tax_rate,
       round(sum(sale_price*qty*(tax/100)/(1+tax/100)),2) money,
       concat('SH_',case when type = 0 then 'WUMART' 
                         when type = 1 then 'JISHOU' 
                          end,'_',cast(tax as int)) flag
  from 
(
select zone_id,zone_name,order_id,afs_id,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date
  from mds_pss_mall_sale_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-01'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
   and user_type = 1
   and afs_id <> 0

) as a
join 
(select zone_id,zone_name,return_id,second_receipt_id,product_code,qty,sale_price,tax,type
   from temp_finance_return_detail
  where type in ('0','1')
) as b
on (a.zone_id = b.zone_id and a.afs_id = b.return_id)
group by a.zone_name,b.second_receipt_id,a.pstng_date,b.tax,b.type
) a join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area;



drop table fin_certificate_rv_detail_3;
create table fin_certificate_rv_detail_3 as
select a.zone_name,a.receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
       a.vendor as customer,'' as vendor,f.dd,f.project,f.project_cate,a.money,concat_ws(' ',a.pstng_date,item_text) text,a.pstng_date
from
(
select a.zone_name,a.receipt_id,a.pstng_date,round(sum(money),2) money,flag,vendor 
  from 
(
select zone_name,sale_id as receipt_id,pstng_date,
       price*qty money,
       case when type = 2 then 'DAIXIAO'
            when type = 2 and vendor = '01020007' then 'SH_BSDS'
            end flag,
       vendor
  from temp_finance_sale_detail
 where type = 2
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
   and user_type = 1
   and sale_type = 0
) as a 
 group by zone_name,receipt_id,pstng_date,flag,vendor
union all
select a.zone_name,a.receipt_id,a.pstng_date,round(sum(money),2) as money,flag,vendor  
  from 
(
select a.zone_name,b.second_receipt_id as receipt_id,pstng_date,
       sale_price*qty money,
       concat('SH',case when b.type = 2 then 'DAIXIAO'
                        when b.type = 2 and b.vendor = '01020007' then 'BSDS'
                        end) flag,
       vendor

  from 
(
select zone_id,zone_name,afs_id,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date
  from mds_pss_mall_sale_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-01'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
   and user_type = 1
   and afs_id <> 0
) as a
join 
(select zone_id,zone_name,return_id,second_receipt_id,product_code,qty,sale_price,tax,type,vendor
   from temp_finance_return_detail
  where type = 2
) as b
on (a.zone_id = b.zone_id and a.afs_id = b.return_id)
) as a 
group by a.zone_name,a.receipt_id,a.pstng_date,a.vendor,a.flag
) a join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area;

--计算折扣数据

drop table fin_certificate_rv_detail;
create table fin_certificate_rv_detail as
select zone_name,sale_id as receipt_id,pstng_date,
       sum(price*qty) all_money,max(final_amount) money,
       sum(case when type = '2' then price*qty else 0 end) dx
  from temp_finance_sale_detail
 where user_type = 1
   and sale_type = 0
 group by zone_name,sale_id,pstng_date;

drop table fin_certificate_rv_detail_4;
create table fin_certificate_rv_detail_4 as
select 
        a.zone_name,
        a.receipt_id,
        a.tax_rate,
        f.doc_type,
        f.dctype,
        f.gl_account doc_number,
        f.dd,
        f.project,
        f.project_cate,
        (round((s_money*(all_money-money)/(all_money-dx))/(1+a.tax_rate/100) ,2))*f.pn money,
        concat_ws(' ',a.pstng_date,item_text) text,
        a.pstng_date,
        a.flag,
        '' as customer,
        '' as vendor,
        '1' status
from (
select zone_name,pstng_date,sale_id as receipt_id,
       case when type = 0 or type = 1 then tax else 0 end tax_rate,
       concat('NO',
              case when type = 0 then 'WUMART' 
                   when type = 1 then 'JISHOU' 
                   --when type = 2 then 'DAIXIAO'
                   end,
              '_',
              case when type = 0 or type = 1 then cast(tax as int) else 0 end,'_ZK') flag,
       sum(price*qty) s_money
  from temp_finance_sale_detail
 where type in ('0','1')
   and user_type = 1
   and sale_type = 0
 group by zone_name,sale_id,pstng_date,type,tax 
) as a
join fin_certificate_rv_detail n on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area;



drop table fin_certificate_rv_detail_5;
create table fin_certificate_rv_detail_5 as
select a.zone_name,
        a.receipt_id,
        a.tax_rate,
        f.doc_type,
        f.dctype,
        f.gl_account doc_number,
        f.dd,
        f.project,
        f.project_cate,
        (round( (s_money*(all_money-money)*(a.tax_rate/100)/(all_money-dx))/(1+a.tax_rate/100) ,2))*f.pn money,
        concat_ws(' ',a.pstng_date,item_text) text,
        a.pstng_date,
        a.flag,
        '' as customer,
        '' as vendor,
        '1' status
from (
select zone_name,pstng_date,sale_id as receipt_id,
       case when type = 0 or type = 1 then tax else 0 end tax_rate,
       concat(case when type = 0 then 'WUMART' 
                   when type = 1 then 'JISHOU' 
                   --when type = 2 then 'DAIXIAO'
                   end,
              '_',
              case when type = 0 or type = 1 then cast(tax as int) else 0 end,'_ZK') flag,
       sum(price*qty) s_money
  from temp_finance_sale_detail
 where type in ('0','1')
   and user_type = 1
   and sale_type = 0
 group by zone_name,sale_id,pstng_date,type,tax 
) as a
join fin_certificate_rv_detail n on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area;




drop table fin_certificate_rv_detail_7;
create table fin_certificate_rv_detail_7 as
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_rv_detail_1
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_rv_detail_2
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_rv_detail_3
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_rv_detail_4
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_rv_detail_5;




drop table fin_certificate_rv;
create table fin_certificate_rv as
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,'1' status,
    unix_timestamp() created_at,unix_timestamp() updated_at
from (
    select zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,f.customer as customer,'' as vendor,f.dd,
           case when f.gl_account = '6001010101' or f.gl_account = '6001010203' then '0101' else '' end as project,
           case when f.gl_account = '6001010101' or f.gl_account = '6001010203' then '01' else '' end as project_cate,
           money,concat_ws(' ',pstng_date,f.item_text) text,pstng_date
    from (
        SELECT zone_name,receipt_id,pstng_date,
            --case when sub_money>0 then 'RV_D' else 'RV_C' end flag,
            'RV_D' flag,
            sub_money money

        from (
            select zone_name,receipt_id,pstng_date,
                round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
            from fin_certificate_rv_detail_7
            group by zone_name,receipt_id,pstng_date
        ) a
        where round(sub_money,2)<>0
    ) a
    join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
    from fin_certificate_rv_detail_7
) b ;




drop table temp_fin_rv;
create table temp_fin_rv as 
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
  from fin_certificate_rv
 group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
       text,customer,vendor,status,created_at,updated_at


select zone_name,doc_number,sum(money) from fin_certificate_rv 
 group by zone_name,doc_number
 order by zone_name asc,doc_number asc

hive -e "
select zone_name,receipt_id,tax_rate, doc_type, dctype, doc_number, dd, project, 
       project_cate, money, text, pstng_date, customer, vendor, status, created_at, updated_at 
  from temp_fin_rv
" > /home/inf/zhaoning/u8/data/201707/rv_20170701_31.txt
--------------------------------------------------------------------------------------------
ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2 
--------------------------------------------------------------------------------------------

drop table temp_finance_certificate_zv2_zn_1;
create table temp_finance_certificate_zv2_zn_1 as
select a.zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
       a.customer,a.vendor,f.dd,money*f.pn as money,concat_ws(' ',pstng_date,item_text) as text,pstng_date
from (
--主营业务成本\外部\物美集采\订单金额 主营业务成本\外部\链商寄售\订单金额
select zone_name,receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       flag
from 
(
select zone_name,sale_id as receipt_id,average_price*real_qty money,tax as tax_rate,pstng_date,
       concat('ZV2_',case when type = 0 then 'WUMART' 
                          when type = 1 then 'JISHOU' end) as flag
  from temp_finance_sale_detail
 where type in (0,1)
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
   and user_type = 1
   and sale_type = 0
) as a  
group by zone_name,receipt_id,pstng_date,flag

union all
--库存商品\物美集采 库存商品\链商寄售
select zone_name,receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       flag
from 
(
select zone_name,sale_id as receipt_id,average_price*real_qty money,tax as tax_rate,pstng_date,
       concat('ZV2_KC_',case when type = 0 then 'WUMART' 
                             when type = 1 then 'JISHOU' end) as flag
  from temp_finance_sale_detail
 where type in (0,1)
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
   and user_type = 1
   and sale_type = 0
) as a  
group by zone_name,receipt_id,pstng_date,flag
) as a
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and zone_name=f.area;



drop table temp_finance_certificate_zv2_zn_2;
create table temp_finance_certificate_zv2_zn_2 as
select a.zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
       a.customer,a.vendor,f.dd,money*f.pn as money,concat_ws(' ',pstng_date,item_text) as text,pstng_date
from (

--主营业务成本\外部\物美集采\销售退回 主营业务成本\外部\链商寄售\销售退回
select a.zone_name,b.second_receipt_id as receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       concat('ZV2_SH_',case when type = 0 then 'WUMART' 
                             when type = 1 then 'JISHOU' end) as flag
  from 
(
select zone_id,zone_name,order_id,afs_id,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date
  from mds_pss_mall_sale_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-01'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
   and user_type = 1
   and afs_id <> 0
) as a
join 
(select zone_id,zone_name,return_id,second_receipt_id,average_price*qty money,tax as tax_rate,type
   from temp_finance_return_detail
  where type in (0,1)
) as b
on (a.zone_id = b.zone_id and a.afs_id = b.return_id)
group by a.zone_name,b.second_receipt_id,a.pstng_date,b.type
union all
-- 库存商品\物美集采退回 库存商品\链商寄售退回
select a.zone_name,b.second_receipt_id as receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       concat('ZV2_SH_KC_',case when type = 0 then 'WUMART' 
                                when type = 1 then 'JISHOU' end) as flag
  from 
(
select zone_id,zone_name,order_id,afs_id,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date
  from mds_pss_mall_sale_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-01'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
   and user_type = 1
   and afs_id <> 0

) as a
join 
(select zone_id,zone_name,return_id,second_receipt_id,average_price*qty money,tax as tax_rate,type
   from temp_finance_return_detail
  where type in (0,1)
) as b
on (a.zone_id = b.zone_id and a.afs_id = b.return_id)
group by a.zone_name,b.second_receipt_id,a.pstng_date,b.type
) as a
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and zone_name=f.area;



--生成最终的凭证数据
drop table temp_finance_certificate_zv2_zn;
create table temp_finance_certificate_zv2_zn as
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,dd,'' as project,'' as project_cate,money,text,pstng_date,customer,vendor,'1' status,
    unix_timestamp() created_at,unix_timestamp() updated_at
from (
    select zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,
           '' as customer,'' as vendor,f.dd,money,concat_ws(' ',pstng_date,item_text) text,pstng_date
    from (
        SELECT zone_name,receipt_id,pstng_date,
            case when sub_money>0 then 'ZV2_D' else 'ZV2_C' end flag,
            abs(sub_money) money
        from (
            select zone_name,receipt_id,pstng_date,
                round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
            from temp_finance_certificate_zv2_zn_1
            group by zone_name,receipt_id,pstng_date
            union all
            select zone_name,receipt_id,pstng_date,
                round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
            from temp_finance_certificate_zv2_zn_2
            group by zone_name,receipt_id,pstng_date
        ) a
        where round(sub_money,2)<>0
    ) a
    join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,money,text,pstng_date
    from temp_finance_certificate_zv2_zn_1
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,money,text,pstng_date
    from temp_finance_certificate_zv2_zn_2
) b ;

select zone_name,doc_number,sum(money) from temp_finance_certificate_zv2_zn group by zone_name,doc_number;

select zone_name,doc_number,sum(money) from fin_certificate_rv group by zone_name,doc_number;


drop table temp_fin_zv2;
create table temp_fin_zv2 as 
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
  from temp_finance_certificate_zv2_zn_new_2
 group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
       text,
       customer,vendor,status,created_at,updated_at


hive -e "
select zone_name,receipt_id,tax_rate, doc_type, dctype, doc_number, dd, project, 
       project_cate, money, text, pstng_date, customer, vendor, status, created_at, updated_at 
  from temp_fin_zv2
" > /home/inf/zhaoning/u8/data/201707/zv2_20170701_31.txt


####################################################################################
####################################################################################
################################返仓数据的凭证写入####################################
####################################################################################
####################################################################################


drop table temp_finance_certificate_re_fc_zn_1;
create table temp_finance_certificate_re_fc_zn_1 as
select zone_name,order_id as receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,'' customer,vendor,f.dd,money,concat_ws(' ',pstng_date,item_text) text,pstng_date
from (
--库存商品\物美集采
select zone_name,order_id,from_unixtime(cast(date as int),'yyyy-MM-dd') pstng_date,nt_amount as money,'FC_RE_WUMART' as flag,'' vendor
  from mds_pss_wm_return_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-30'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
union all
--其他应收款\待摊费用\待抵扣进项税
select zone_name,order_id,from_unixtime(cast(date as int),'yyyy-MM-dd') pstng_date,it_amount - nt_amount as money,'FC_RE_TAX_WUMART' as flag,'' vendor
  from mds_pss_wm_return_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-30'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
union all
--应付账款\商品采购款\物美集采
select zone_name,order_id,from_unixtime(cast(date as int),'yyyy-MM-dd') pstng_date,it_amount as money,'FC_RE_GY' as flag,
       case when zone_id = '1000' then '01010001' 
            when zone_id = '1001' then '02010001'
            when zone_id = '1002' then '03010001'
            end 
       as vendor
  from mds_pss_wm_return_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-30'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
) as a
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and zone_name=f.area;


drop table temp_finance_certificate_re_fc_zn;
create table temp_finance_certificate_re_fc_zn as
select zone_name,receipt_id,tax_rate,'RE_F' doc_type,dctype,doc_number,dd,'' as project,'' as project_cate,money,text,pstng_date,customer,vendor,'1' status,
    unix_timestamp() created_at,unix_timestamp() updated_at
from (
    select zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,'' as customer ,'' as vendor,f.dd,money,concat_ws(' ',pstng_date,'物美退货') text,pstng_date
    from (
        SELECT zone_name,receipt_id,pstng_date,
            case when sub_money>0 then 'FC_RE_D' else 'FC_RE_C' end flag,
            abs(sub_money) money
        from (
            select zone_name,receipt_id,pstng_date,
                round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
            from temp_finance_certificate_re_fc_zn_1
            group by zone_name,receipt_id,pstng_date
        ) a
        where round(sub_money,2)<>0
    ) a
    join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,money,text,pstng_date
    from temp_finance_certificate_re_fc_zn_1
) b ;


create table temp_fin_re_f as 
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
  from temp_finance_certificate_re_fc_zn
 group by zone_name,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,text,customer,vendor,status,created_at,updated_at



hive -e "
select zone_name,receipt_id,tax_rate, doc_type, dctype, doc_number, dd, project, 
       project_cate, money, text, pstng_date, customer, vendor, status, created_at, updated_at 
  from temp_fin_re_f
" > /home/inf/zhaoning/u8/data/201707/re_f_20170701_31.txt


-- 承运商承担 收入
-- RV 寄售和物美的商品未税售价
drop table temp_finance_certificate_rv_fc_zn_1;
create table temp_finance_certificate_rv_fc_zn_1 as
select a.zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,a.customer,'' vendor,f.dd,f.project,f.project_cate,money,concat_ws(' ',pstng_date,item_text)
text,pstng_date
from (
select zone_name,receipt_id,round(sum(price*qty/(1+tax_rate/100)),2) money,pstng_date,flag,'' as customer
  from 
(
select zone_name,sale_id as receipt_id,price,qty,tax as tax_rate,pstng_date ,
       concat('FC_NO',case when type = 0 then 'WUMART' 
                           when type = 1 then 'JISHOU' 
                           end,
              '_',
              cast(tax as int) 
             ) as flag,
       '' customer
  from temp_finance_sale_detail
 where type in (0,1)
   and user_type = 3
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
) as a
group by zone_name,receipt_id,pstng_date,flag
union all
select zone_name,sale_id as receipt_id,round(sum(price*qty/(1+tax/100)),2) money,pstng_date,'FC_DAIXIAO' flag,'' as customer
  from temp_finance_sale_detail
 where type = 2
   and user_type = 3
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
group by zone_name,sale_id,pstng_date
union all
select zone_name,receipt_id,round(sum(price*qty*(tax_rate/100)/(1+tax_rate/100)),2) money,pstng_date,flag,'' as customer
  from 
(
select zone_name,sale_id as receipt_id,price,qty,tax as tax_rate,pstng_date,
       concat('FC_',case when type = 0 then 'WUMART' 
                         when type = 1 then 'JISHOU' 
                         end,
              '_',
              cast(tax as int) 
             ) as flag
  from temp_finance_sale_detail
 where type in (0,1)
   and user_type = 3
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
) as a
group by zone_name,receipt_id,pstng_date,flag   
union all
select zone_name,sale_id as receipt_id,final_amount money,
       from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,'FC_RV_CY' flag,fin_code as customer
  from mds_pss_mall_sale_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-01'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
   and user_type = 3
) as a
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and zone_name=f.area;



drop table temp_finance_certificate_rv_fc_zn;
create table temp_finance_certificate_rv_fc_zn as
select zone_name,receipt_id,tax_rate, doc_type,dctype,doc_number,dd,money,text,pstng_date,customer,vendor,project,project_cate,'1' status,
    unix_timestamp() created_at,unix_timestamp() updated_at
from (
    select zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,
           case when zone_name = '北京' then '01010001' 
                when zone_name = '天津' then '02010001'
                when zone_name = '杭州' then '03010003' 
                end as customer,
           '' vendor,f.dd,f.project,f.project_cate,money,
           concat_ws(' ',pstng_date,item_text) text,pstng_date
    from (
        SELECT zone_name,receipt_id,pstng_date,
               --'FC_RV_CY' flag,
               case when sub_money>0 and zone_name = '北京' then 'FC_NOWUMART_17' 
                    when sub_money>0 and zone_name = '天津' then 'FC_NOWUMART_17' 
                    when sub_money>0 and zone_name = '杭州' then 'FC_NOJISHOU_17'
               else 'FC_RV_CY' 
               end flag,
               abs(sub_money) money
        from (
            select zone_name,receipt_id,pstng_date,
                round(round(sum(case when dctype='C' then money else 0 end),2) - abs(round(sum(case when dctype='D' then money else 0 end),2)),2) sub_money
            from temp_finance_certificate_rv_fc_zn_1
            group by zone_name,receipt_id,pstng_date
        ) a
        where round(sub_money,2)<>0
    ) a
    join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,abs(money) money,text,pstng_date
    from temp_finance_certificate_rv_fc_zn_1
) b ;

create table temp_fin_rv_f as 
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
  from temp_finance_certificate_rv_fc_zn
 group by zone_name,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,text,customer,vendor,status,created_at,updated_at


select zone_name,doc_type,sum(money) from temp_finance_certificate_rv_fc_zn group by zone_name,doc_type

hive -e "
select zone_name,receipt_id,tax_rate, doc_type, dctype, doc_number, dd, project, 
       project_cate, money, text, pstng_date, customer, vendor, status, created_at, updated_at 
  from temp_fin_rv_f
" > /home/inf/zhaoning/u8/data/201707/rv_f_20170701_31.txt

===============================
--承运商承担 结转成本

drop table temp_finance_certificate_zv2_fc_zn_1;
create table temp_finance_certificate_zv2_fc_zn_1 as
select zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
       '' customer,'' as vendor,f.dd,money,concat_ws(' ',pstng_date,item_text) text,pstng_date
from (
select zone_name,receipt_id,round(sum(price*qty/(1+tax_rate/100)),2) money,pstng_date,flag
  from 
(
select zone_name,sale_id as receipt_id,price,qty,tax as tax_rate,pstng_date,
       concat('FC_ZV2_',case when type = 0 then 'WUMART' 
                             when type = 1 then 'JISHOU' 
                             end) as flag
  from temp_finance_sale_detail
 where type in (0,1)
   and user_type = 3
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
) as a
group by zone_name,receipt_id,pstng_date,flag
union all
select zone_name,receipt_id,round(sum(price*qty/(1+tax_rate/100)),2) money,pstng_date,flag
  from 
(
select zone_name,sale_id as receipt_id,price,qty,tax as tax_rate,pstng_date,
       concat('FC_ZV2_KC_',case when type = 0 then 'WUMART' 
                                when type = 1 then 'JISHOU' 
                                end) as flag
  from temp_finance_sale_detail
 where type in (0,1)
   and user_type = 3
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
) as a
group by zone_name,receipt_id,pstng_date,flag
) a
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and zone_name=f.area;



drop table temp_finance_certificate_zv2_fc_zn_cy;
create table temp_finance_certificate_zv2_fc_zn_cy as
select zone_name,receipt_id,tax_rate, doc_type,dctype,doc_number,dd,'' as project,'' as project_cate, money,text,pstng_date,nvl(customer,'') as customer,vendor,'1' status,
    unix_timestamp() created_at,unix_timestamp() updated_at
from (
    select zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,
           '' customer,'' as vendor,f.dd,money,concat_ws(' ',pstng_date,item_text) text,pstng_date
    from (
        SELECT zone_name,receipt_id,pstng_date,
            case when sub_money>0 then 'FC_ZV2_D' else 'FC_ZV2_C' end flag,
            abs(sub_money) money
        from (
            select zone_name,receipt_id,pstng_date,
                round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
            from temp_finance_certificate_zv2_fc_zn_1
            group by zone_name,receipt_id,pstng_date
        ) a
        where round(sub_money,2)<>0
    ) a
    join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,money,text,pstng_date
    from temp_finance_certificate_zv2_fc_zn_1
) b ;

drop table temp_fin_zv2_cy;
create table temp_fin_zv2_cy as 
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
  from temp_finance_certificate_zv2_fc_zn_cy
 group by zone_name,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,text,customer,vendor,status,created_at,updated_at



hive -e "
select zone_name,receipt_id,tax_rate, doc_type, dctype, doc_number, dd, project, 
       project_cate, money, text, pstng_date, customer, vendor, status, created_at, updated_at 
  from temp_fin_zv2_cy
" > /home/inf/zhaoning/u8/data/201707/zv2_cy_20170701_31.txt

--链商承担

drop table temp_fin_zv2_ls;
create table temp_fin_zv2_ls as 
select zone_name,'' as receipt_id,'' tax_rate,'ZV2_LS' doc_type,'C' dctype,'6401010101' as doc_number,'' as project,
       '' as project_cate,sum(nt_amount) money,concat(pstng_date,'链商承担成本') text,pstng_date,'' customer,'' vendor,
       '1' status,unix_timestamp() created_at,unix_timestamp() updated_at
  from test_lsh
group by zone_name,pstng_date
union all
select zone_name,'' as receipt_id,'' tax_rate,'ZV2_LS' doc_type,'D' dctype,'140501' as doc_number,'' as project,
       '' as project_cate,sum(nt_amount) money,concat(pstng_date,'链商承担成本') text,pstng_date,'' customer,'' vendor,
       '1' status,unix_timestamp() created_at,unix_timestamp() updated_at
  from test_lsh
group by zone_name,pstng_date


hive -e "
select zone_name,receipt_id,tax_rate, doc_type, dctype, doc_number,'010202' dd, project, 
       project_cate, money, text, pstng_date, customer, vendor, status, created_at, updated_at 
  from temp_fin_zv2_ls
" > /home/inf/zhaoning/u8/data/201707/zv2_ls_20170701_31.txt

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################


hive -e "
select 
name, waybill_no, tax_rate, dctype, cd_type, doc_number, dd, project, project_cate, money, text, 
pstng_date, customer, vendor, status, created_at, updated_at
  from mds_fin_voucher_result
 where dt = '2017-06-30' 
   and pstng_date >= '2017-06-28'
   and pstng_date <= '2017-06-30'
" > /home/inf/zhaoning/u8/data/201706/result_all_0628_30.txt
 

/home/inf/zhaoning/u8/data/201707/re_f_20170701_29.txt
/home/inf/zhaoning/u8/data/201707/re_20170701_29.txt

mysql -h192.168.60.49 -uroot -proot123 -P3332 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '/home/inf/zhaoning/u8/data/201707/re_20170701_29.txt' REPLACE INTO TABLE dt_db.fin_voucher_yongyou_07(name,waybill_no,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
"





mysql -h192.168.60.59 -uroot -P5200 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '/home/inf/zhaoning/u8/dimension/dim_fin_certificate_info_u8_new.sql' REPLACE INTO TABLE lsh_vrm_qa.dim_fin_certificate_info_u8 (name, doc_type, dctype, gl_account, dd_name, dd, customer, project, project_cate, pn, alloc_nmbr, tax_rate, amtdoccur, item_text, system, area, flag )
"


mysql -h192.168.70.7 -uvrmdev -pPassvrmdev2017 -P3307 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '/home/inf/zhaoning/u8/dimension/dim_fin_certificate_info_u8_new.sql' REPLACE INTO TABLE lsh_vrm.dim_fin_certificate_info_u8 (name, doc_type, dctype, gl_account, dd_name, dd, customer, project, project_cate, pn, alloc_nmbr, tax_rate, amtdoccur, item_text, system, area, flag )
"





