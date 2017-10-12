--------------------------------------------------------------------------------------------
RVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRVRV
--------------------------------------------------------------------------------------------

drop table temp_finance_certificate_ka_rv_zn_1;
create table temp_finance_certificate_ka_rv_zn_1 as
select a.zone_name,a.receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
       case when f.gl_account = '112203' then a.fin_code else '' end as customer,'' as vendor,f.dd,
       a.money,
       concat_ws(' ',a.pstng_date,f.item_text) text,
       f.project,
       f.project_cate,
       a.pstng_date
  from (
        select zone_name,sale_id as receipt_id,order_id,final_amount as money,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,user_fin_code as fin_code,
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
           and user_type = 2
        ) as a
        join dim_fin_certificate_info_u8_new f on f.flag=TYPE and a.zone_name=f.area;


drop table temp_finance_certificate_ka_rv_zn_2;
create table temp_finance_certificate_ka_rv_zn_2 as
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
   and user_type = 2
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
   and user_type = 2
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
   and user_type = 2
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
   and user_type = 2
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



drop table temp_finance_certificate_ka_rv_zn_3;
create table temp_finance_certificate_ka_rv_zn_3 as
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
       case when zone_id = '1000' and cast(product_code as int) = 444010  then  '01020008'
            when zone_id = '1000' and cast(product_code as int) = 599679  then  '01020002'
            when zone_id = '1000' and cast(product_code as int) = 631267  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 631278  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 631304  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 638648  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 631264  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 631282  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 638652  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 396797  then  '01020001'
            when zone_id = '1000' and cast(product_code as int) = 631265  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 631298  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 631313  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 638653  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 134125  then  '01020010'
            when zone_id = '1000' and cast(product_code as int) = 631284  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 631288  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 631291  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 631295  then  '01020006'
            when zone_id = '1000' and cast(product_code as int) = 638650  then  '01020006'
            when zone_id = '1001' and cast(product_code as int) = 631312  then  '02020006'
            when zone_id = '1001' and cast(product_code as int) = 108600  then  '02020005'
            when zone_id = '1001' and cast(product_code as int) = 631265  then  '02020006'
            when zone_id = '1001' and cast(product_code as int) = 631283  then  '02020006'
            when zone_id = '1001' and cast(product_code as int) = 631295  then  '02020006'
            when zone_id = '1001' and cast(product_code as int) = 631307  then  '02020006'
            when zone_id = '1001' and cast(product_code as int) = 638647  then  '02020006'
            when zone_id = '1001' and cast(product_code as int) = 638648  then  '02020006'
            when zone_id = '1002' and cast(product_code as int) = 10000518 then '03020002'
            else vendor end as vendor
  from temp_finance_sale_detail
 where type = 2
   and pstng_date >= '2017-07-01'
   and pstng_date <= '2017-07-31'
   and user_type = 2
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
       case when a.zone_id = '1000' and cast(product_code as int) = 444010  then  '01020008'
            when a.zone_id = '1000' and cast(product_code as int) = 599679  then  '01020002'
            when a.zone_id = '1000' and cast(product_code as int) = 631267  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 631278  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 631304  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 638648  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 631264  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 631282  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 638652  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 396797  then  '01020001'
            when a.zone_id = '1000' and cast(product_code as int) = 631265  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 631298  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 631313  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 638653  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 134125  then  '01020010'
            when a.zone_id = '1000' and cast(product_code as int) = 631284  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 631288  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 631291  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 631295  then  '01020006'
            when a.zone_id = '1000' and cast(product_code as int) = 638650  then  '01020006'
            when a.zone_id = '1001' and cast(product_code as int) = 631312  then  '02020006'
            when a.zone_id = '1001' and cast(product_code as int) = 108600  then  '02020005'
            when a.zone_id = '1001' and cast(product_code as int) = 631265  then  '02020006'
            when a.zone_id = '1001' and cast(product_code as int) = 631283  then  '02020006'
            when a.zone_id = '1001' and cast(product_code as int) = 631295  then  '02020006'
            when a.zone_id = '1001' and cast(product_code as int) = 631307  then  '02020006'
            when a.zone_id = '1001' and cast(product_code as int) = 638647  then  '02020006'
            when a.zone_id = '1001' and cast(product_code as int) = 638648  then  '02020006'
            when a.zone_id = '1002' and cast(product_code as int) = 10000518 then '03020002'
            else b.vendor end as vendor

  from 
(
select zone_id,zone_name,afs_id,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date
  from mds_pss_mall_sale_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-01'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'
   and user_type = 2
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

drop table temp_finance_zk_data_ka_zn;
create table temp_finance_zk_data_ka_zn as
select zone_name,sale_id as receipt_id,pstng_date,
       sum(price*qty) all_money,max(final_amount) money,
       sum(case when type = '2' then price*qty else 0 end) dx
  from temp_finance_sale_detail
 where user_type = 2
   and sale_type = 0
 group by zone_name,sale_id,pstng_date;

drop table temp_finance_certificate_rv_zn_4;
create table temp_finance_certificate_rv_zn_4 as
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
   and user_type = 2
   and sale_type = 0
 group by zone_name,sale_id,pstng_date,type,tax 
) as a
join temp_finance_zk_data_ka_zn n on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area;



drop table temp_finance_certificate_ka_rv_zn_5;
create table temp_finance_certificate_ka_rv_zn_5 as
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
   and user_type = 2
   and sale_type = 0
 group by zone_name,sale_id,pstng_date,type,tax 
) as a
join temp_finance_zk_data_ka_zn n on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area;




drop table temp_finance_certificate_ka_rv_zn_7;
create table temp_finance_certificate_ka_rv_zn_7 as
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from temp_finance_certificate_ka_rv_zn_1
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from temp_finance_certificate_ka_rv_zn_2
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from temp_finance_certificate_ka_rv_zn_3
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from temp_finance_certificate_rv_zn_4
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from temp_finance_certificate_ka_rv_zn_5;




drop table temp_finance_certificate_ka_rv_zn;
create table temp_finance_certificate_ka_rv_zn as
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
            from temp_finance_certificate_ka_rv_zn_7
            group by zone_name,receipt_id,pstng_date
        ) a
        where round(sub_money,2)<>0
    ) a
    join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
    from temp_finance_certificate_ka_rv_zn_7
) b ;



drop table temp_fin_ka_rv;
create table temp_fin_ka_rv as 
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
  from temp_finance_certificate_ka_rv_zn
 group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
       text,customer,vendor,status,created_at,updated_at


select zone_name,doc_number,sum(money) from temp_finance_certificate_ka_rv_zn group by zone_name,doc_number


hive -e "
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,concat_ws(' ',pstng_date,'物美入库') text,pstng_date,customer,vendor,status,created_at,updated_at
  from temp_fin_ka_rv
 group by zone_name,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,customer,vendor,status,created_at,updated_at
" > /home/inf/zhaoning/u8/data/201707/ka_rv_20170701_31.txt


--------------------------------------------------------------------------------------------
ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2 
--------------------------------------------------------------------------------------------

drop table temp_finance_certificate_ka_zv2_zn_1;
create table temp_finance_certificate_ka_zv2_zn_1 as
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
   and user_type = 2
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
   and user_type = 2
   and sale_type = 0
) as a  
group by zone_name,receipt_id,pstng_date,flag
) as a
join dim_fin_certificate_info_u8_new f on f.flag=a.flag and zone_name=f.area;



drop table temp_finance_certificate_ka_zv2_zn_2;
create table temp_finance_certificate_ka_zv2_zn_2 as
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
   and user_type = 2
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
   and user_type = 2
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
drop table temp_finance_certificate_ka_zv2_zn_new;
create table temp_finance_certificate_ka_zv2_zn_new as
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
            from temp_finance_certificate_ka_zv2_zn_1
            group by zone_name,receipt_id,pstng_date
            union all
            select zone_name,receipt_id,pstng_date,
                round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
            from temp_finance_certificate_ka_zv2_zn_2
            group by zone_name,receipt_id,pstng_date
        ) a
        where round(sub_money,2)<>0
    ) a
    join dim_fin_certificate_info_u8_new f on f.flag=a.flag and a.zone_name=f.area
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,money,text,pstng_date
    from temp_finance_certificate_ka_zv2_zn_1
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,money,text,pstng_date
    from temp_finance_certificate_ka_zv2_zn_2
) b ;


select zone_name,doc_number,sum(money) from temp_finance_certificate_ka_zv2_zn_new group by zone_name,doc_number;


select zone_name,doc_number,sum(money) from temp_finance_certificate_ka_rv_zn group by zone_name,doc_number;



drop table temp_fin_zv2_ka;
create table temp_fin_zv2_ka as 
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
  from temp_finance_certificate_ka_zv2_zn
 group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
       text,
       customer,vendor,status,created_at,updated_at

hive -e "
select zone_name,'' receipt_id,'' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
       sum(money) money,concat_ws(' ',pstng_date,'物美入库') text,pstng_date,customer,vendor,status,created_at,updated_at
  from temp_fin_zv2_ka
 group by zone_name,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,customer,vendor,status,created_at,updated_at
" > /home/inf/zhaoning/u8/data/201707/zv2_ka_20170701_31.txt



select zone_name,doc_number,sum(money) from temp_finance_certificate_ka_zv2_zn group by zone_name,doc_number
