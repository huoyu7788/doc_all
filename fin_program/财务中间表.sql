-- drop table temp_ceshi;
-- create table temp_ceshi as 
-- select a.*,b.waybill_id 
--   from 
-- (select zone_name,receipt_id,money,customer,text from temp_finance_certificate_rv_zn_1 where doc_number = '11220202') as a
-- left join
-- (select zone_name,sale_id,waybill_id from mds_pss_mall_sale_head where dt = '2017-07-31') as b 
-- on ( a.receipt_id = b.sale_id)

drop table temp_fin_receipt;
create table temp_fin_receipt as 
select 
  a.zone_id, a.zone_name, a.sale_id, a.product_code, a.product_name, a.qty, 
a.real_qty, a.price, b.average_price, a.nt_price, b.tax, a.type, a.fin_code, 
a.warehouse_id, a.day, a.created_at, a.updated_at, a.sale_type, a.dt
from 
(
select 
zone_id, zone_name, sale_id, product_code, product_name, qty, 
real_qty, price, average_price, nt_price, tax, type, fin_code, 
warehouse_id, day, created_at, updated_at, sale_type, dt
  from mds_pss_mall_sale_detail
 where dt = '2017-07-02'
   and average_price is null
) as a 
left join
(
select wmcode, zone_id, min(nt_mvp) average_price, dt, taxrate tax 
  from mds_pss_his_mvavgpri_day 
 where nt_mvp is not null 
   and dt >= '2017-07-01' and dt <= '2017-07-31' 
 group by wmcode, zone_id, taxrate, dt, nt_mvp
) as b
on (a.day = b.dt and a.zone_id = b.zone_id and a.product_code = b.wmcode)
union all
select 
zone_id, zone_name, sale_id, product_code, product_name, qty, 
real_qty, price, average_price, nt_price, tax, type, fin_code, 
warehouse_id, day, created_at, updated_at, sale_type, dt
  from mds_pss_mall_sale_detail
 where dt = '2017-07-02'
   and average_price is not null



--签收
drop table temp_finance_sale_detail;
create table temp_finance_sale_detail as 
select a.zone_id,
       a.zone_name,
       a.order_id,
       a.sale_id,
       a.final_amount,
       a.pstng_date,
       a.user_type,
       a.fin_code as customer,
       b.product_code,
       b.product_name,
       b.price,
       b.qty,
       b.real_qty,
       b.tax,
       b.average_price,
       b.nt_price,
       b.type,
       b.fin_code as vendor,
       b.sale_type
  from 
(
select zone_id,zone_name,sale_id,order_id,final_amount,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,user_type,fin_code
  from mds_pss_mall_sale_head
 where dt = '2017-07-31'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') >= '2017-07-01'
   and from_unixtime(cast(date as int),'yyyy-MM-dd') <= '2017-07-31'

  ) as a 
join 
(select zone_id,sale_id,product_code,product_name,price,qty,real_qty,average_price,nt_price,tax,type,fin_code,sale_type
   from temp_fin_receipt
  --where sale_type = 0
  --where dt = '2017-07-01'
) as b
on (a.zone_id = b.zone_id and a.sale_id = b.sale_id)


--售后
drop table temp_finance_return_detail;
create table temp_finance_return_detail as 
select a.zone_id,a.zone_name,a.return_id,a.receipt_id,a.second_receipt_id,a.it_amount,a.nt_amount,
       b.product_code,b.product_name,b.qty,b.sale_price,b.average_price,b.tax,b.type,b.fin_code as vendor,a.pstng_date
  from
(
select zone_id,
       zone_id as zone_name,
       return_id,receipt_id,second_receipt_id,it_amount,nt_amount,from_unixtime(cast(return_date as int),'yyyy-MM-dd') as pstng_date
  from mds_pss_mall_return_head
 where dt = '2017-07-31'
   and from_unixtime(cast(return_date as int),'yyyy-MM-dd') >= '2017-07-01'
   and from_unixtime(cast(return_date as int),'yyyy-MM-dd') <= '2017-07-31'
) as a
join
(
select zone_id,
       zone_id as zone_name,
       return_id,product_code,product_name,sale_price,average_price,qty,tax,type,fin_code
  from mds_pss_mall_return_detail
 where dt = '2017-07-31'
) as b
on (a.zone_id = b.zone_id and a.return_id = b.return_id)




---------------------------------------------------------------------
insert into mds_fin_sale_detail
select a.zone_id,
       a.zone_name,
       a.order_id,
       a.sale_id,
       a.final_amount,
       a.user_type,
       a.fin_code as customer,
       b.product_code,
       b.product_name,
       b.price,
       b.qty,
       b.real_qty,
       b.tax,
       b.average_price,
       b.nt_price,
       b.type,
       b.fin_code as vendor,
       a.pstng_date
  from 
(
select zone_id,zone_name,sale_id,order_id,final_amount,day as pstng_date,user_type,fin_code
  from mall_sale_head
 where day >= '2017-07-01'
   and day <= '2017-07-10'

  ) as a 
join 
(select zone_id,sale_id,product_code,product_name,price,qty,real_qty,average_price,nt_price,tax,type,fin_code
   from mall_sale_detail
  where day >= '2017-07-01'
    and day <= '2017-07-10'
) as b
on (a.zone_id = b.zone_id and a.sale_id = b.sale_id);


insert into mds_fin_return_detail
select a.zone_id,a.zone_name,a.return_id,a.receipt_id,a.second_receipt_id,a.it_amount,a.nt_amount,
       b.product_code,b.product_name,b.qty,b.real_qty,b.sale_price,b.average_price,b.tax,b.type,b.fin_code as vendor,a.pstng_date
  from
(
select zone_id,
       zone_id as zone_name,
       return_id,receipt_id,second_receipt_id,it_amount,nt_amount,day as pstng_date
  from mall_return_head
 where day >= '2017-07-01'
   and day <= '2017-07-10'
) as a
join
(
select zone_id,
       zone_id as zone_name,
       return_id,product_code,product_name,sale_price,average_price,qty,real_qty,tax,type,fin_code
  from mall_return_detail
 where day >= '2017-07-01'
   and day <= '2017-07-10'
) as b
on (a.zone_id = b.zone_id and a.return_id = b.return_id)
