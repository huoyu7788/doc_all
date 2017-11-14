

create table temp_sale_201710 as 
select id, zone_id, zone_name, sale_id, product_code, product_name, qty, real_qty, price, average_price, nt_price, tax, type, fin_code, warehouse_id, day, created_at, updated_at
from mall_sale_detail where average_price = 0 and tax = 0 and nt_price = 0 and fin_code = ' and day >= '2017-10-30' and day <='2017-10-31'

select * from temp_product_ok;

create table temp_product_ok_bak as 
select * from temp_product_ok

create table temp_mall_sale as 
select id, zone_id, a.zone_name, sale_id, a.product_code as product_code, 
       product_name, qty, real_qty, price, b.average_price as average_price, 
       nt_price, b.tax as tax, b.type as type, b.fin_code as fin_code, warehouse_id, day, created_at, updated_at
  from
(
select id, zone_id, zone_name, sale_id, product_code, product_name, qty, real_qty, price, average_price, nt_price, tax, type, fin_code, warehouse_id, day, created_at, updated_at
from temp_sale_201710 where average_price = 0 and tax = 0 and nt_price = 0 and fin_code = ' and day >= '2017-10-30' and day <='2017-10-31'
) as a
left join 
(select zone_name,product_code,average_price,tax,fin_code,type from temp_product_ok) as b
on (a.zone_name = b.zone_name and a.product_code = b.product_code)

select * from temp_mall_sale where average_price = '

insert into temp_mall_sale
select id, zone_id, zone_name, sale_id, product_code, product_name, qty, real_qty, price, average_price, nt_price, tax, type, fin_code, warehouse_id, day, created_at, updated_at
from mall_sale_detail where  fin_code <> ' and day >= '2017-10-30' and day <='2017-10-31'

select * from temp_mall_sale where average_price = 0 and type = 1

select count(*) from temp_mall_sale where day >= '2017-10-30' and day <='2017-10-31'


