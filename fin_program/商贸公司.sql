--201612月份的商贸公司数据

drop table temp_finance_certificate_bsds_dr_zn_1;
create table temp_finance_certificate_bsds_dr_zn_1 as 
select zone_name,sale_id as receipt_id,'' tax_rate,'DR' doc_type,'C' dctype,'224103' doc_number,'' dd,'' as project,'' as project_cate,
    round(sum(price*qty),2) money,
    '商贸公司' text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,'01020007' customer, '' vendor
from temp_finance_sale_detail a
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd')

union all 
select zone_name,sale_id as receipt_id,'' tax_rate,'DR' doc_type,'D' dctype,'22410205' doc_number,'' dd,'' as project,'' as project_cate,
    round(sum(price*qty)*0.9,2) money,
    '商贸公司' text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,'' customer, '' vendor
from temp_finance_sale_detail a
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd')

union all 
select zone_name,sale_id as receipt_id,'' tax_rate,'DR' doc_type,'D' dctype,'60510101' doc_number,'010202' dd,'' as project,'' as project_cate,
    round(sum(price*qty)*0.1,2) money,
    '商贸公司'text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,'' customer, '' vendor
from temp_finance_sale_detail a
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd')



-- drop table temp_finance_certificate_bsds_dr_zn_1;
-- create table temp_finance_certificate_bsds_dr_zn_1 as 
-- select zone_name,waybill_no,'' tax_rate,'DR' doc_type,'C' dctype,'224103' doc_number,'' dd,'' as project,'' as project_cate,
--     round(sum(origin_price*qty),2) money,
--     '商贸公司'text,to_date(from_unixtime(cast(actived_at as int))) pstng_date,'01020007' customer, '' vendor
-- from mds_fin_receipt_details_month a
-- where day>='2017-03-01' and day<='2017-03-31' and dt='2017-03-31'
--     and fin_code='01020007'
-- group by zone_name,waybill_no,to_date(from_unixtime(cast(actived_at as int)))

-- union all 
-- select zone_name,waybill_no,'' tax_rate,'DR' doc_type,'D' dctype,'22410205' doc_number,'' dd,'' as project,'' as project_cate,
--     round(sum(origin_price*qty)*0.9,2) money,
--     '商贸公司' text,to_date(from_unixtime(cast(actived_at as int))) pstng_date,'' customer, '' vendor
-- from mds_fin_receipt_details_month a
-- where day>='2017-03-01' and day<='2017-03-31' and dt='2017-03-31'
--     and fin_code='01020007'
-- group by zone_name,waybill_no,to_date(from_unixtime(cast(actived_at as int)))

-- union all 
-- select zone_name,waybill_no,'' tax_rate,'DR' doc_type,'D' dctype,'60510101' doc_number,'010202' dd,'' as project,'' as project_cate,
--     round(sum(origin_price*qty)*0.1,2) money,
--     '商贸公司'text,to_date(from_unixtime(cast(actived_at as int))) pstng_date,'' customer, '' vendor
-- from mds_fin_receipt_details_month a
-- where day>='2017-03-01' and day<='2017-03-31' and dt='2017-03-31'
--     and fin_code='01020007'
-- group by zone_name,waybill_no,to_date(from_unixtime(cast(actived_at as int)));

select pstng_date,zone_name,waybill_no,sub_money
from (
    select zone_name,waybill_no,pstng_date,
        round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
    from temp_finance_certificate_bsds_dr_zn_1
    group by zone_name,waybill_no,pstng_date
) a
where abs(round(sub_money,2))<>0
order by sub_money;


hive -e "
select zone_name,waybill_no,tax_rate, doc_type,dctype,doc_number,dd,project,project_cate,money,text,'2017-04-02' pstng_date,
       customer,vendor,'1' status,unix_timestamp() created_at,unix_timestamp() updated_at
from temp_finance_certificate_bsds_dr_zn_1 
where pstng_date>='2017-03-01'
  and pstng_date<='2017-03-31'
" > /home/inf/zhaoning/u8/data/201703/bsds_dr_20170301_31.txt 


--销售RV凭证
drop table temp_finance_certificate_bsds_sm_rv_zn_1;
create table temp_finance_certificate_bsds_sm_rv_zn_1 as 
select zone_name,sale_id as receipt_id,'' tax_rate,'BSDS_RV' doc_type,'C' dctype,'12210302' doc_number,'' dd,'' as project,
       '' as project_cate,
       round(sum(price*qty)*0.9,2) money,
       '商贸公司'text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,
       '' as customer,
       '' as vendor
from temp_finance_sale_detail a
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd')
union all
select zone_name,sale_id as receipt_id,'' tax_rate,'BSDS_RV' doc_type,'C' dctype,'66010402' doc_number,'050202' dd,'0101' as project,
       '01' as project_cate,
       round(sum((price*qty)*0.1)/1.06,2) money,
       '商贸公司'text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,
       '' as customer,
       '' as vendor
from temp_finance_sale_detail a
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd')
union all
select zone_name,waybill_no,'' tax_rate,'BSDS_RV' doc_type,'C' dctype,'12210404' doc_number,'' dd,'' as project,
       '' as project_cate,
       round(sum((price*qty)*0.1)/1.06*0.06,2) money,
       '商贸公司'text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,
       '' as customer,
       '' as vendor
from temp_finance_sale_detail a
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd')

union all 
select zone_name,sale_id as receipt_id,tax as tax_rate,'BSDS_RV' doc_type,'D' dctype,
       '6001010201' doc_number,'050202' dd,
       case when cast(tax as int) = 17 then '0101' 
            when cast(tax as int) = 13 then '0102'
            when cast(tax as int) = 0  then '0103'
       end as project,
       case when cast(tax as int) = 17 then '01' 
            when cast(tax as int) = 13 then '01'
            when cast(tax as int) = 0  then '01'
       end as project_cate,
       round(sum(price*qty/(1+tax/100)),2) money,
       '商贸公司'text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,
       '' as customer,
       '' as vendor
from temp_finance_sale_detail
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd'),tax

union all 
select zone_name,sale_id as receipt_id,tax as tax_rate,'BSDS_RV' doc_type,'D' dctype,
       case when cast(tax as int)=17 then '2221010101' else '2221010102' end doc_number,'' dd,
       '' as project,'' as project_cate,
       round(sum(price*qty*(tax/100)/(1+tax/100)),2) money,
       '商贸公司' text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,
       '' as customer,
       '' as vendor
from temp_finance_sale_detail
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd'),tax,
         case when cast(tax as int)=17 then '2221010101' else '2221010102' end;

select pstng_date,zone_name,waybill_no,sub_money
from (
    select zone_name,waybill_no,pstng_date,
        round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
    from temp_finance_certificate_bsds_sm_rv_zn_1
    group by zone_name,waybill_no,pstng_date
) a
where abs(round(sub_money,2))<>0
order by sub_money;

hive -e "
select zone_name,waybill_no,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,money,text,'2017-04-02' pstng_date,
       customer,vendor,'1' status,unix_timestamp() created_at,unix_timestamp() updated_at
  from temp_finance_certificate_bsds_sm_rv_zn_1 
 where pstng_date>='2017-03-01'
   and pstng_date<='2017-03-31'
" > /home/inf/zhaoning/u8/data/201703/bsds_rv_20170301_0331.txt 


--结转成本zv2凭证
drop table temp_finance_certificate_bsds_zv2_zn_1;
create table temp_finance_certificate_bsds_zv2_zn_1 as 
select zone_name,sale_id as receipt_id,'' tax_rate,'WE' doc_type,'C' dctype,'6401010201' doc_number,'050202' dd,
       round(sum(average_price*qty/(1+tax/100)),2) money,
       '商贸公司'text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,
       '' as customer,
       '' as vendor
from temp_finance_sale_detail
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd')
union all 
select zone_name,sale_id as receipt_id,tax as tax_rate,'WE' doc_type,'D' dctype,'140502' doc_number,'050202' dd,
       round(sum(average_price*qty/(1+tax/100)),2) money,
       '商贸公司'text,from_unixtime(cast(date as int),'yyyy-MM-dd') as pstng_date,
       '' as customer,
       '' as vendor
 from temp_finance_sale_detail
where vendor='01020007'
  and user_type = 1
group by zone_name,sale_id,from_unixtime(cast(date as int),'yyyy-MM-dd'),tax;

select pstng_date,zone_name,waybill_no,sub_money
from (
    select zone_name,waybill_no,pstng_date,
        round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
    from temp_finance_certificate_bsds_zv2_zn_1
    group by zone_name,waybill_no,pstng_date
) a
where abs(round(sub_money,2))<>0
order by sub_money;



hive -e "
select '百世鼎盛'zone_name,waybill_no,tax_rate, doc_type,dctype,doc_number,dd,'' as project,'' as project_cate,money,text,'2017-04-02' pstng_date,
       customer,vendor,'1' status,unix_timestamp() created_at,unix_timestamp() updated_at
 from temp_finance_certificate_bsds_zv2_zn_1 
where pstng_date>='2017-03-01'
  and pstng_date<='2017-03-31'
" > /home/inf/zhaoning/u8/data/201703/bsds_zv2_20170301_0331.txt 



java -jar wumart_sap.jar C092 DR 2017-01-20
java -jar wumart_sap.jar C117 RV 2017-01-20
java -jar wumart_sap.jar C117 WE 2017-01-20

bsds_dr_20170301_31 804
bsds_rv_20170301_0331 1340 
bsds_zv2_20170301_0331 536 









