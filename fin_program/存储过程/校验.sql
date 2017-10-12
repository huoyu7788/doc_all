--fin_voucher
select 'RE',name,sum(money) 
  from fin_voucher 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'RE' 
   and doc_number = '22020101'
group by name
union all
select 'RE_F',name,sum(money) 
  from fin_voucher 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'RE_F' 
   and doc_number = '22020101'
group by name
union all
select 'JISHOU_RE',name,sum(money) 
  from fin_voucher 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'JISHOU_RE' 
   and doc_number = '22020102'
group by name
union all
select 'RV',name,sum(money) 
  from fin_voucher 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'RV'
   and money > 0
   and doc_number in ('224103','6001010101','6001010201','2221010101','2221010104')
group by name
union all
select 'KA_RV',name,sum(money) 
  from fin_voucher 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'KA_RV'
   and money > 0
   and doc_number in ('224103','6001010101','6001010201','2221010101','2221010104')
group by name
union all
select 'RV_F',name,sum(money) 
  from fin_voucher 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'RV_F'
   and doc_number in ('224103','6001010101','6001010201','2221010101','2221010104')
group by name


select case when dctype = 'BR1' then '微信扫码'
            when dctype = 'BR2' then '微信APP'
            when dctype = 'BR4' then '支付宝'
            when dctype = 'BR17' then '支付宝扫码'
            when dctype = 'BR13' then '拉卡拉'
       end as type,
       sum(money)
  from fin_voucher 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and receipt_id = '0102'
 group by dctype

--fin_certificate_result

select 'RE',name,sum(money) 
  from fin_certificate_result 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'RE' 
   and doc_number = '22020101'
group by name
union all
select 'RE_F',name,sum(money) 
  from fin_certificate_result 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'RE_F' 
   and doc_number = '22020101'
group by name
union all
select 'JISHOU_RE',name,sum(money) 
  from fin_certificate_result 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'JISHOU_RE' 
   and doc_number = '22020102'
group by name
union all
select 'RV',name,sum(money) 
  from fin_certificate_result 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'RV'
   and money > 0
   and doc_number in ('224103','6001010101','6001010201','2221010101','2221010104')
group by name
union all
select 'KA_RV',name,sum(money) 
  from fin_certificate_result 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'KA_RV'
   and money > 0
   and doc_number in ('224103','6001010101','6001010201','2221010101','2221010104')
group by name
union all
select 'RV_F',name,sum(money) 
  from fin_certificate_result 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and dctype = 'RV_F'
   and doc_number in ('224103','6001010101','6001010201','2221010101','2221010104')
group by name


select case when dctype = 'BR1' then '微信扫码'
            when dctype = 'BR2' then '微信APP'
            when dctype = 'BR4' then '支付宝'
            when dctype = 'BR17' then '支付宝扫码'
            when dctype = 'BR13' then '拉卡拉'
       end as type,
       sum(money)
  from fin_certificate_result 
 where pstng_date >= '2017-09-26' 
   and pstng_date <= '2017-09-26' 
   and receipt_id = '0102'
 group by dctype

