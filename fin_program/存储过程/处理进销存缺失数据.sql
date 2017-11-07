
truncate table mds_fin_sale_detail;
insert into mds_fin_sale_detail 
select a.zone_id,a.zone_name,a.order_id,a.sale_id,a.pay_type,a.final_amount,a.afs_amount,a.user_type,
      a.fin_code as customer,b.product_code,b.product_name,b.price,b.qty,b.real_qty,b.tax,
      b.average_price,b.nt_price,b.type,b.fin_code as vendor,a.user_fin_code,a.pstng_date,a.provider
 from 
      (select zone_id,zone_name,sale_id,order_id,pay_type,final_amount,day as pstng_date,user_type,fin_code,user_fin_code,day,afs_amount,provider
         from mall_sale_head
        where day >= '2017-10-01'                       
          and day <= '2017-10-31'
          and sale_id in 
          (4510826672451313552,558495769320590776,5760997800476081878,2972002996883912405,4716303427080116878,6484529231559266603) 
      ) as a 
      join 
      (select a.zone_id,a.sale_id,a.product_code,a.product_name,a.price,a.qty,a.real_qty,a.average_price,a.nt_price,
              a.tax,a.type,a.fin_code
         from mall_sale_detail as a 
        where day >= '2017-10-01'                       
          and day <= '2017-10-31'
          and not exists(select zone_id,sale_id 
                          from mall_sale_detail as b 
                         where day >= '2017-10-01'                       
                           and day <= '2017-10-31'
                           and (fin_code = '' or fin_code is null)
                           and type = 2
                           and a.sale_id = b.sale_id 
                           and a.zone_id = b.zone_id)
      ) as b
      on (a.zone_id = b.zone_id and a.sale_id = b.sale_id);

truncate table mds_fin_return_detail;
insert into mds_fin_return_detail 
       select a.zone_id,a.zone_name,a.return_id,a.receipt_id,a.second_receipt_id,a.it_amount,a.nt_amount,a.user_type,
            b.product_code,b.product_name,b.qty,b.real_qty,b.sale_price,b.average_price,b.tax,b.type,b.fin_code as vendor,a.pstng_date
       from (select zone_id,zone_name,return_id,receipt_id,second_receipt_id,it_amount,user_type,
                    nt_amount,day as pstng_date
               from mall_return_head
              where day >= '2017-10-01'                       
                and day <= '2017-10-31'
                and second_receipt_id in 
                 (4510826672451313552,558495769320590776,5760997800476081878,2972002996883912405,4716303427080116878,6484529231559266603) 
            ) as a
            join
            (select zone_id,zone_name,return_id,product_code,product_name,sale_price,
                    average_price,qty,real_qty,tax,type,fin_code
               from mall_return_detail as a
              where day >= '2017-10-01'                       
                and day <= '2017-10-31'
                and type in (0,1,2,3)
                and not exists(select zone_id,return_id 
                                from mall_return_detail as b 
                               where day >= '2017-10-01'                       
                                 and day <= '2017-10-31'
                                 and (fin_code = '' or fin_code is null)
                                 and type = 2
                                 and a.return_id = b.return_id 
                                 and a.zone_id = b.zone_id)
            ) as b
            on (a.zone_id = b.zone_id and a.return_id = b.return_id);







insert into mds_fin_vss_detail
select a.zone_id,a.zone_name,a.order_id,a.it_amount,a.nt_amount,b.vendor,b.vendor_name,b.type,a.pstng_date
  from 
(
select zone_id,
       case when zone_id = '1000' then '北京'
            when zone_id = '1001' then '天津'
            when zone_id = '1002' then '杭州'
            end zone_name,
       vid,reference as order_id,it_amount,nt_amount,from_unixtime(order_date,'%Y-%m-%d') as pstng_date
  from receipt_head
 where from_unixtime(order_date,'%Y-%m-%d') >= @pstng_date_start
   and from_unixtime(order_date,'%Y-%m-%d') <= @pstng_date_end
union all
select zone_id,
       case when zone_id = '1000' then '北京'
            when zone_id = '1001' then '天津'
            when zone_id = '1002' then '杭州'
            end zone_name,
       vid,return_id as order_id,it_amount*-1,nt_amount*-1,from_unixtime(date,'%Y-%m-%d') as pstng_date
  from return_head
 where from_unixtime(date,'%Y-%m-%d') >= @pstng_date_start
   and from_unixtime(date,'%Y-%m-%d') <= @pstng_date_end
) as a
left join
(select vid,type,fin_code as vendor,name as vendor_name from vender_main) as b 
on (a.vid = b.vid)



--导入寄售入库
mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '/home/inf/zhaoning/u8/data/201709/jishou_re_20170921.sql' REPLACE INTO TABLE lsh_vrm.mds_fin_vss_detail(zone_id,zone_name,order_id,it_amount,nt_amount,vendor,vendor_name,type,pstng_date)
"



call fin_voucher_rv_modify('','',@rv);
call fin_voucher_zv2_modify('','',@zv2);

call fin_voucher_ka_rv_modify('','',@rv);
call fin_voucher_ka_zv2_modify('','',@zv2);

call fin_voucher_jishou_re_modify('','',@jishou_re_num);

truncate table fin_certificate_result



