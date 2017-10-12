

DROP PROCEDURE IF EXISTS fin_voucher_data_modify;  
  delimiter //
  create procedure fin_voucher_data_modify 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10) /*结束日期*/
   )
  begin 
  set @pstng_date_start = pstng_date_start;
  set @vocher_num = 0;
  set @sale_num = 0;
  set @return_num = 0;
  set @vss_num = 0;
  set @gmv_num = 0;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  truncate table certificate_head;
  truncate table mds_fin_return_detail;
  truncate table mds_fin_sale_detail;
  truncate table mds_fin_vss_detail;
  truncate table mds_fin_rece_detail;
  truncate table fin_certificate_result;


  set @strsql = 'delete from fin_certificate_gmv where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  /*插入商城销售明细数据临时表*/
  set @strsql = '
     insert into mds_fin_sale_detail 
     select a.zone_id,a.zone_name,a.order_id,a.sale_id,a.pay_type,a.final_amount,a.afs_amount,a.user_type,
            a.fin_code as customer,b.product_code,b.product_name,b.price,b.qty,b.real_qty,b.tax,
            b.average_price,b.nt_price,b.type,b.fin_code as vendor,a.user_fin_code,a.pstng_date
       from 
            (select zone_id,zone_name,sale_id,order_id,pay_type,final_amount,day as pstng_date,user_type,fin_code,user_fin_code,day,afs_amount
               from mall_sale_head
              where day >= @pstng_date_start                       
                and day <= @pstng_date_end
            ) as a 
            join 
            (select a.zone_id,a.sale_id,a.product_code,a.product_name,a.price,a.qty,a.real_qty,a.average_price,a.nt_price,
                    a.tax,a.type,a.fin_code
               from mall_sale_detail as a 
              where day >= @pstng_date_start                       
                and day <= @pstng_date_end
                and not exists(select zone_id,sale_id 
                                from mall_sale_detail as b 
                               where day >= @pstng_date_start                       
                                 and day <= @pstng_date_end
                                 and (fin_code = '''' or fin_code is null)
                                 and type = 2
                                 and a.sale_id = b.sale_id 
                                 and a.zone_id = b.zone_id)
            ) as b
            on (a.zone_id = b.zone_id and a.sale_id = b.sale_id)';
  PREPARE createsql FROM @strsql;  
  EXECUTE createsql; 
  DROP PREPARE createsql;

  select FOUND_ROWS() into @sale_num;
  
  /*插入商城退货明细数据表*/
  set @strsql = '
       insert into mds_fin_return_detail 
       select a.zone_id,a.zone_name,a.return_id,a.receipt_id,a.second_receipt_id,a.it_amount,a.nt_amount,a.user_type,
            b.product_code,b.product_name,b.qty,b.real_qty,b.sale_price,b.average_price,b.tax,b.type,b.fin_code as vendor,a.pstng_date
       from (select zone_id,zone_name,return_id,receipt_id,second_receipt_id,it_amount,user_type,
                    nt_amount,day as pstng_date
               from mall_return_head
              where day >= @pstng_date_start                       
                and day <= @pstng_date_end
            ) as a
            join
            (select zone_id,zone_name,return_id,product_code,product_name,sale_price,
                    average_price,qty,real_qty,tax,type,fin_code
               from mall_return_detail as a
              where day >= @pstng_date_start                       
                and day <= @pstng_date_end
                and not exists(select zone_id,return_id 
                                from mall_return_detail as b 
                               where day >= @pstng_date_start                       
                                 and day <= @pstng_date_end
                                 and (fin_code = '''' or fin_code is null)
                                 and type = 2
                                 and a.return_id = b.return_id 
                                 and a.zone_id = b.zone_id)
            ) as b
            on (a.zone_id = b.zone_id and a.return_id = b.return_id)';

  PREPARE createsql FROM @strsql;  
  EXECUTE createsql; 
  DROP PREPARE createsql;

  select FOUND_ROWS() into @return_num;


  set @strsql = '
      insert into mds_fin_vss_detail
      select a.zone_id,a.zone_name,a.order_id,a.it_amount,a.nt_amount,b.vendor,b.vendor_name,b.type,a.pstng_date
        from 
      (
      select zone_id,
             case when zone_id = ''1000'' then ''北京''
                  when zone_id = ''1001'' then ''天津''
                  when zone_id = ''1002'' then ''杭州''
                  end zone_name,
             vid,reference as order_id,it_amount,nt_amount,from_unixtime(order_date,''%Y-%m-%d'') as pstng_date
        from receipt_head
       where from_unixtime(order_date,''%Y-%m-%d'') >= @pstng_date_start
         and from_unixtime(order_date,''%Y-%m-%d'') <= @pstng_date_end
      union all
      select zone_id,
             case when zone_id = ''1000'' then ''北京''
                  when zone_id = ''1001'' then ''天津''
                  when zone_id = ''1002'' then ''杭州''
                  end zone_name,
             vid,return_id as order_id,it_amount*-1,nt_amount*-1,from_unixtime(date,''%Y-%m-%d'') as pstng_date
        from return_head
       where from_unixtime(date,''%Y-%m-%d'') >= @pstng_date_start
         and from_unixtime(date,''%Y-%m-%d'') <= @pstng_date_end
      ) as a
      left join
      (select vid,type,fin_code as vendor,name as vendor_name from vender_main) as b 
      on (a.vid = b.vid)';

  PREPARE createsql FROM @strsql;  
  EXECUTE createsql; 
  DROP PREPARE createsql;

  select FOUND_ROWS() into @vss_num;


  set @strsql = '
      insert into mds_fin_rece_detail
      select b.zone_id,
             case when b.zone_id = 1000 then ''北京''
                  when b.zone_id = 1001 then ''天津''
                  when b.zone_id = 1002 then ''杭州''
                  end as zone_name,
             a.money,
             b.vendor,b.vendor_name,b.type,a.status,a.is_prepayer,b.settle_type,a.pstng_date
        from 
      (select vid,it_amount as money,status,is_prepayer,from_unixtime(issue_date,''%Y-%m-%d'') pstng_date 
         from pay_sheet
        where from_unixtime(issue_date,''%Y-%m-%d'') >= @pstng_date_start 
          and from_unixtime(issue_date,''%Y-%m-%d'') <= @pstng_date_end
         ) as a
      join
      (select zone_id,vid,type,settle_type,fin_code as vendor,name as vendor_name from vender_main) as b
      on (a.vid = b.vid)';

  PREPARE createsql FROM @strsql;  
  EXECUTE createsql; 
  DROP PREPARE createsql;

  select FOUND_ROWS() into @rece_num;

  set @strsql = '
    insert into fin_certificate_gmv 
    select zone_id,zone_name,sum(price*real_qty) as gmv_money,sum(average_price*real_qty) as cost_money,vendor,pstng_date 
      from mds_fin_sale_detail 
     where  type = 2 
       and pstng_date >= @pstng_date_start 
       and pstng_date <= @pstng_date_end
     group by zone_id,zone_name,vendor,pstng_date';

  PREPARE createsql FROM @strsql;  
  EXECUTE createsql; 
  DROP PREPARE createsql;

  select FOUND_ROWS() into @gmv_num;


  set @strsql = '
  insert into certificate_head (type,head_id,created_at,updated_at) 
  select ''wm_receipt'' as type,receipt_id as head_id,unix_timestamp() created_at,unix_timestamp() updated_at 
    from wm_receipt_head
   where day >= @pstng_date_start                       
     and day <= @pstng_date_end
  union all
  select ''wm_return'' as type,return_id as head_id,unix_timestamp() created_at,unix_timestamp() updated_at 
    from wm_return_head
   where day >= @pstng_date_start                       
     and day <= @pstng_date_end
  union all
  select ''mall_sale'' as type,a.sale_id as head_id,unix_timestamp() created_at,unix_timestamp() updated_at 
    from (select distinct sale_id from mds_fin_sale_detail) as a
  union all
  select ''mall_return'' as type,return_id as head_id,unix_timestamp() created_at,unix_timestamp() updated_at 
    from (select distinct return_id from mds_fin_return_detail) as a';

   PREPARE createsql FROM @strsql;  
   EXECUTE createsql; 
   DROP PREPARE createsql;


   select FOUND_ROWS() into @vocher_num;
   
   
   select concat_ws(' ','生成凭证有效数据',@vocher_num,'生成商城有效数据',@sale_num,'生成商城退货数据',@return_num,'生成寄售入库数据',@vss_num,'付款数据',@rece_num,'GMV数据',@gmv_num);

end
 //
 
# call fin_voucher_data_modify('2017-07-01','2017-07-31');




