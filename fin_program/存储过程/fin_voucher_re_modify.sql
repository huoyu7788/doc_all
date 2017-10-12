

DROP PROCEDURE IF EXISTS fin_voucher_re_modify;  
  delimiter //
  create procedure fin_voucher_re_modify 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out re_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  -- truncate table fin_certificate_re_detail;

  drop table if exists fin_certificate_re_detail;
  

  set @strsql = 'delete from fin_certificate_result where dctype = ''RE''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;
  
  set @strsql = 'delete from fin_certificate_re where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
      create table fin_certificate_re_detail as 
      select zone_name,order_id,'''' as tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,f.dd,a.money,concat(pstng_date,'' '',f.item_text) text,pstng_date,a.vendor
        from 
      (
      select zone_name,order_id,day as pstng_date,round(nt_amount,2) as money,''RE_WUMART'' flag,'''' as vendor
        from wm_receipt_head
       where day >= @pstng_date_start                       
         and day <= @pstng_date_end
      union all
      select zone_name,order_id,day as pstng_date,round(it_amount - nt_amount,2) as money,''RE_TAX_WUMART'' flag,'''' as vendor
        from wm_receipt_head
       where day >= @pstng_date_start                       
         and day <= @pstng_date_end
      union all
      select zone_name,order_id,day as pstng_date,round(it_amount,2) as money,''RE_GY'' flag,
             case when zone_id = ''1000'' then ''01010001'' 
                  when zone_id = ''1001'' then ''02010001''
                  when zone_id = ''1002'' then ''03010001''
                  end 
             as vendor
        from wm_receipt_head
       where day >= @pstng_date_start                       
         and day <= @pstng_date_end
      ) as a 
      left join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
      insert into fin_certificate_re 
      select zone_name,order_id,tax_rate,doc_type,dctype,doc_number,dd,'''' as project,'''' as project_cate,money,text,pstng_date,'''' as customer ,vendor,''1'' status,
          unix_timestamp() created_at,unix_timestamp() updated_at
      from (
          select zone_name,order_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,f.dd,money,pstng_date text,pstng_date,f.customer as vendor
          from (
              SELECT zone_name,order_id,pstng_date,
                  case when sub_money>0 then ''RE_D'' else ''RE_C'' end flag,
                  abs(sub_money) money
              from (
                  select zone_name,order_id,pstng_date,
                      round(round(sum(case when dctype=''C'' then money else 0 end),2) - round(sum(case when dctype=''D'' then money else 0 end),2),2) sub_money
                  from  fin_certificate_re_detail
                  group by zone_name,order_id,pstng_date
              ) a
              where round(sub_money,2)<>0
          ) a
          join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
          union all
          select zone_name,order_id,tax_rate,doc_type,dctype,doc_number,dd,money,text,pstng_date,vendor
          from fin_certificate_re_detail
      ) b ';
      
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' order_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
      from fin_certificate_re
     where pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor,status,created_at,updated_at';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into re_num; 

  truncate table fin_certificate_re_detail;
  drop table fin_certificate_re_detail;
  
end
//

#call fin_voucher_re_modify('2017-05-01','2017-05-10',@re_num);



