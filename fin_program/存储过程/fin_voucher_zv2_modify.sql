DROP PROCEDURE IF EXISTS fin_voucher_zv2_modify;  
  delimiter //
  create procedure fin_voucher_zv2_modify 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out zv2_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  drop table if exists fin_certificate_zv2_detail;
  
  set @strsql = 'delete from fin_certificate_result where dctype = ''ZV2''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
      create table fin_certificate_zv2_detail as 
      select a.zone_name,receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
             a.customer,a.vendor,f.dd,money*f.pn as money,concat_ws('' '',pstng_date,item_text) as text,pstng_date
      from (
      select zone_name,receipt_id,'''' as customer,'''' as vendor,pstng_date,
             round(sum(money),2) money,
             flag
      from 
      (
      select zone_name,sale_id as receipt_id,average_price*real_qty money,tax as tax_rate,pstng_date,
             concat(''ZV2_'',case when type = 0 then ''WUMART''
                                  when type = 1 then ''JISHOU'' end) as flag
        from mds_fin_sale_detail
       where type in (0,1)
         and user_type = 1
         and vendor <> ''999999''
      ) as a  
      group by zone_name,receipt_id,pstng_date,flag
      union all
      select zone_name,receipt_id,'''' as customer,'''' as vendor,pstng_date,
             round(sum(money),2) money,
             flag
      from 
      (
      select zone_name,sale_id as receipt_id,average_price*real_qty money,tax as tax_rate,pstng_date,
             concat(''ZV2_KC_'',case when type = 0 then ''WUMART'' 
                                     when type = 1 then ''JISHOU'' end) as flag
        from mds_fin_sale_detail
       where type in (0,1)
         and user_type = 1
         and vendor <> ''999999''
      ) as a  
      group by zone_name,receipt_id,pstng_date,flag
      union all
      select zone_name,second_receipt_id as receipt_id,'''' as customer,'''' as vendor,pstng_date,
             round(sum(money),2) money,
             concat(''ZV2_SH_'',case when type = 0 then ''WUMART'' 
                                     when type = 1 then ''JISHOU'' end) as flag
        from 
      (select zone_id,zone_name,return_id,second_receipt_id,average_price*real_qty money,tax as tax_rate,type,pstng_date
         from mds_fin_return_detail
        where type in (0,1)
          and user_type = 1
          and vendor <> ''999999''
      ) as b
      group by zone_name,second_receipt_id,pstng_date,type
      union all
      select zone_name,second_receipt_id as receipt_id,'''' as customer,'''' as vendor,pstng_date,
             round(sum(money),2) money,
             concat(''ZV2_SH_KC_'',case when type = 0 then ''WUMART'' 
                                        when type = 1 then ''JISHOU'' end) as flag
        from 
      (select zone_id,zone_name,return_id,second_receipt_id,average_price*real_qty money,tax as tax_rate,type,pstng_date
         from mds_fin_return_detail
        where type in (0,1)
          and user_type = 1
          and vendor <> ''999999''
      ) as b
      group by zone_name,second_receipt_id,pstng_date,type
      ) as a
      join dim_fin_certificate_info_u8 f on f.flag=a.flag and zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;
 
  set @strsql = '
        create table fin_certificate_zv2_modify as
        select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,dd,'''' as project,'''' as project_cate,money,text,pstng_date,customer,vendor,''1'' status,
            unix_timestamp() created_at,unix_timestamp() updated_at
        from (
            select zone_name,receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,
                   '''' as customer,'''' as vendor,f.dd,money,concat_ws('' '',pstng_date,item_text) text,pstng_date
            from (
                SELECT zone_name,receipt_id,pstng_date,
                    case when sub_money>0 then ''ZV2_D'' else ''ZV2_C'' end flag,
                    abs(sub_money) money
                from (
                    select zone_name,receipt_id,pstng_date,
                        round(round(sum(case when dctype=''C'' then money else 0 end),2) - round(sum(case when dctype=''D'' then money else 0 end),2),2) sub_money
                    from fin_certificate_zv2_detail
                    group by zone_name,receipt_id,pstng_date
                ) a
                where round(sub_money,2)<>0
            ) a
            join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
            union all
            select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,money,text,pstng_date
            from fin_certificate_zv2_detail
        ) b ';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;
 
 
  set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' receipt_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
      from fin_certificate_zv2_modify
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor,status,created_at,updated_at';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into zv2_num;

  truncate table fin_certificate_zv2_detail;
  drop table if exists fin_certificate_zv2_detail;
  drop table fin_certificate_zv2_modify;
  
end
//


#   call fin_voucher_zv2_modify('2017-07-01','2017-07-10',@zv2);




