
DROP PROCEDURE IF EXISTS fin_voucher_jishou_re_modify;  
  delimiter //
  create procedure fin_voucher_jishou_re_modify 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out jishou_re_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;


  drop table if exists fin_certificate_jishou_re_detail;


  
  set @strsql = 'delete from fin_certificate_result where dctype = ''JISHOU_RE''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  
  set @strsql = '
      create table fin_certificate_jishou_re_detail as
      SELECT zone_name,
             order_id,
             '''' tax_rate,
             ''JISHOU_RE'' doc_type,
             ''C'' dctype,
             ''140502'' doc_number,
             case when zone_name=''北京'' then ''010202''
                  when zone_name=''天津'' then ''020202''
                  when zone_name=''杭州'' then ''030202''
                  else '''' end dd,
             '''' as project,
             '''' as project_cate,
             round(sum(nt_amount),2) money,
             concat_ws('' '',pstng_date,vendor_name,order_id,''寄售入库'') text,
             pstng_date,
             '''' as customer,
             '''' as vendor
      FROM mds_fin_vss_detail
     where type = 1
      group by zone_name,order_id,vendor
      union all
      SELECT zone_name,
             order_id,
             '''' as tax_rate,
             ''JISHOU_RE'' doc_type,
             ''C'' dctype,
             ''12210404'' doc_number,
             case when zone_name=''北京'' then ''010202''
                  when zone_name=''天津'' then ''020202''
                  when zone_name=''杭州'' then ''030202''
                  else '''' end dd,
             '''' as project,
             '''' as project_cate,
             round(sum(it_amount - nt_amount),2) money,
             concat_ws('' '',pstng_date,vendor_name,order_id,''寄售入库'') text,
             pstng_date,
             '''' as customer,
             '''' as vendor
      FROM mds_fin_vss_detail
     where type = 1
      group by zone_name,order_id,vendor
      union all
      SELECT zone_name,
             order_id, 
             '''' tax_rate,
             ''JISHOU_RE'' doc_type,
             ''D'' dctype,
             ''22020102'' as doc_number,
             case when zone_name=''北京'' then ''010202''
                  when zone_name=''天津'' then ''020202''
                  when zone_name=''杭州'' then ''030202''
                  else '''' end dd,
             '''' as project,
             '''' as project_cate,
             round(sum(it_amount),2) money,
             concat_ws('' '',pstng_date,vendor_name,order_id,''寄售入库'') text,
             pstng_date,
             '''' customer,
             vendor
      FROM mds_fin_vss_detail
     where type = 1
      group by zone_name,order_id,vendor';


  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
      create table fin_certificate_jishou_re_modify as 
      select zone_name,
             order_id,
             '''' as tax_rate,
             ''JISHOU_RE'' as doc_type,
             case when submoney > 0 then ''D'' else ''C'' end dctype,
             case when submoney > 0 then ''22020102'' else ''140502'' end doc_number,
             case when zone_name=''北京'' then ''010202''
                  when zone_name=''天津'' then ''020202''
                  when zone_name=''杭州'' then ''030202''
                  else '''' end dd,
             '''' as project,
             '''' as project_cate,
             abs(submoney) as money,
             ''寄售入库'' as text,
             pstng_date,
             '''' customer,
             case when submoney > 0 and zone_name = ''北京'' then ''01010001''
                  when submoney > 0 and zone_name = ''天津'' then ''02010001''
                  when submoney > 0 and zone_name = ''杭州'' then ''03010001''
                  else '''' end vendor,
             unix_timestamp() created_at,
             unix_timestamp() updated_at
        from 
      (
      select zone_name,order_id,pstng_date,sum(case when dctype = ''C'' then money end) - sum(case when dctype = ''D'' then money end) as submoney
        from fin_certificate_jishou_re_detail 
       group by order_id,pstng_date
      ) as a 
      where submoney <> 0
      union all
      select zone_name,order_id,tax_rate,doc_type,dctype,doc_number,dd,project,
             project_cate,money,text,pstng_date,customer,vendor,unix_timestamp() created_at,
             unix_timestamp() updated_at 
        from fin_certificate_jishou_re_detail';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;  


    set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' order_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,1 as status,created_at,updated_at
      from fin_certificate_jishou_re_modify
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor,created_at,updated_at';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into jishou_re_num; 

  truncate table fin_certificate_jishou_re_detail;
  drop table fin_certificate_jishou_re_detail;
  drop table fin_certificate_jishou_re_modify;
  
end
//

#call fin_voucher_jishou_re_modify('2017-08-01','2017-08-31',@jishou_re_num)
