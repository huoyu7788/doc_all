
drop table IF EXISTS fin_certificate_bsds_rv;
CREATE TABLE `fin_certificate_bsds_rv` (
  `zone_name` varchar(10) COMMENT '区域',
  `receipt_id` varchar(32) COMMENT '签收单',
  `tax_rate` char(5) COMMENT '税率',
  `doc_type` varchar(10) COMMENT '凭证类型',
  `dctype` varchar(10) COMMENT '借贷标志',
  `doc_number` varchar(10) COMMENT '科目编码',
  `dd` varchar(10) COMMENT '部门号',
  `project` varchar(10) COMMENT '项目大类',
  `project_cate` varchar(10) COMMENT '项目',
  `money` decimal(16,2) COMMENT '金额',
  `text` varchar(67) COMMENT '项目',
  `pstng_date` varchar(16) COMMENT '时间',
  `customer` varchar(20) COMMENT '承运商编码',
  `vendor` varchar(20) COMMENT '供应商编码'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP PROCEDURE IF EXISTS fin_voucher_bsds_rv;  
  delimiter //
  create procedure fin_voucher_bsds_rv 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out bsds_rv_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  drop table if exists fin_certificate_bsds_rv_detail;
 
  set @strsql = 'delete from fin_certificate_result where dctype = ''BSDS_RV''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;
   
  set @strsql = 'delete from fin_certificate_bsds_rv where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
        create table fin_certificate_bsds_rv_detail as 
        select zone_name,sale_id as receipt_id,'''' as tax_rate,''BSDS_RV'' as doc_type,''C'' as dctype,
               ''12210302'' as doc_number,'''' as dd,'''' as project,'''' as project_cate,
               round(sum(price*qty)*0.9,2) as money,''商贸公司'' as text,pstng_date,'''' as customer,
               '''' as vendor
          from mds_fin_sale_detail a
         where vendor=''01020007''
           and user_type = 1
         group by zone_name,sale_id,pstng_date
        union all
        select zone_name,sale_id as receipt_id,'''' as tax_rate,''BSDS_RV'' as doc_type,''C'' as dctype,
               ''66010402'' as doc_number,''050202'' as dd,''0101'' as project,
               ''01'' as project_cate,round(sum((price*qty)*0.1)/1.06,2) money,
               ''商贸公司'' as text,pstng_date,'''' as customer,'''' as vendor
          from mds_fin_sale_detail a
         where vendor=''01020007''
           and user_type = 1
         group by zone_name,sale_id,pstng_date
        union all
        select zone_name,sale_id as receipt_id,'''' as tax_rate,''BSDS_RV'' as doc_type,''C''  as dctype,''12210404'' as doc_number,'''' as dd,'''' as project,
               '''' as project_cate,round(sum((price*qty)*0.1)/1.06*0.06,2) as money,
               ''商贸公司'' as text,pstng_date,'''' as customer,'''' as vendor
          from mds_fin_sale_detail a
         where vendor=''01020007''
           and user_type = 1
         group by zone_name,sale_id,pstng_date

        union all 
        select zone_name,sale_id as receipt_id,tax as tax_rate,''BSDS_RV'' as doc_type,''D'' as dctype,
               ''6001010201'' as doc_number,''050202'' as dd,
               case when tax = 17 then ''0101'' 
                    when tax = 13 then ''0102''
                    when tax = 0  then ''0103''
               end as project,
               case when tax = 17 then ''01'' 
                    when tax = 13 then ''01''
                    when tax = 0  then ''01''
               end as project_cate,
               round(sum(price*qty/(1+tax/100)),2) money,
               ''商贸公司'' as text,pstng_date,
               '''' as customer,
               '''' as vendor
        from mds_fin_sale_detail
        where vendor=''01020007''
          and user_type = 1
        group by zone_name,sale_id,pstng_date,tax

        union all 
        select zone_name,sale_id as receipt_id,tax as tax_rate,''BSDS_RV'' doc_type,''D'' dctype,
               case when tax = 17 then ''2221010101'' else ''2221010104'' end doc_number,
               '''' dd,'''' as project,'''' as project_cate,round(sum(price*qty*(tax/100)/(1+tax/100)),2) as money,
               ''商贸公司'' as text,pstng_date,'''' as customer,'''' as vendor
        from mds_fin_sale_detail
        where vendor=''01020007''
          and user_type = 1
        group by zone_name,sale_id,pstng_date,tax';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_bsds_rv
    select zone_name,'''' as receipt_id,'''' as tax_rate,''BSDS_RV'' as doc_type,''D'' as dctype,
               ''6001010201'' as doc_number,''050202'' as dd,''0101'' as project,''01'' as project_cate,
               sub_money as money,''商贸公司'' as text,pstng_date,'''' as customer,'''' as vendor
      from (select zone_name,pstng_date,
                   sum(case when dctype = ''C'' then money end) - 
                   sum(case when dctype = ''D'' then money end) as sub_money
              from fin_certificate_bsds_rv_detail
             where pstng_date >= @pstng_date_start
               and pstng_date <= @pstng_date_end
             group by zone_name,pstng_date
            ) as a
      where round(sub_money,2)<>0
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           money,text,pstng_date,customer,vendor
      from fin_certificate_bsds_rv_detail';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;


  set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' receipt_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,''1'' as status,unix_timestamp() created_at,
           unix_timestamp() updated_at
      from fin_certificate_bsds_rv
     where pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  truncate table fin_certificate_bsds_rv_detail;
  drop table fin_certificate_bsds_rv_detail;

  select FOUND_ROWS() into bsds_rv_num; 


end
//

#  call fin_voucher_bsds_rv('2017-05-01','2017-05-10',@bsds_rv_num);



