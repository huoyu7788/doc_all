drop table if exists fin_certificate_bsds_we;
CREATE TABLE `fin_certificate_bsds_we` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP PROCEDURE IF EXISTS fin_voucher_bsds_we;  
  delimiter //
  create procedure fin_voucher_bsds_we 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out bsds_we_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  
  set @strsql = 'delete from fin_certificate_result where dctype = ''WE''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;
  
  set @strsql = 'delete from fin_certificate_bsds_we where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
        insert into fin_certificate_bsds_we
        select zone_name,sale_id as receipt_id,'''' as tax_rate,''WE'' as doc_type,''C'' as dctype,
               ''6401010201'' as doc_number,''050202'' as dd,'''' as project,'''' as project_cate,
               round(sum(average_price*qty/(1+tax/100)),2) as money,''商贸公司'' as text,pstng_date,
               '''' as customer,'''' as vendor
        from mds_fin_sale_detail
        where vendor=''01020007''
          and user_type = 1
        group by zone_name,sale_id,pstng_date
        union all 
        select zone_name,sale_id as receipt_id,tax as tax_rate,''WE'' as doc_type,''D'' as dctype,
               ''140502'' as doc_number,''050202'' as dd,'''' as project,'''' as project_cate,
               round(sum(average_price*qty/(1+tax/100)),2) as money,
               ''商贸公司'' as text,pstng_date,'''' as customer,'''' as vendor
         from mds_fin_sale_detail
        where vendor=''01020007''
          and user_type = 1
        group by zone_name,sale_id,pstng_date,tax';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' receipt_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,''1'' as status,unix_timestamp() created_at,
           unix_timestamp() updated_at
      from fin_certificate_bsds_we
     where pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into bsds_we_num; 
end
//

#call fin_voucher_dr('2017-05-01','2017-05-10',@dr_num);




