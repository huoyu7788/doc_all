
drop table if exists fin_certificate_rece;
CREATE TABLE `fin_certificate_rece` (
  `zone_name` varchar(10) COMMENT '地区',
  `order_id` varchar(32) COMMENT '签收单',
  `doc_type` varchar(10) COMMENT '凭证类型',
  `dctype` varchar(10) COMMENT '借贷标志',
  `doc_number` varchar(10) COMMENT '科目编码',
  `dd` varchar(10) COMMENT '部门编码',
  `project` char(10) COMMENT '项目大类',
  `project_cate` char(10) COMMENT '项目',
  `money` decimal(16,2) COMMENT '金额',
  `text` varchar(67) COMMENT '文本',
  `pstng_date` varchar(16) COMMENT '时间',
  `customer` varchar(20) COMMENT '承运商编码',
  `vendor` varchar(20) COMMENT '供应商编码',
  `created_at` int(11) COMMENT '创建时间',
  `updated_at` int(11) COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP PROCEDURE IF EXISTS fin_voucher_rece;  
  delimiter //
  create procedure fin_voucher_rece 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out rece_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;


  set @strsql = 'delete from fin_certificate_result where dctype = ''KZ''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = 'delete from fin_certificate_rece where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
      insert into fin_certificate_rece
      select zone_name,
             b.project as order_id,
             b.doc_type,
             b.dctype,
             b.gl_account as doc_number,
             b.dd,
             '''' as project,
             '''' as project_cate,
             money,
             concat_ws('' '',a.pstng_date,a.vendor_name,b.item_text) as text,
             pstng_date,
             case when b.gl_account = ''12210202'' or b.gl_account = ''224103'' then vendor else '''' end as customer,
             case when b.gl_account = ''12210202'' or b.gl_account = ''224103'' then '''' else vendor end vendor,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from 
      (
      select zone_name,round(money,2) as money,pstng_date,vendor,vendor_name,
             case when type = 1 and is_prepayer = 0 and status = 5 and settle_type = 1 then ''KZ_JISHOU_ZQ_C''
                  when type = 1 and is_prepayer = 1 and status = 5 and settle_type = 1 then ''KZ_JISHOU_YF_C''
                  when type = 2 and status = 5 and settle_type = 1 then ''KZ_DAIXIAO_BZJ_C''
                  when type = 2 and status = 5 and settle_type = 3 then ''KZ_DAIXIAO_XLKS_C''
                  when type = 2 and status = 5 and settle_type = 2 then ''KZ_DAIXIAO_PCJS_C''
             end as flag
        from mds_fin_rece_detail
       where pstng_date >= @pstng_date_start
         and pstng_date <= @pstng_date_end
      union all
      select zone_name,round(money,2) as money,pstng_date,vendor,vendor_name,
             case when type = 1 and is_prepayer = 0 and status = 5 and settle_type = 1 then ''KZ_JISHOU_ZQ_D''
                  when type = 1 and is_prepayer = 1 and status = 5 and settle_type = 1 then ''KZ_JISHOU_YF_D''
                  when type = 2 and status = 5 and settle_type = 1 then ''KZ_DAIXIAO_BZJ_D''
                  when type = 2 and status = 5 and settle_type = 3 then ''KZ_DAIXIAO_XLKS_D''
                  when type = 2 and status = 5 and settle_type = 2 then ''KZ_DAIXIAO_PCJS_D''
             end as flag
        from mds_fin_rece_detail
       where pstng_date >= @pstng_date_start
         and pstng_date <= @pstng_date_end
      ) as a
      join 
      dim_third_pay_u8 as b
      on (a.zone_name = b.area and a.flag = b.flag)';



  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


    set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,order_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           money,text,pstng_date,customer,vendor,1 as status,created_at,updated_at
      from fin_certificate_rece
     where pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into rece_num; 
  
end
//

#  call fin_voucher_rece('2017-09-01','2017-09-11',@rece_num)
