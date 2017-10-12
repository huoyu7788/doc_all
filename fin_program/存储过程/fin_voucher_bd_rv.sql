
-- FGS001  富君辉大红门  01020016
-- FGS002  富君辉望京  01020016
-- FGS003  顺民永恒  01020022
-- FGS004  天佑汇鑫  01020021


drop table if exists fin_certificate_bd_rv_detail;

CREATE TABLE `fin_certificate_bd_rv_detail` (
  `zone_name` varchar(10) DEFAULT NULL COMMENT '区域',
  `receipt_id` varchar(32) DEFAULT NULL COMMENT '签收单',
  `pstng_date` varchar(16) DEFAULT NULL COMMENT '时间',
  `all_money` decimal(16,2) DEFAULT NULL COMMENT '单据金额',
  `money` decimal(16,2) DEFAULT NULL COMMENT '单据实收金额',
  `dx` decimal(16,2) DEFAULT NULL COMMENT '代销金额',
  KEY `idx_zone_name` (`zone_name`),
  KEY `idx_receipt_id` (`receipt_id`),
  KEY `idx_pstng_date` (`pstng_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

drop table if exists fin_certificate_bd_rv_detail_all;

CREATE TABLE `fin_certificate_bd_rv_detail_all` (
  `zone_name` varchar(10) DEFAULT NULL COMMENT '区域',
  `receipt_id` varchar(32) DEFAULT NULL COMMENT '签收单',
  `tax_rate` char(5) DEFAULT NULL COMMENT '税率',
  `doc_type` varchar(10) DEFAULT NULL COMMENT '凭证类型',
  `dctype` varchar(10) DEFAULT NULL COMMENT '借贷标志',
  `doc_number` varchar(10) DEFAULT NULL COMMENT '科目编码',
  `customer` varchar(20) DEFAULT NULL COMMENT '承运商编码',
  `vendor` varchar(20) DEFAULT NULL COMMENT '供应商编码',
  `dd` varchar(10) DEFAULT NULL COMMENT '部门号',
  `project` varchar(10) DEFAULT NULL COMMENT '项目大类',
  `project_cate` varchar(10) DEFAULT NULL COMMENT '项目',
  `money` decimal(16,2) DEFAULT NULL COMMENT '金额',
  `text` varchar(67) DEFAULT NULL COMMENT '项目',
  `pstng_date` varchar(16) DEFAULT NULL COMMENT '时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

drop table if exists fin_certificate_bd_rv;
CREATE TABLE `fin_certificate_bd_rv` (
  `zone_name` varchar(10) DEFAULT NULL COMMENT '区域',
  `receipt_id` varchar(32) DEFAULT NULL COMMENT '签收单',
  `tax_rate` char(5) DEFAULT NULL COMMENT '税率',
  `doc_type` varchar(10) DEFAULT NULL COMMENT '凭证类型',
  `dctype` varchar(10) DEFAULT NULL COMMENT '借贷标志',
  `doc_number` varchar(10) DEFAULT NULL COMMENT '科目编码',
  `dd` varchar(10) DEFAULT NULL COMMENT '部门号',
  `project` varchar(10) DEFAULT NULL COMMENT '项目大类',
  `project_cate` varchar(10) DEFAULT NULL COMMENT '项目',
  `money` decimal(16,2) DEFAULT NULL COMMENT '单据实收',
  `text` varchar(67) DEFAULT NULL COMMENT '文本',
  `pstng_date` varchar(16) DEFAULT NULL COMMENT '时间',
  `customer` varchar(20) DEFAULT NULL COMMENT '承运商编码',
  `vendor` varchar(20) DEFAULT NULL COMMENT '供应商编码',
  `status` varchar(1) DEFAULT NULL COMMENT '生成凭证状态',
  `created_at` int(11) DEFAULT NULL COMMENT '创建时间',
  `updated_at` int(11) DEFAULT NULL COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP PROCEDURE IF EXISTS fin_voucher_bd_rv;  
  delimiter //
  create procedure fin_voucher_bd_rv 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out bd_rv_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end = '' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;


  set @strsql = 'delete from fin_certificate_result where dctype = ''BD_RV''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = 'delete from fin_sale_bug where type = ''BD'' and pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = 'delete from fin_certificate_bd_rv where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_bd_rv
    select a.zone_name,a.receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
           f.dd,f.project,f.project_cate,a.money,
           concat_ws('' '',a.pstng_date,case when f.gl_account = ''224103'' then ''冻品平台代收款'' else f.item_text end) text,
           a.pstng_date,a.customer,'''' as vendor,''1'' status,unix_timestamp() created_at,
           unix_timestamp() updated_at
      from (
            select zone_name,sale_id as receipt_id,round(max(final_amount),2) as money,pstng_date,'''' as customer,
                   case when pay_type=3 then ''现金''
                        when pay_type=1 then ''支付宝''
                        when pay_type=5 then ''拉卡拉''
                        when pay_type=6 then ''微信-扫码''
                        when pay_type=2 then ''微信-APP''
                        when pay_type=8 then ''支付宝-扫码''
                        when pay_type=9 then ''赊销''
                        when pay_type=0 then ''现金''
                        end as type
              from mds_fin_sale_detail
             where pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
               and type = 4
               and pay_type not in (0,3)
             group by zone_name,sale_id,pstng_date,customer,pay_type 
             union all
             select zone_name,sale_id as receipt_id,round(max(final_amount),2) as money,pstng_date,'''' as customer,
                    ''DAIXIAO'' as type
              from mds_fin_sale_detail
             where pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
               and type = 4
               and pay_type not in (0,3)
             group by zone_name,sale_id,pstng_date,customer,pay_type
            ) as a
            join dim_fin_certificate_info_u8 f on f.flag=TYPE and a.zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
      insert into fin_sale_bug
      select distinct zone_name,sale_id as receipt_id,final_amount as money,pstng_date,''BD'' as type
        from mds_fin_sale_detail
       where pstng_date >= @pstng_date_start                       
         and pstng_date <= @pstng_date_end
         and type = 4
         and pay_type in (0,3)';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' receipt_id,'''' tax_rate,''BD_RV'' as doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
      from fin_certificate_bd_rv
     where pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor,status,created_at,updated_at';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into bd_rv_num; 
  
end
//

#    call fin_voucher_bd_rv('2017-09-18','2017-09-18',@bd_rv);





