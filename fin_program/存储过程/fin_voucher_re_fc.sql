drop table if exists fin_certificate_re_fc;
CREATE TABLE `fin_certificate_re_fc` (
  `zone_name` varchar(10) COMMENT '区域',
  `order_id` varchar(32) COMMENT '签收单',
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
  `vendor` varchar(20) COMMENT '供应商编码',
  `status` varchar(1) COMMENT '生成凭证状态',
  `created_at` int(11) comment '创建时间',
  `updated_at` int(11) comment '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

drop table if exists fin_certificate_re_fc_detail;
CREATE TABLE `fin_certificate_re_fc_detail` (
  `zone_name` varchar(10) COMMENT '区域',
  `order_id` varchar(32) COMMENT '签收单',
  `tax_rate` char(5) COMMENT '税率',
  `doc_type` varchar(10) COMMENT '凭证类型',
  `dctype` varchar(10) COMMENT '借贷标志',
  `doc_number` varchar(10) COMMENT '科目编码',
  `customer` varchar(20) COMMENT '承运商编码',
  `vendor` varchar(20) COMMENT '供应商编码',
  `dd` varchar(10) COMMENT '部门号',
  `money` decimal(16,2) COMMENT '金额',
  `text` varchar(67) COMMENT '项目',
  `pstng_date` varchar(16) COMMENT '时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP PROCEDURE IF EXISTS fin_voucher_re_fc;  
  delimiter //
  create procedure fin_voucher_re_fc 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out re_fc_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  
  drop table if exists fin_certificate_re_fc_detail;

  set @strsql = 'delete from fin_certificate_result where dctype = ''RE_F''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = 'delete from fin_certificate_re_fc where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
        create table fin_certificate_re_fc_detail as 
        select zone_name,order_id as order_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,'''' customer,vendor,f.dd,money,concat_ws('' '',pstng_date,item_text) text,pstng_date
        from (
        select zone_name,order_id,day as pstng_date,round(nt_amount,2) as money,''FC_RE_WUMART'' as flag,'''' vendor
          from wm_return_head
         where day >= @pstng_date_start
           and day <= @pstng_date_end
           and gen_evidence = 0
        union all
        select zone_name,order_id,day as pstng_date,round(it_amount - nt_amount,2) as money,''FC_RE_TAX_WUMART'' as flag,'''' vendor
          from wm_return_head
         where day >= @pstng_date_start
           and day <= @pstng_date_end
           and gen_evidence = 0
        union all
        select zone_name,order_id,day as pstng_date,round(it_amount,2) as money,''FC_RE_GY'' as flag,
               case when zone_id = ''1000'' then ''01010001''
                    when zone_id = ''1001'' then ''02010001''
                    when zone_id = ''1002'' then ''03010001''
                    end 
               as vendor
          from wm_return_head
         where day >= @pstng_date_start
           and day <= @pstng_date_end
           and gen_evidence = 0
        ) as a
        join dim_fin_certificate_info_u8 f on f.flag=a.flag and zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  set @strsql = '
        insert into fin_certificate_re_fc
        select zone_name,order_id,tax_rate,''RE_F'' doc_type,dctype,doc_number,dd,'''' as project,'''' as project_cate,money,text,pstng_date,customer,vendor,''1'' status,
            unix_timestamp() created_at,unix_timestamp() updated_at
        from (
            select zone_name,order_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,'''' as customer ,f.customer as vendor,f.dd,money,concat_ws('' '',pstng_date,''物美退货'') text,pstng_date
            from (
                SELECT zone_name,order_id,pstng_date,
                    case when sub_money>0 then ''FC_RE_D'' else ''FC_RE_C'' end flag,
                    abs(sub_money) money
                from (
                    select zone_name,order_id,pstng_date,
                        round(round(sum(case when dctype=''C'' then money else 0 end),2) - round(sum(case when dctype=''D'' then money else 0 end),2),2) sub_money
                    from fin_certificate_re_fc_detail
                    group by zone_name,order_id,pstng_date
                ) a
                where round(sub_money,2)<>0
            ) a
            join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
            union all
            select zone_name,order_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,money,text,pstng_date
            from fin_certificate_re_fc_detail
        ) b ';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' order_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
      from fin_certificate_re_fc
     where pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor,status,created_at,updated_at';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into re_fc_num;

  truncate table fin_certificate_re_fc_detail;
  drop table fin_certificate_re_fc_detail;
  
end
//



