drop table if exists fin_certificate_zv2_ls;
CREATE TABLE `fin_certificate_zv2_ls` (
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
  `vendor` varchar(20) COMMENT '供应商编码',
  `status` varchar(1) COMMENT '生成凭证状态',
  `created_at` int(11) comment '创建时间',
  `updated_at` int(11) comment '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

drop table if exists fin_certificate_zv2_ls_detail;
CREATE TABLE `fin_certificate_zv2_ls_detail` (
  `zone_name` varchar(10) COMMENT '区域',
  `receipt_id` varchar(32) COMMENT '签收单',
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


DROP PROCEDURE IF EXISTS fin_voucher_zv2_ls;  
  delimiter //
  create procedure fin_voucher_zv2_ls 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out zv2_ls_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  drop table if exists fin_certificate_zv2_ls_detail;

  set @strsql = 'delete from fin_certificate_result where dctype = ''ZV2_LS''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = 'delete from fin_certificate_zv2_ls where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
      create table fin_certificate_zv2_ls_detail as 
      select zone_name,receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
             '''' customer,'''' as vendor,f.dd,money,concat_ws('' '',pstng_date,item_text) text,
             pstng_date
      from (
      select zone_name,receipt_id,round(sum(average_price*real_qty),2) money,pstng_date,flag
        from 
      (
      select zone_name,sale_id as receipt_id,average_price,real_qty,tax as tax_rate,pstng_date,
             concat(''FC_LSH_'',case when type = 0 then ''WUMART'' 
                                     when type = 1 then ''JISHOU'' 
                                      end) as flag
        from mds_fin_sale_detail
       where type in (0,1)
         and user_type = 4
         and pstng_date >= @pstng_date_start
         and pstng_date <= @pstng_date_end
      ) as a
      group by zone_name,receipt_id,pstng_date,flag
      union all
      select zone_name,receipt_id,round(sum(average_price*real_qty),2) money,pstng_date,flag
        from 
      (
      select zone_name,sale_id as receipt_id,average_price,real_qty,tax as tax_rate,pstng_date,
             concat(''FC_LSH_CB_'',case when type = 0 then ''WUMART'' 
                                        when type = 1 then ''JISHOU'' 
                                         end) as flag
        from mds_fin_sale_detail
       where type in (0,1)
         and user_type = 4
         and pstng_date >= @pstng_date_start
         and pstng_date <= @pstng_date_end
      ) as a
      group by zone_name,receipt_id,pstng_date,flag
      ) a
      join dim_fin_certificate_info_u8 f on f.flag=a.flag and zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


set @strsql = '
      insert into fin_certificate_zv2_ls 
      select zone_name,receipt_id,tax_rate, doc_type,dctype,doc_number,dd,'''' as project,
             '''' as project_cate, money,text,pstng_date,customer,
             vendor,''1'' status,unix_timestamp() created_at,unix_timestamp() updated_at
      from (
          select zone_name,receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,
                 '''' customer,'''' as vendor,f.dd,money,concat_ws('' '',pstng_date,item_text) text,pstng_date
          from (
              SELECT zone_name,receipt_id,pstng_date,
                  case when sub_money>0 then ''FC_LSH_WUMART'' else ''FC_LSH'' end flag,
                  abs(sub_money) money
              from (
                  select zone_name,receipt_id,pstng_date,
                      round(round(sum(case when dctype=''C'' then money else 0 end),2) - round(sum(case when dctype=''D'' then money else 0 end),2),2) sub_money
                  from fin_certificate_zv2_ls_detail
                  group by zone_name,receipt_id,pstng_date
              ) a
              where round(sub_money,2)<>0
          ) a
          join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
          union all
          select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,money,text,pstng_date
          from fin_certificate_zv2_ls_detail
      ) b ';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' receipt_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
      from fin_certificate_zv2_ls
     where pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor,status,created_at,updated_at';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into zv2_ls_num;

  truncate table fin_certificate_zv2_ls_detail;
  drop table fin_certificate_zv2_ls_detail;


end
//

#  call fin_voucher_zv2_ls ('2017-07-01','2017-07-10',@zv2_cy);



