drop table if exists fin_certificate_rv_fc;
CREATE TABLE `fin_certificate_rv_fc` (
  `zone_name` varchar(10) COMMENT '区域',
  `receipt_id` varchar(32) COMMENT '签收单',
  `tax_rate` char(5) COMMENT '税率',
  `doc_type` varchar(10) COMMENT '凭证类型',
  `dctype` varchar(10) COMMENT '借贷标志',
  `doc_number` varchar(10) COMMENT '科目编码',
  `dd` varchar(10) COMMENT '部门号',
  `money` decimal(16,2) COMMENT '金额',
  `text` varchar(67) COMMENT '项目',
  `pstng_date` varchar(16) COMMENT '时间',
  `customer` varchar(20) COMMENT '承运商编码',
  `vendor` varchar(20) COMMENT '供应商编码',
  `project` varchar(10) COMMENT '项目大类',
  `project_cate` varchar(10) COMMENT '项目',
  `status` varchar(1) COMMENT '生成凭证状态',
  `created_at` int(11) comment '创建时间',
  `updated_at` int(11) comment '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


drop table if exists fin_certificate_rv_fc_detail;
CREATE TABLE `fin_certificate_rv_fc_detail` (
  `zone_name` varchar(10) COMMENT '区域',
  `receipt_id` varchar(32) COMMENT '签收单',
  `tax_rate` char(5) COMMENT '税率',
  `doc_type` varchar(10) COMMENT '凭证类型',
  `dctype` varchar(10) COMMENT '借贷标志',
  `doc_number` varchar(10) COMMENT '科目编码',
  `customer` varchar(20) COMMENT '承运商编码',
  `vendor` varchar(20) COMMENT '供应商编码',
  `dd` varchar(10) COMMENT '部门号',
  `project` varchar(10) COMMENT '项目大类',
  `project_cate` varchar(10) COMMENT '项目',
  `money` decimal(16,2) COMMENT '金额',
  `text` varchar(67) COMMENT '项目',
  `pstng_date` varchar(16) COMMENT '时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP PROCEDURE IF EXISTS fin_voucher_rv_fc;  
  delimiter //
  create procedure fin_voucher_rv_fc 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out rv_fc_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  drop table if exists fin_certificate_rv_fc_detail;


  set @strsql = 'delete from fin_certificate_result where dctype = ''RV_F''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = 'delete from fin_certificate_rv_fc where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
        create table fin_certificate_rv_fc_detail as 
        select a.zone_name,receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,a.customer,'''' vendor,
               f.dd,f.project,f.project_cate,money,concat_ws('' '',pstng_date,item_text)
        text,pstng_date
        from (
        select zone_name,receipt_id,round(sum(price*real_qty/(1+tax_rate/100)),2) money,pstng_date,flag,'''' as customer
          from 
        (
        select zone_name,sale_id as receipt_id,price,real_qty,tax as tax_rate,pstng_date ,
               concat(''FC_NO'',case when type = 0 then ''WUMART''
                                   when type = 1 then ''JISHOU'' 
                                   end,
                      ''_'',tax) as flag,
               '''' customer
          from mds_fin_sale_detail
         where type in (0,1)
           and user_type = 3
           and pstng_date >= @pstng_date_start
           and pstng_date <= @pstng_date_end
        ) as a
        group by zone_name,receipt_id,pstng_date,flag
        union all
        select zone_name,sale_id as receipt_id,round(sum(price*real_qty),2) money,pstng_date,
               ''FC_DAIXIAO'' flag,vendor as customer
          from mds_fin_sale_detail
         where type = 2
           and user_type = 3
           and pstng_date >= @pstng_date_start
           and pstng_date <= @pstng_date_end
        group by zone_name,sale_id,pstng_date
        union all
        select zone_name,receipt_id,round(sum(price*real_qty*(tax_rate/100)/(1+tax_rate/100)),2) money,
               pstng_date,flag,'''' as customer
          from 
        (
        select zone_name,sale_id as receipt_id,price,real_qty,tax as tax_rate,pstng_date,
               concat(''FC_'',case when type = 0 then ''WUMART'' 
                                 when type = 1 then ''JISHOU'' 
                                 end,
                      ''_'',tax) as flag
          from mds_fin_sale_detail
         where type in (0,1)
           and user_type = 3
           and pstng_date >= @pstng_date_start
           and pstng_date <= @pstng_date_end
        ) as a
        group by zone_name,receipt_id,pstng_date,flag   
        union all
        select zone_name,sale_id as receipt_id,max(final_amount) as money,pstng_date,''FC_RV_CY'' flag, 
               case when zone_name = ''北京'' and (customer = '''' or customer is null) then ''01010013''
                    when zone_name = ''天津'' and (customer = '''' or customer is null) then ''02010003''
                    when zone_name = ''杭州'' and (customer = '''' or customer is null) then ''03010001''
                else customer end as customer
          from mds_fin_sale_detail 
         where pstng_date >= @pstng_date_start
           and pstng_date <= @pstng_date_end
           and user_type = 3
           and type in (0,1,2)
         group by zone_name,sale_id,pstng_date,customer
        ) as a
        join dim_fin_certificate_info_u8 f on f.flag=a.flag and zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  set @strsql = '
        insert into fin_sale_bug
        SELECT zone_name,receipt_id,sub_money money,pstng_date,type
          from (select zone_name,receipt_id,pstng_date,
                       round(round(sum(case when dctype=''C'' then money else 0 end),2) - 
                       round(sum(case when dctype=''D'' then money else 0 end),2),2) sub_money,
                       ''RV_FC'' as type
                  from fin_certificate_rv_fc_detail
                 group by zone_name,receipt_id,pstng_date
                ) a
         where abs(sub_money)>0.1';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
        insert into fin_certificate_rv_fc
        select zone_name,receipt_id,tax_rate,''RV_F'' doc_type,dctype,doc_number,dd,money,text,pstng_date,customer,
               vendor,project,project_cate,''1'' status,unix_timestamp() created_at,unix_timestamp() updated_at
        from (
            select zone_name,receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,
                   case when f.gl_account = ''11220202'' and zone_name = ''北京'' then ''01010001''
                        when f.gl_account = ''11220202'' and zone_name = ''天津'' then ''02010001''
                        when f.gl_account = ''11220202'' and zone_name = ''杭州'' then ''03010003''
                        else '''' end as customer,
                   '''' vendor,f.dd,f.project,f.project_cate,money,
                   concat_ws('' '',pstng_date,item_text) text,pstng_date
            from (
                SELECT zone_name,receipt_id,pstng_date,
                       case when sub_money>0 and zone_name = ''北京'' then ''FC_NOWUMART_17'' 
                            when sub_money>0 and zone_name = ''天津'' then ''FC_NOWUMART_17'' 
                            when sub_money>0 and zone_name = ''杭州'' then ''FC_NOJISHOU_17''
                       else ''FC_RV_CY'' 
                       end flag,
                       abs(sub_money) money
                from (
                    select zone_name,receipt_id,pstng_date,
                        round(round(sum(case when dctype=''C'' then money else 0 end),2) - abs(round(sum(case when dctype=''D'' then money else 0 end),2)),2) sub_money
                    from fin_certificate_rv_fc_detail
                    group by zone_name,receipt_id,pstng_date
                ) a
                where round(sub_money,2)<>0
            ) a
            join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
            union all
            select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,abs(money) money,text,pstng_date
            from fin_certificate_rv_fc_detail
        ) b ';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' receipt_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
      from fin_certificate_rv_fc
     where pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor,status,created_at,updated_at';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into rv_fc_num;
  
  truncate table fin_certificate_rv_fc_detail;
  drop table fin_certificate_rv_fc_detail;

end
//




