drop table if exists fin_certificate_bank_result_bug;
CREATE TABLE `fin_certificate_bank_result_bug` (
  `name` varchar(10) COMMENT '区域',
  `waybill_no` varchar(32) COMMENT '运单号',
  `tax_rate` varchar(20) COMMENT '税率',
  `dctype` varchar(5) COMMENT '凭证类型',
  `cd_type` varchar(5) COMMENT '借贷标示',
  `doc_number` varchar(20) COMMENT '科目编码',
  `dd` varchar(10) COMMENT '部门号',
  `project` varchar(10) COMMENT '项目大类',
  `project_cate` varchar(10) COMMENT '项目',
  `money` decimal(16,2) COMMENT '金额',
  `text` varchar(128) COMMENT '项目',
  `pstng_date` varchar(16) COMMENT '时间',
  `customer` varchar(20) COMMENT '承运商编码',
  `status` varchar(1) COMMENT '生成凭证状态',
  `created_at` int(11) comment '创建时间',
  `updated_at` int(11) comment '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP PROCEDURE IF EXISTS fin_vocher_bank_result_bug;  
  delimiter //
  create procedure fin_vocher_bank_result_bug 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out bank_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end=''
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  set @strsql = 'delete from fin_certificate_bank_result_bug where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
      insert into fin_certificate_bank_result_bug
      select ''北京'' as name,
             '''' as waybill_no,
             '''' as tax_rate,
             b.doc_type,
             b.dctype,
             b.gl_account doc_number,
             b.dd,
             '''' as project,
             '''' as project_cate,
             case when a.system = ''BR_BJ_BJ_SXF'' then a.money_sxf_bj
                  when a.system = ''BR_BJ_TJ_SXF'' then a.money_sxf_tj
                  when a.system = ''BR_BJ_HZ_SXF'' then a.money_sxf_hz
                  when a.system = ''BR_BJ_YFZK'' then a.money_yszk
                  when a.system = ''BR_BJ_TJ_NBWL'' then a.money_nbwl_tj
                  when a.system = ''BR_BJ_HZ_NBWL'' then a.money_nbwl_hz
             end as money,
             concat_ws('' '',pstng_date,b.item_text) as text,
             pstng_date as pstng_date,
             '''' as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
             (
              select pstng_date,
                     flag,
                     system,
                     module,
                     round(sum(money_sxf_bj),2) as money_sxf_bj,
                     round(sum(money_sxf_tj),2) as money_sxf_tj,
                     round(sum(money_sxf_hz),2) as money_sxf_hz,
                     round(sum(money_yszk),2) as money_yszk,
                     round(sum(money_nbwl_tj),2) as money_nbwl_tj,
                     round(sum(money_nbwl_hz),2) as money_nbwl_hz
               from fin_certificate_no_lkl_detail_bug
              where pstng_date >= @pstng_date_start                       
                and pstng_date <= @pstng_date_end
              group by pstng_date,flag,system,module
             ) as a
             JOIN dim_third_pay_u8 as b
              on (a.flag = b.flag)
      union all
      select ''北京'' as name,
             ''0102'' as waybill_no,
             '''' as tax_rate,
             b.doc_type,
             b.dctype,
             b.gl_account doc_number,
             b.dd,
             '''' as project,
             '''' as project_cate,
             a.money as money,
             concat_ws('' '',pstng_date,b.item_text) as text,
             pstng_date as pstng_date,
             '''' as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
             (
              select pstng_date,
                     module,
                     round(sum((money_nbwl_hz + money_nbwl_tj + money_yszk) - (money_sxf_bj + money_sxf_tj + money_sxf_hz)),2) as money
                from fin_certificate_no_lkl_detail_bug
              where pstng_date >= @pstng_date_start                       
                and pstng_date <= @pstng_date_end
               group by pstng_date,module
              ) as a
              join dim_third_pay_u8 as b
              on (concat(''BR_BJ_YHCK_'',a.module) = b.flag)
      union all
      select ''天津'' as name,
             '''' as waybill_no,
             '''' as tax_rate,
             b.doc_type,
             b.dctype,
             b.gl_account doc_number,
             b.dd,
             '''' as project,
             '''' as project_cate,
             a.money as money,
             concat_ws('' '',pstng_date,b.item_text) as text,
             pstng_date as pstng_date,
             '''' as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
            (
            select pstng_date,
                   round(sum(money_nbwl_tj),2) as money,
                   module
              from fin_certificate_no_lkl_detail_bug
             where name = ''天津''
               and pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
             group by pstng_date,module
            ) as a
            join dim_third_pay_u8 as b
            on (concat(''BR_TJ_NBWL_'',a.module) = b.flag)
        union all
      select ''天津'' as name,
             '''' as waybill_no,
             '''' as tax_rate,
             b.doc_type,
             b.dctype,
             b.gl_account doc_number,
             b.dd,
             '''' as project,
             '''' as project_cate,
             a.money as money,
             concat_ws('' '',pstng_date,b.item_text) as text,
             pstng_date as pstng_date,
             '''' as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
            (
            select pstng_date,
                   round(sum(money_nbwl_tj),2) as money,
                   module
              from fin_certificate_no_lkl_detail_bug
             where name = ''天津''
               and pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
             group by pstng_date,module
            ) as a
            join dim_third_pay_u8 as b
            on (concat(''BR_TJ_YFZK_'',a.module) = b.flag)
      union all
      select ''杭州'' as name,
             '''' as waybill_no,
             '''' as tax_rate,
             b.doc_type,
             b.dctype,
             b.gl_account doc_number,
             b.dd,
             '''' as project,
             '''' as project_cate,
             a.money as money,
             concat_ws('' '',pstng_date,b.item_text) as text,
             pstng_date as pstng_date,
             '''' as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
            (
            select pstng_date,
                   round(sum(money_nbwl_hz),2) as money,
                   module
              from fin_certificate_no_lkl_detail_bug
             where name = ''杭州''
               and pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
             group by pstng_date,module
            ) as a
            join dim_third_pay_u8 as b
            on (concat(''BR_HZ_NBWL_'',a.module) = b.flag)
        union all
      select ''杭州'' as name,
             '''' as waybill_no,
             '''' as tax_rate,
             b.doc_type,
             b.dctype,
             b.gl_account doc_number,
             b.dd,
             '''' as project,
             '''' as project_cate,
             a.money as money,
             concat_ws('' '',pstng_date,b.item_text) as text,
             pstng_date as pstng_date,
             '''' as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
            (
            select pstng_date,
                   round(sum(money_nbwl_hz),2) as money,
                   module
              from fin_certificate_no_lkl_detail_bug
             where name = ''杭州''
               and pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
             group by pstng_date,module
            ) as a
            join dim_third_pay_u8 as b
            on (concat(''BR_HZ_YFZK_'',a.module) = b.flag)
      union all
      select ''北京'' as name,
             '''' as waybill_no,
             '''' as tax_rate,
             b.doc_type as doc_type,
             b.dctype as dctype,
             b.gl_account as doc_number,
             b.dd as dd,
             '''' as project,
             '''' as project_cate,
             case when a.flag = ''BR_BJ_BJ_SXF_LKL'' then a.money_sxf_bj_lkl
                  when a.flag = ''BR_BJ_LSZG_TJ_SXF_LKL'' then a.money_sxf_tj_lkl
                  when a.flag = ''BR_BJ_LSZG_HZ_SXF_LKL'' then a.money_sxf_hz_lkl
                  when a.flag = ''BR_BJ_YFZK_LKL'' then a.money_yszk_bj
                  when a.flag = ''BR_BJ_YFZK_LKL_CASH'' then a.money_yszk_cys_bj
                  when a.flag = ''BR_BJ_LSZG_TJ_NBWL_LKL'' then a.money_nbwl_cys_tj
                  when a.flag = ''BR_BJ_LSZG_HZ_NBWL_LKL'' then a.money_nbwl_cys_hz
             end as money,
             concat_ws('' '',lkl_date,a.waybill_no,b.item_text) as text,
             pstng_date as pstng_date,
             case when a.flag = ''BR_BJ_YFZK_LKL_CASH'' then a.cy_code_q
                  else b.customer end as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
            (
            select pstng_date,
                   lkl_date,
                   name,
                   system,
                   module,
                   flag,
                   cy_code_q,
                   waybill_no,
                   round(sum(money_sxf_bj_lkl),2) as money_sxf_bj_lkl,
                   round(sum(money_sxf_tj_lkl),2) as money_sxf_tj_lkl,
                   round(sum(money_sxf_hz_lkl),2) as money_sxf_hz_lkl,
                   round(sum(money_yszk_bj),2) as money_yszk_bj,
                   round(sum(money_yszk_cys_bj),2) as money_yszk_cys_bj,
                   round(sum(money_nbwl_cys_tj),2) as money_nbwl_cys_tj,
                   round(sum(money_nbwl_cys_hz),2) as money_nbwl_cys_hz
              from fin_certificate_lkl_detail_bug
             where name = ''北京''
               and pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
             group by pstng_date,lkl_date,name,system,module,flag,cy_code_q,waybill_no
            ) as a
            JOIN dim_third_pay_u8 as b
             on (a.flag = b.flag)
      union all
      select ''北京'' as name,
             ''0102'' as waybill_no,
             '''' as tax_rate,
             b.doc_type as doc_type,
             b.dctype as dctype,
             b.gl_account as doc_number,
             b.dd as dd,
             '''' as project,
             '''' as project_cate,
             round((a.money_yszk_bj + a.money_yszk_cys_bj + a.money_nbwl_cys_tj + a.money_nbwl_cys_hz) - (a.money_sxf_bj_lkl + a.money_sxf_tj_lkl + a.money_sxf_hz_lkl),2) as money,
             concat_ws('' '',lkl_date,b.item_text) as text,
             pstng_date as pstng_date,
             '''' as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
            (
            select pstng_date,
                   lkl_date,
                   substr(module,1,3) as module,
                   sum(money_sxf_bj_lkl) as money_sxf_bj_lkl,
                   sum(money_sxf_tj_lkl) as money_sxf_tj_lkl,
                   sum(money_sxf_hz_lkl) as money_sxf_hz_lkl,
                   sum(money_yszk_bj) as money_yszk_bj,
                   sum(money_yszk_cys_bj) as money_yszk_cys_bj,
                   sum(money_nbwl_cys_tj) as money_nbwl_cys_tj,
                   sum(money_nbwl_cys_hz) as money_nbwl_cys_hz
              from fin_certificate_lkl_detail_bug
             where name = ''北京''
               and pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
             group by pstng_date,lkl_date,substr(module,1,3)
            ) as a
            JOIN dim_third_pay_u8 as b
             on (concat(''BR_BJ_YHCK_'',a.module) = b.flag)
      union all
      select ''天津'' as name,
             '''' as waybill_no,
             '''' as tax_rate,
             b.doc_type as doc_type,
             b.dctype as dctype,
             b.gl_account as doc_number,
             b.dd as dd,
             '''' as project,
             '''' as project_cate,
             case when a.flag = ''BR_TJ_YFZK_LKL'' then a.money_yszk_tj
                  when a.flag = ''BR_TJ_YFZK_LKL_CASH'' then a.money_yszk_cys_tj
             end as money,
             concat_ws('' '',lkl_date,a.waybill_no,b.item_text) as text,
             pstng_date as pstng_date,
             case when a.flag = ''BR_TJ_YFZK_LKL_CASH'' then a.cy_code_q
                  else b.customer end as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
            (
            select pstng_date,
                   lkl_date,
                   name,
                   system,
                   module,
                   flag,
                   cy_code_q,
                   waybill_no,
                   sum(money_yszk_tj) as money_yszk_tj,
                   sum(money_yszk_cys_tj) as money_yszk_cys_tj
              from fin_certificate_lkl_detail_bug
             where name = ''天津''
               and pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
             group by pstng_date,lkl_date,name,system,module,flag,cy_code_q,waybill_no
            ) as a
            JOIN dim_third_pay_u8 as b
             on (a.flag = b.flag)
        union all
        select ''天津'' as name,
               '''' as waybill_no,
               '''' as tax_rate,
               b.doc_type as doc_type,
               b.dctype as dctype,
               b.gl_account as doc_number,
               b.dd as dd,
               '''' as project,
               '''' as project_cate,
               round(a.money_yszk_tj,2) as money,
               concat_ws('' '',lkl_date,b.item_text) as text,
               pstng_date as pstng_date,
               '''' as customer,
               ''1'' as status,
               unix_timestamp() as created_at,
               unix_timestamp() as updated_at
          from
              (
              select pstng_date,
                     lkl_date,
                     name,
                     system,
                     ''LKL'' as module,
                     sum(money_yszk_tj + money_yszk_cys_tj) as money_yszk_tj
                from fin_certificate_lkl_detail_bug
               where name = ''天津''
                 and pstng_date >= @pstng_date_start                       
                 and pstng_date <= @pstng_date_end
               group by pstng_date,lkl_date,name,system
              ) as a
              JOIN dim_third_pay_u8 as b
               on (concat(''BR_TJ_NBWL_'',a.module) = b.flag)
      union all
      select ''杭州'' as name,
             '''' as waybill_no,
             '''' as tax_rate,
             b.doc_type as doc_type,
             b.dctype as dctype,
             b.gl_account as doc_number,
             b.dd as dd,
             '''' as project,
             '''' as project_cate,
             case when a.flag = ''BR_HZ_YFZK_LKL'' then a.money_yszk_hz
                  when a.flag = ''BR_HZ_YFZK_LKL_CASH'' then a.money_yszk_cys_hz
             end as money,
             concat_ws('' '',lkl_date,a.waybill_no,b.item_text) as text,
             pstng_date as pstng_date,
             case when a.flag = ''BR_HZ_YFZK_LKL_CASH'' then a.cy_code_q
                  else b.customer end as customer,
             ''1'' as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
            (
            select pstng_date,
                   lkl_date,
                   name,
                   system,
                   module,
                   flag,
                   cy_code_q,
                   waybill_no,
                   round(sum(money_yszk_hz),2) as money_yszk_hz,
                   round(sum(money_yszk_cys_hz),2) as money_yszk_cys_hz
              from fin_certificate_lkl_detail_bug
             where name = ''杭州''
               and pstng_date >= @pstng_date_start                       
               and pstng_date <= @pstng_date_end
             group by pstng_date,lkl_date,name,system,module,flag,cy_code_q,waybill_no
            ) as a
            JOIN dim_third_pay_u8 as b
             on (a.flag = b.flag)
        union all
        select ''杭州'' as name,
               '''' as waybill_no,
               '''' as tax_rate,
               b.doc_type as doc_type,
               b.dctype as dctype,
               b.gl_account as doc_number,
               b.dd as dd,
               '''' as project,
               '''' as project_cate,
               round(a.money_yszk_hz,2) as money,
               concat_ws('' '',lkl_date,b.item_text) as text,
               pstng_date as pstng_date,
               '''' as customer,
               ''1'' as status,
               unix_timestamp() as created_at,
               unix_timestamp() as updated_at
          from
              (
              select pstng_date,
                     lkl_date,
                     name,
                     system,
                     ''LKL'' as module,
                     sum(money_yszk_hz + money_yszk_cys_hz) as money_yszk_hz
                from fin_certificate_lkl_detail_bug
               where name = ''杭州''
                 and pstng_date >= @pstng_date_start                       
                 and pstng_date <= @pstng_date_end
               group by pstng_date,lkl_date,name,system
              ) as a
              JOIN dim_third_pay_u8 as b
               on (concat(''BR_HZ_NBWL_'',a.module) = b.flag)';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
      insert into fin_certificate_result
      (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
      select a.name,
             '''' as receipt_id,
             '''' as tax_rate,
             b.doc_type,
             b.dctype,
             b.gl_account doc_number,
             b.dd,
             '''' as project,
             '''' as project_cate,
             sub_money as money,
             concat_ws('' '',pstng_date,b.item_text) as text,
             pstng_date as pstng_date,
             '''' as customer,
             '''' as vendor,
             1 as status,
             unix_timestamp() as created_at,
             unix_timestamp() as updated_at
        from
      (
      select name,pstng_date,dctype,flag,sub_money
        from
      (
      select name,pstng_date,dctype,
             case when dctype = ''BR1'' then ''BR_BJ_YFZK_WX_SM''
                  when dctype = ''BR2'' then ''BR_BJ_YFZK_WX_APP''
                  when dctype = ''BR3'' then ''BR_BJ_YFZK_WX_GZH''
                  when dctype = ''BR4'' then ''BR_BJ_YFZK_ZFB''
                  when dctype = ''BR13'' then ''BR_BJ_YFZK_LKL''
                  when dctype = ''BR14'' then ''BR_TJ_YFZK_LKL''
                  when dctype = ''BR15'' then ''BR_HZ_YFZK_LKL''
                  when dctype = ''BR17'' then ''BR_BJ_YFZK_ZFB_SM''
                  else ''other''
              end flag,
             round(round(sum(case when cd_type = ''C'' then money end),2) - round(sum(case when cd_type = ''D'' then money end),2),2) as sub_money
        from fin_certificate_bank_result_bug
       where pstng_date >= @pstng_date_start                       
         and pstng_date <= @pstng_date_end
      group by name,pstng_date,dctype
      ) a
      where sub_money <> 0
      ) as a
      JOIN dim_third_pay_u8 as b
      on (a.flag = b.flag)
      union all
      select name, waybill_no as receipt_id, tax_rate, dctype, cd_type, doc_number, dd, project, project_cate, money, text,
             pstng_date, customer,'''' as vendor, status, created_at, updated_at
        from fin_certificate_bank_result_bug
       where pstng_date >= @pstng_date_start                       
         and pstng_date <= @pstng_date_end';


  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into bank_num; 
end
//

#call fin_vocher_bank_result_bug('2017-08-01','2017-08-01',@bank_num);





