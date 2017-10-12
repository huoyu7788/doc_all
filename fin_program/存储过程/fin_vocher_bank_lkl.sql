drop table if exists fin_certificate_lkl_detail;
CREATE TABLE `fin_certificate_lkl_detail` (
  `name` varchar(10) COMMENT '区域',
  `money_sxf_bj_lkl` decimal(16,2) COMMENT '北京手续费',
  `money_sxf_tj_lkl` decimal(16,2) COMMENT '天津手续费',
  `money_sxf_hz_lkl` decimal(16,2) COMMENT '杭州手续费',
  `money_yszk_bj` decimal(16,2) COMMENT '北京应收账款',
  `money_yszk_tj` decimal(16,2) COMMENT '天津应收账款',
  `money_yszk_hz` decimal(16,2) COMMENT '杭州应收账款',
  `money_yszk_cys_bj` decimal(16,2) COMMENT '北京应收账款承运商',
  `money_yszk_cys_tj` decimal(16,2) COMMENT '天津应收账款承运商',
  `money_yszk_cys_hz` decimal(16,2) COMMENT '杭州应收账款承运商',
  `money_nbwl_cys_tj` decimal(16,2) COMMENT '承运商内部往来天津',
  `money_nbwl_cys_hz` decimal(16,2) COMMENT '承运商内部往来杭州',
  `system` varchar(50) COMMENT '支付唯一标示',
  `cy_code_q` varchar(20) COMMENT '承运商编码',
  `flag` varchar(50) COMMENT '唯一标示',
  `module` varchar(50) COMMENT '支付类型',
  `pstng_date` varchar(16) COMMENT '时间',
  `lkl_date` varchar(16) COMMENT '拉卡拉时间',
  `waybill_no` varchar(32) COMMENT '运单号'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP PROCEDURE IF EXISTS fin_vocher_bank_lkl;  
  delimiter //
  create procedure fin_vocher_bank_lkl 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out lkl_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end=''
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  
  set @strsql = 'delete from fin_certificate_lkl_detail where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
      insert into fin_certificate_lkl_detail
      select ''北京'' as name,
             round(money,2) as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             0 as money_yszk_tj,
             0 as money_yszk_hz,
             0 as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             0 as money_nbwl_cys_hz,
             ''BR_BJ_BJ_SXF'' as system,
             '''' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '''' waybill_no
        from
            (
            select ''LKL'' module,
                   sum(factorage) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit
             where zone_name = ''北京''
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat(''BR_BJ_BJ_SXF_'',a.module) = b.flag)
      union all
      select ''北京'' as name,
             0 as money_sxf_bj_lkl,
             round(money,2) as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             0 as money_yszk_tj,
             0 as money_yszk_hz,
             0 as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             0 as money_nbwl_cys_hz,
             ''BR_BJ_LSZG_TJ_SXF'' as system,
             '''' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '''' waybill_no
        from
            (
            select ''LKL'' as module,
                   sum(factorage) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit
             where zone_name = ''天津''
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat(''BR_BJ_LSZG_TJ_SXF_'',a.module) = b.flag)
      union all
      select ''北京'' as name,
             0 as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             round(money,2) as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             0 as money_yszk_tj,
             0 as money_yszk_hz,
             0 as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             0 as money_nbwl_cys_hz,
             ''BR_BJ_LSZG_HZ_SXF'' as system,
             '''' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '''' waybill_no
        from
            (
            select ''LKL'' as module,
                   sum(factorage) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit
             where zone_name = ''杭州''
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat(''BR_BJ_LSZG_HZ_SXF_'',a.module) = b.flag)
      union all
      select ''北京'' as name,
             0 as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             round(money,2) as money_yszk_bj,
             0 as money_yszk_tj,
             0 as money_yszk_hz,
             0 as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             0 as money_nbwl_cys_hz,
             ''BR_BJ_YFZK'' as system,
             '''' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '''' waybill_no
        from
            (
            select ''LKL'' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit
             where zone_name = ''北京''
               and zone_name is not null
               and pay_type = 5
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat(''BR_BJ_YFZK_'',a.module) = b.flag)
      union all
      select ''天津'' as name,
             0 as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             round(money,2) as money_yszk_tj,
             0 as money_yszk_hz,
             0 as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             0 as money_nbwl_cys_hz,
             ''BR_TJ_YFZK'' as system,
             '''' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '''' waybill_no
        from
            (
            select ''LKL'' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit
             where zone_name = ''天津''
               and zone_name is not null
               and pay_type = 5
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat(''BR_TJ_YFZK_'',a.module) = b.flag)
      union all
      select ''杭州'' as name,
             0 as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             0 as money_yszk_tj,
             round(money,2) as money_yszk_hz,
             0 as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             0 as money_nbwl_cys_hz,
             ''BR_HZ_YFZK'' as system,
             '''' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '''' waybill_no
        from
            (
            select ''LKL'' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit
             where zone_name = ''杭州''
               and zone_name is not null
               and pay_type = 5
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat(''BR_HZ_YFZK_'',a.module) = b.flag)
      union all
      select ''北京'' as name,
             0 as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             0 as money_yszk_tj,
             0 as money_yszk_hz,
             round(money,2) as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             0 as money_nbwl_cys_hz,
             ''BR_BJ_YFZK'' as system,
             -- ifnull(c.f_code,''01010013'') as cy_code_q,
             case when c.f_code is null or c.f_code = '''' then ''01010013'' else c.f_code end as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             a.trade_id waybill_no
        from
            (
            select ''LKL_CASH''as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date,
                   ticket_id as trade_id
              from bank_credit
             where zone_name = ''北京''
               and pay_type in (3,10,11)
               and zone_name is not null
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type,ticket_id
            ) as a
            left outer join
            (select waybill_id as waybill_no, f_code
               from waybill_fin_code
            ) as c
            on (a.trade_id = c.waybill_no)
            join
            dim_third_pay_u8 as b
            on (concat(''BR_BJ_YFZK_'',a.module) = b.flag)
      union all
      select ''天津'' as name,
             0 as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             0 as money_yszk_tj,
             0 as money_yszk_hz,
             0 as money_yszk_cys_bj,
             round(money,2) as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             0 money_nbwl_cys_hz,
             ''BR_TJ_YFZK'' as system,
             -- ifnull(c.f_code,''02010003'') as cy_code_q,
             case when c.f_code is null or c.f_code = '''' then ''02010003'' else c.f_code end as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             a.trade_id waybill_no
        from
            (
            select ''LKL_CASH'' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date,
                   ticket_id as trade_id
              from bank_credit
             where zone_name = ''天津''
               and pay_type in (3,10,11)
               and zone_name is not null
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type,ticket_id
            ) as a
            left outer join
            (select waybill_id as waybill_no, f_code
               from waybill_fin_code
            ) as c
            on (a.trade_id = c.waybill_no)
            join
            dim_third_pay_u8 as b
            on (concat(''BR_TJ_YFZK_'',a.module) = b.flag)
      union all
      select ''杭州'' as name,
             0 as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             0 as money_yszk_tj,
             0 as money_yszk_hz,
             0 as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             round(money,2) as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             0 money_nbwl_cys_hz,
             ''BR_HZ_YFZK'' as system,
             -- ifnull(c.f_code,''03010001'') as cy_code_q,
             case when c.f_code is null or c.f_code = '''' then ''03010001'' else c.f_code end as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             a.trade_id waybill_no
        from
            (
            select ''LKL_CASH'' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date,
                   ticket_id as trade_id
              from bank_credit
             where zone_name = ''杭州''
               and pay_type in (3,10,11)
               and zone_name is not null
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type,ticket_id
            ) as a
            left outer join
            (select waybill_id as waybill_no, f_code
               from waybill_fin_code
            ) as c
            on (a.trade_id = c.waybill_no)
            join
            dim_third_pay_u8 as b
            on (concat(''BR_HZ_YFZK_'',a.module) = b.flag)
      union all
      select ''北京'' as name,
             0 as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             0 as money_yszk_tj,
             0 as money_yszk_hz,
             0 as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             round(money,2) as money_nbwl_cys_tj,
             0 as money_nbwl_cys_hz,
             ''BR_BJ_LSZG_TJ_NBWL'' as system,
             '''' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '''' waybill_no
        from
            (
            select ''LKL'' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit
             where zone_name = ''天津''
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat(''BR_BJ_LSZG_TJ_NBWL_'',a.module) = b.flag)
      union all
      select ''北京'' as name,
             0 as money_sxf_bj_lkl,
             0 as money_sxf_tj_lkl,
             0 as money_sxf_hz_lkl,
             0 as money_yszk_bj,
             0 as money_yszk_tj,
             0 as money_yszk_hz,
             0 as money_yszk_cys_bj,
             0 as money_yszk_cys_tj,
             0 as money_yszk_cys_hz,
             0 as money_nbwl_cys_tj,
             round(money,2) money_nbwl_cys_hz,
             ''BR_BJ_LSZG_HZ_NBWL'' as system,
             '''' as cy_code_q,
             b.flag as flag,
             a.module as module,
             a.psting_date as pstng_date,
             a.lkl_date as lkl_date,
             '''' waybill_no
        from
            (
            select ''LKL'' as module,
                   sum(final_amount) as money,
                   day as psting_date,
                   substr(lkl_date,1,10) as lkl_date
              from bank_credit
             where zone_name = ''杭州''
               and zone_name is not null
               and pay_type in (3,5,10,11)
               and day >= @pstng_date_start                       
               and day <= @pstng_date_end
             group by substr(lkl_date,1,10),day,pay_type
            ) as a
              join
            dim_third_pay_u8 as b
            on (concat(''BR_BJ_LSZG_HZ_NBWL_'',a.module) = b.flag)';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;



  select FOUND_ROWS() into lkl_num; 
end
//

#call fin_vocher_bank_lkl('2017-08-01,'2017-08-01',@lkl_num);





