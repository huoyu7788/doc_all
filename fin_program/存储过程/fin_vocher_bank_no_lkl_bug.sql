drop table if exists fin_certificate_no_lkl_detail_bug;
CREATE TABLE `fin_certificate_no_lkl_detail_bug` (
  `name` varchar(10) COMMENT '区域',
  `money_sxf_bj` decimal(16,2) COMMENT '北京手续费',
  `money_sxf_tj` decimal(16,2) COMMENT '天津手续费',
  `money_sxf_hz` decimal(16,2) COMMENT '杭州手续费',
  `money_yszk` decimal(16,2) COMMENT '北京应收账款',
  `money_nbwl_tj` decimal(16,2) COMMENT '内部往来天津',
  `money_nbwl_hz` decimal(16,2) COMMENT '内部往来杭州',
  `system` varchar(50) COMMENT '支付唯一标示',
  `flag` varchar(50) COMMENT '唯一标示',
  `module` varchar(50) COMMENT '支付类型',
  `pstng_date` varchar(16) COMMENT '时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



DROP PROCEDURE IF EXISTS fin_vocher_bank_no_lkl_bug;  
  delimiter //
  create procedure fin_vocher_bank_no_lkl_bug 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out no_lkl_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end=''
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  
  set @strsql = 'delete from fin_certificate_no_lkl_detail_bug where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = 'delete from bank_credit_bug where day >= @pstng_date_start and day <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
      insert into bank_credit_bug
      (id, ticket_id, it_amount, pay_amount, final_amount, rate, factorage, type, pay_type, 
       date, lkl_date, day, status, gen_evidence, check_mall_data, pay_payment_no, zone_id,
       zone_name, created_at, updated_at)
      select id, ticket_id, it_amount, pay_amount, final_amount, rate, factorage, type, 
             pay_type, date, lkl_date, day, status, gen_evidence, check_mall_data, 
             pay_payment_no, zone_id, ''北京'' zone_name, created_at, updated_at
        from bank_credit 
       where day >= @pstng_date_start 
         and day <= @pstng_date_end 
         and zone_name = ''''';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
        insert into fin_certificate_no_lkl_detail_bug
        select ''北京'' as name,
               round(money,2) as money_sxf_bj,
               0 as money_sxf_tj,
               0 as money_sxf_hz,
               0 as money_yszk,
               0 as money_nbwl_tj,
               0 as money_nbwl_hz,
               ''BR_BJ_BJ_SXF'' as system,
               b.flag as flag,
               a.module as module,
               a.psting_date as pstng_date
          from
              (
              select case when pay_type = 1 then ''ZFB''
                          when pay_type = 2 then ''WX_APP''
                          when pay_type = 6 then ''WX_SM''
                          when pay_type = 8 then ''ZFB_SM''
                     end module,
                     sum(factorage) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = ''北京''
                 -- and pay_type not in (3,5)
                 and pay_type in (1,2,6,8)
                 and day >= @pstng_date_start                       
                 and day <= @pstng_date_end
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat(''BR_BJ_BJ_SXF_'',a.module) = b.flag)
        union all
        select ''天津'' as name,
               0 as money_sxf_bj,
               round(money,2) as money_sxf_tj,
               0 as money_sxf_hz,
               0 as money_yszk,
               0 as money_nbwl_tj,
               0 as money_nbwl_hz,
               ''BR_BJ_TJ_SXF'' as system,
               b.flag as flag,
               a.module as module,
               a.psting_date as pstng_date
          from
              (
              select case when pay_type = 1 then ''ZFB''
                          when pay_type = 2 then ''WX_APP''
                          when pay_type = 6 then ''WX_SM''
                          when pay_type = 8 then ''ZFB_SM''
                     end module,
                     sum(factorage) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = ''天津''
                 -- and pay_type not in (3,5)
                 and pay_type in (1,2,6,8)
                 and day >= @pstng_date_start                       
                 and day <= @pstng_date_end
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat(''BR_BJ_TJ_SXF_'',a.module) = b.flag)
        union all
        select ''杭州'' as name,
               0 as money_sxf_bj,
               0 as money_sxf_tj,
               round(money,2) as money_sxf_hz,
               0 as money_yszk,
               0 as money_nbwl_tj,
               0 as money_nbwl_hz,
               ''BR_BJ_HZ_SXF'' as system,
               b.flag as flag,
               a.module as module,
               a.psting_date as pstng_date
          from
              (
              select case when pay_type = 1 then ''ZFB''
                          when pay_type = 2 then ''WX_APP''
                          when pay_type = 6 then ''WX_SM''
                          when pay_type = 8 then ''ZFB_SM''
                     end module,
                     sum(factorage) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = ''杭州''
                 -- and pay_type not in (3,5)
                 and pay_type in (1,2,6,8)
                 and day >= @pstng_date_start                       
                 and day <= @pstng_date_end
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat(''BR_BJ_HZ_SXF_'',a.module) = b.flag)
        union all
        select ''北京'' as name,
               0 as money_sxf_bj,
               0 as money_sxf_tj,
               0 as money_sxf_hz,
               round(a.money,2) as money_yszk,
               0 as money_nbwl_tj,
               0 as money_nbwl_hz,
               ''BR_BJ_YFZK'' as system,
               b.flag as flag,
               a.module as module,
               a.psting_date as pstng_date
          from
              (
              select case when pay_type = 1 then ''ZFB''
                          when pay_type = 2 then ''WX_APP''
                          when pay_type = 6 then ''WX_SM''
                          when pay_type = 8 then ''ZFB_SM''
                     end module,
                     sum(final_amount) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = ''北京''
                 -- and pay_type not in (3,5)
                 and pay_type in (1,2,6,8)
                 and day >= @pstng_date_start                       
                 and day <= @pstng_date_end
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat(''BR_BJ_YFZK_'',a.module) = b.flag)

        union all
        select ''天津'' as name,
               0 as money_sxf_bj,
               0 as money_sxf_tj,
               0 as money_sxf_hz,
               0 as money_yszk,
               round(a.money,2)  as money_nbwl_tj,
               0 as money_nbwl_hz,
               ''BR_BJ_TJ_NBWL'' as system,
               b.flag as flag,
               a.module as module,
               a.psting_date as pstng_date
          from
              (
              select case when pay_type = 1 then ''ZFB''
                          when pay_type = 2 then ''WX_APP''
                          when pay_type = 6 then ''WX_SM''
                          when pay_type = 8 then ''ZFB_SM''
                     end module,
                     sum(final_amount) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = ''天津''
                 -- and pay_type not in (3,5)
                 and pay_type in (1,2,6,8)
                 and day >= @pstng_date_start                       
                 and day <= @pstng_date_end
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat(''BR_BJ_TJ_NBWL_'',a.module) = b.flag)

        union all
        select ''杭州'' as name,
               0 as money_sxf_bj,
               0 as money_sxf_tj,
               0 as money_sxf_hz,
               0 as money_yszk,
               0 as money_nbwl_tj,
               round(a.money,2) as money_nbwl_hz,
               ''BR_BJ_HZ_NBWL'' as system,
               b.flag as flag,
               a.module as module,
               a.psting_date as pstng_date
          from
              (
              select case when pay_type = 1 then ''ZFB''
                          when pay_type = 2 then ''WX_APP''
                          when pay_type = 6 then ''WX_SM''
                          when pay_type = 8 then ''ZFB_SM''
                     end module,
                     sum(final_amount) as money,
                     day as psting_date
                from bank_credit_bug
               where zone_name = ''杭州''
                 -- and pay_type not in (3,5)
                 and pay_type in (1,2,6,8)
                 and day >= @pstng_date_start                       
                 and day <= @pstng_date_end
               group by day,pay_type
              ) as a
                join
              dim_third_pay_u8 as b
              on (concat(''BR_BJ_HZ_NBWL_'',a.module) = b.flag)';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  select FOUND_ROWS() into no_lkl_num; 
end
//

#call fin_vocher_bank_no_lkl_bug('2017-08-01','2017-08-01,@no_lkl_num);





