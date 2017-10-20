
DROP PROCEDURE IF EXISTS fin_voucher_rv_new;  
  delimiter //
  create procedure fin_voucher_rv_new 
  (
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10), /*结束日期*/
   out rv_num int
   )
  begin 
  set @pstng_date_start = pstng_date_start;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  truncate table fin_certificate_rv_detail;
  drop table if exists fin_certificate_rv_detail_1;
  drop table if exists fin_certificate_rv_detail_2;
  drop table if exists fin_certificate_rv_detail_3;
  drop table if exists fin_certificate_rv_detail_4;
  drop table if exists fin_certificate_rv_detail_5;
  truncate table fin_certificate_rv_detail_all;


  set @strsql = 'delete from fin_certificate_result where dctype = ''RV''';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = 'delete from fin_certificate_rv where pstng_date >= @pstng_date_start and pstng_date <= @pstng_date_end';
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
    create table fin_certificate_rv_detail_1 as 
    select a.zone_name,a.receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
           case when f.gl_account = ''11220202'' then a.customer else '''' end as customer,'''' as vendor,f.dd,
           a.money,
           concat_ws('' '',a.pstng_date,f.item_text) text,
           f.project,
           f.project_cate,
           a.pstng_date
      from (
            select zone_name,sale_id as receipt_id,round(max(final_amount),2) as money,pstng_date,customer,
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
               and user_type = 1
               and type in (0,1,2)
             group by zone_name,sale_id,pstng_date,customer,pay_type
            ) as a
            join dim_fin_certificate_info_u8 f on f.flag=TYPE and a.zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
    create table fin_certificate_rv_detail_2 as 
    select a.zone_name,receipt_id,a.tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,'''' as customer,'''' as vendor,f.dd,f.project,f.project_cate,
           round(money,2)*pn as money,concat_ws('' '',pstng_date,item_text) text,pstng_date
    FROM (
    select zone_name,sale_id as receipt_id,pstng_date,tax as tax_rate,
           round(sum(price*real_qty/(1+tax/100)),2) money,
           concat(''NO'',case when type = 0 then ''WUMART'' 
                              when type = 1 then ''JISHOU'' 
                              end,''_'',tax) flag
      from mds_fin_sale_detail
     where type in (0,1)
       and pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
       and user_type = 1
       and vendor <> ''999999''
     group by zone_name,sale_id,pstng_date,tax,type 
    union all
    select zone_name,sale_id as receipt_id,pstng_date,tax as tax_rate,
           round(sum(price*real_qty*(tax/100)/(1+tax/100)),2) money,
           concat(case when type = 0 then ''WUMART'' 
                       when type = 1 then ''JISHOU'' 
                       end,''_'',tax) flag
      from mds_fin_sale_detail
     where type in (0,1)
       and pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
       and user_type = 1
       and vendor <> ''999999''
     group by zone_name,sale_id,pstng_date,tax,type 
    union all
    select zone_name,second_receipt_id as receipt_id,pstng_date,tax as tax_rate,
           round(sum(sale_price*real_qty/(1+tax/100)),2) money,
           concat(''SH_NO'',case when type = 0 then ''WUMART'' 
                                 when type = 1 then ''JISHOU'' 
                                 end,''_'',tax) flag
      from 
    (
    select zone_id,
            zone_name,
            return_id,second_receipt_id,product_code,real_qty,pstng_date,sale_price,tax,type,vendor
       from mds_fin_return_detail
      where type in (0,1)
        and user_type = 1
        and pstng_date >= @pstng_date_start
        and pstng_date <= @pstng_date_end
        and vendor <> ''999999''
    ) as a
    group by zone_name,second_receipt_id,pstng_date,tax,type
    union all
    select zone_name,second_receipt_id as receipt_id,pstng_date,tax as tax_rate,
           round(sum(sale_price*real_qty*(tax/100)/(1+tax/100)),2) money,
           concat(''SH_'',case when type = 0 then ''WUMART'' 
                               when type = 1 then ''JISHOU'' 
                               end,''_'',tax) flag
      from  
    (select zone_id,zone_name,return_id,second_receipt_id,product_code,real_qty,sale_price,tax,type,pstng_date
       from mds_fin_return_detail
      where type in (0,1)
        and user_type = 1
        and pstng_date >= @pstng_date_start
        and pstng_date <= @pstng_date_end
        and vendor <> ''999999''
    ) as b
    group by zone_name,second_receipt_id,pstng_date,tax,type
    ) a join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


  set @strsql = '
    create table fin_certificate_rv_detail_3 as 
    select a.zone_name,a.receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
           a.vendor as customer,'''' as vendor,f.dd,f.project,f.project_cate,round(a.money,2)*f.pn as money,concat_ws('' '',a.pstng_date,item_text) text,a.pstng_date
    from
    (
    select a.zone_name,a.receipt_id,a.pstng_date,round(sum(money),2) money,flag,vendor 
      from 
    (
    select zone_name,sale_id as receipt_id,pstng_date,
           price*real_qty money,
           case when type = 2 then ''DAIXIAO''
                when type = 2 and vendor = ''01020007'' then ''SH_BSDS''
                end flag,
           vendor
      from mds_fin_sale_detail
     where type = 2
       and user_type = 1
       and pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
       and vendor <> ''999999''
    ) as a 
     group by zone_name,receipt_id,pstng_date,flag,vendor
    union all
    select zone_name,receipt_id,pstng_date,round(sum(money),2) as money,flag,vendor  
      from 
    (
    select zone_name,second_receipt_id as receipt_id,pstng_date,
           sale_price*real_qty money,
           concat(''SH_'',case when type = 2 then ''DAIXIAO''
                              when type = 2 and vendor = ''01020007'' then ''BSDS''
                          end) flag,
           vendor
       from mds_fin_return_detail
      where type = 2
        and user_type = 1
        and pstng_date >= @pstng_date_start
        and pstng_date <= @pstng_date_end
        and vendor <> ''999999''
    ) as a 
    group by zone_name,receipt_id,pstng_date,vendor,flag
    ) as a 
    join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area';
  
  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


 set @strsql = '
    insert into fin_certificate_rv_detail
    select zone_name,sale_id as receipt_id,pstng_date,
           round(sum(price*real_qty),2) all_money,round(max(final_amount),2) money,
           round(sum(case when type = 2 then price*real_qty else 0 end),2) dx
      from mds_fin_sale_detail
     where user_type = 1
       and type in (0,1,2)
       and pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
       and vendor <> ''999999''
     group by zone_name,sale_id,pstng_date ';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;


 set @strsql = '
    create table fin_certificate_rv_detail_4 as 
    select 
            a.zone_name,
            a.receipt_id,
            a.tax_rate,
            f.doc_type,
            f.dctype,
            f.gl_account doc_number,
            f.dd,
            f.project,
            f.project_cate,
            (round((s_money*(all_money-money-afs_money)/(all_money))/(1+a.tax_rate/100) ,2))*f.pn money,
            concat_ws('' '',a.pstng_date,item_text) text,
            a.pstng_date,
            a.flag,
            '''' as customer,
            '''' as vendor,
            ''1'' status
    from (
    select a.zone_name,a.receipt_id,a.tax_rate,a.s_money,a.pstng_date,a.flag,n.all_money,n.money,a.afs_money
    from (
    select zone_name,pstng_date,sale_id as receipt_id,
           case when type = 0 or type = 1 then tax else 0 end tax_rate,
           concat(''NO'',
                  case when type = 0 then ''WUMART'' 
                       when type = 1 then ''JISHOU'' 
                       when type = 2 then ''DAIXIAO''
                       end,
                  ''_'',
                  case when type = 0 or type = 1 then tax else 0 end,''_ZK'') flag,
           sum(price*real_qty) s_money,
           max(afs_amount) afs_money
      from mds_fin_sale_detail
     where user_type = 1
       and type in (0,1,2)
       and pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
       and vendor <> ''999999''
     group by zone_name,sale_id,pstng_date,type,tax 
    ) as a
    join 
    (select receipt_id,pstng_date,all_money,money
       from fin_certificate_rv_detail
    ) as n 
    on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date
    ) as a
    join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
    create table fin_certificate_rv_detail_5 as 
    select a.zone_name,
            a.receipt_id,
            a.tax_rate,
            f.doc_type,
            f.dctype,
            f.gl_account doc_number,
            f.dd,
            f.project,
            f.project_cate,
            (round( (s_money*(all_money-money-afs_money)*(a.tax_rate/100)/(all_money))/(1+a.tax_rate/100) ,2))*f.pn money,
            concat_ws('' '',a.pstng_date,item_text) text,
            a.pstng_date,
            a.flag,
            '''' as customer,
            '''' as vendor,
            ''1'' status
    from (
    select a.zone_name,a.receipt_id,a.tax_rate,a.s_money,a.pstng_date,a.flag,n.all_money,n.money,a.afs_money
    from (
    select zone_name,pstng_date,sale_id as receipt_id,
           case when type = 0 or type = 1 then tax else 0 end tax_rate,
           concat(case when type = 0 then ''WUMART'' 
                       when type = 1 then ''JISHOU'' 
                       when type = 2 then ''DAIXIAO''
                       end,
                  ''_'',
                  case when type = 0 or type = 1 then tax else 0 end,''_ZK'') flag,
           sum(price*real_qty) s_money,
           max(afs_amount) afs_money
      from mds_fin_sale_detail
     where user_type = 1
       and type in (0,1,2)
       and pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
       and vendor <> ''999999''
     group by zone_name,sale_id,pstng_date,type,tax 
    ) as a
    join 
    (select receipt_id,pstng_date,all_money,money
       from fin_certificate_rv_detail
    ) as n 
    on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date
    ) as a
    join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_rv_detail_all 
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,
           case when doc_number = ''11220202'' and zone_name = ''北京'' and (customer = '''' or customer is null) then ''01010013''
                when doc_number = ''11220202'' and zone_name = ''天津'' and (customer = '''' or customer is null) then ''02010003''
                when doc_number = ''11220202'' and zone_name = ''杭州'' and (customer = '''' or customer is null) then ''03010001''
                else customer end as customer,vendor,dd,project,project_cate,money,text,pstng_date
    from fin_certificate_rv_detail_1
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
    from fin_certificate_rv_detail_2
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
    from fin_certificate_rv_detail_3
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
    from fin_certificate_rv_detail_4
    union all
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
    from fin_certificate_rv_detail_5';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
        insert into fin_sale_bug
        SELECT zone_name,receipt_id,sub_money money,pstng_date,type
          from (select zone_name,receipt_id,pstng_date,
                       round(round(sum(case when dctype=''C'' then money else 0 end),2) - 
                       round(sum(case when dctype=''D'' then money else 0 end),2),2) sub_money,
                       ''RV'' as type
                  from fin_certificate_rv_detail_all
                 group by zone_name,receipt_id,pstng_date
                ) a
         where abs(sub_money)>0.1';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_rv
    select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,''1'' status,
        unix_timestamp() created_at,unix_timestamp() updated_at
    from (
        select zone_name,receipt_id,'''' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,f.customer as customer,'''' as vendor,f.dd,
               case when f.gl_account = ''6001010101'' or f.gl_account = ''6001010203'' then ''0101'' else '''' end as project,
               case when f.gl_account = ''6001010101'' or f.gl_account = ''6001010203'' then ''01'' else '''' end as project_cate,
               money,concat_ws('' '',pstng_date,f.item_text) text,pstng_date
        from (
            SELECT zone_name,receipt_id,pstng_date,
                ''RV_D'' flag,
                sub_money money
            from (
                select zone_name,receipt_id,pstng_date,
                    round(round(sum(case when dctype=''C'' then money else 0 end),2) - round(sum(case when dctype=''D'' then money else 0 end),2),2) sub_money
                from fin_certificate_rv_detail_all as a
               where not exists (select distinct receipt_id from fin_sale_bug as b where a.receipt_id = b.receipt_id)
                group by zone_name,receipt_id,pstng_date
            ) a
            where round(sub_money,2)<>0
        ) a
        join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
        union all
        select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
          from fin_certificate_rv_detail_all as a 
         where not exists (select distinct receipt_id from fin_sale_bug as b where a.receipt_id = b.receipt_id)
    ) b ';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql; 
  DROP PREPARE slesql;

  set @strsql = '
    insert into fin_certificate_result
    (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,status,created_at,updated_at)
    select zone_name,'''' receipt_id,'''' tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,
           sum(money) money,text,pstng_date,customer,vendor,status,created_at,updated_at
      from fin_certificate_rv
     where pstng_date >= @pstng_date_start
       and pstng_date <= @pstng_date_end
     group by zone_name,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,pstng_date,
              text,customer,vendor,status,created_at,updated_at';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into rv_num; 


  truncate table fin_certificate_rv_detail_1;
  truncate table fin_certificate_rv_detail_2;
  truncate table fin_certificate_rv_detail_3;
  truncate table fin_certificate_rv_detail_4;
  truncate table fin_certificate_rv_detail_5;

  drop table if exists fin_certificate_rv_detail_1;
  drop table if exists fin_certificate_rv_detail_2;
  drop table if exists fin_certificate_rv_detail_3;
  drop table if exists fin_certificate_rv_detail_4;
  drop table if exists fin_certificate_rv_detail_5;
  
end
//

#  call fin_voucher_rv_new('2017-09-14','2017-09-14',@rv);

