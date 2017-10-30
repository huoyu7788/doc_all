drop table mds_fin_waybill_sale_detail;
CREATE TABLE `mds_fin_waybill_sale_detail` (
  `zone_id` varchar(16) DEFAULT NULL COMMENT '区域id',
  `zone_name` varchar(10) DEFAULT NULL COMMENT '地区',
  `order_id` bigint(32) DEFAULT NULL COMMENT '订单号',
  `waybill_id` varchar(32) DEFAULT NULL COMMENT '运单号',
  `sale_id` bigint(32) DEFAULT NULL COMMENT '销售单id',
  `pay_type` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '支付方式',
  `final_amount` decimal(15,4) DEFAULT NULL COMMENT '单据实收',
  `afs_amount` decimal(15,4) DEFAULT NULL COMMENT '优惠金额',
  `user_type` tinyint(3) DEFAULT NULL COMMENT '用户类型：1-普通用户 ,2-KA用户, 3-承运商,4-链商',
  `customer` varchar(32) DEFAULT NULL COMMENT '承运商财务编码',
  `product_code` varchar(32) DEFAULT NULL COMMENT '物美编码',
  `product_name` varchar(50) DEFAULT NULL COMMENT '商品名称',
  `price` decimal(15,4) DEFAULT NULL COMMENT '原售价',
  `qty` int(10) DEFAULT NULL COMMENT '销售数量',
  `real_qty` int(10) DEFAULT NULL COMMENT '销售数量EA',
  `tax` tinyint(3) DEFAULT NULL COMMENT '税率 17/13/11/0',
  `average_price` decimal(15,4) DEFAULT NULL COMMENT '移动平均价 取发货单日期价格or签收单日期价格',
  `nt_price` decimal(15,4) DEFAULT NULL COMMENT '未税进货价',
  `type` tinyint(3) DEFAULT NULL COMMENT '商品模式 0-物美，1-寄售,2-代销',
  `vendor` varchar(32) DEFAULT NULL COMMENT '商品所属供商账务编码',
  `user_fin_code` varchar(16) NOT NULL DEFAULT ' COMMENT '客户财务编码 KA编码/收入编码/承运商编码',
  `pstng_date` varchar(16) DEFAULT NULL COMMENT '数据同步时间，精确到天',
  `provider` varchar(32) DEFAULT NULL,
  KEY `idx_zone_id` (`zone_id`),
  KEY `idx_user_type` (`user_type`),
  KEY `idx_pstng_date` (`pstng_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

insert into mds_fin_waybill_sale_detail
select a.zone_id,a.zone_name,a.order_id,a.waybill_id,a.sale_id,a.pay_type,a.final_amount,a.afs_amount,a.user_type,
      a.fin_code as customer,b.product_code,b.product_name,b.price,b.qty,b.real_qty,b.tax,
      b.average_price,b.nt_price,b.type,b.fin_code as vendor,a.user_fin_code,a.pstng_date,a.provider
 from 
      (select zone_id,zone_name,waybill_id,sale_id,order_id,pay_type,final_amount,day as pstng_date,user_type,fin_code,user_fin_code,day,afs_amount,provider
         from mall_sale_head
        where day >= '2017-01-01'                       
          and day <= '2017-06-30'          
      ) as a 
      join 
      (select a.zone_id,a.sale_id,a.product_code,a.product_name,a.price,a.qty,a.real_qty,a.average_price,a.nt_price,
              a.tax,a.type,a.fin_code
         from mall_sale_detail as a 
        where day >= '2017-01-01'                       
          and day <= '2017-06-30'
          and type in (0,1,2,3)
          and not exists(select zone_id,sale_id 
                          from mall_sale_detail as b 
                         where day >= '2017-01-01'                       
                           and day <= '2017-06-30'
                           and (fin_code = ' or fin_code is null)
                           and type = 2
                           and a.sale_id = b.sale_id 
                           and a.zone_id = b.zone_id)
      ) as b
      on (a.zone_id = b.zone_id and a.sale_id = b.sale_id);


drop table mds_fin_waybill_return_detail;
CREATE TABLE `mds_fin_waybill_return_detail` (
  `zone_id` varchar(16) DEFAULT NULL COMMENT '区域id',
  `zone_name` varchar(10) DEFAULT NULL COMMENT '地区',
  `return_id` bigint(32) DEFAULT NULL COMMENT '退货单ID',
  `waybill_id` varchar(32) DEFAULT NULL COMMENT '运单号',
  `receipt_id` bigint(32) DEFAULT NULL COMMENT '原签收单号',
  `second_receipt_id` bigint(32) DEFAULT NULL COMMENT '二次签收单号',
  `it_amount` decimal(15,4) DEFAULT NULL COMMENT '退货含税总额',
  `nt_amount` decimal(15,4) DEFAULT NULL COMMENT '未税总额',
  `user_type` tinyint(3) DEFAULT NULL COMMENT '用户类型：1-普通用户 ,2-KA用户, 3-承运商,4-链商',
  `product_code` varchar(32) DEFAULT NULL COMMENT '物美编码',
  `product_name` varchar(50) DEFAULT NULL COMMENT '商品名称',
  `qty` int(10) DEFAULT NULL COMMENT '退货数量',
  `real_qty` int(10) DEFAULT NULL COMMENT '销售数量EA',
  `sale_price` decimal(15,4) DEFAULT NULL COMMENT '商品原售价',
  `average_price` decimal(15,4) DEFAULT NULL COMMENT '商品移动平均价',
  `tax` tinyint(3) DEFAULT NULL COMMENT '税率 17/13/11/0',
  `type` tinyint(3) DEFAULT NULL COMMENT '商品模式 0-物美，1-寄售,2-代销',
  `vendor` varchar(32) DEFAULT NULL COMMENT '商品所属供商账务编码',
  `pstng_date` varchar(16) DEFAULT NULL COMMENT '时间',
  KEY `idx_zone_id` (`zone_id`),
  KEY `idx_pstng_date` (`pstng_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

insert into mds_fin_waybill_return_detail 
select a.zone_id,a.zone_name,a.return_id,a.receipt_id,c.waybill_id,a.second_receipt_id,a.it_amount,a.nt_amount,a.user_type,
    b.product_code,b.product_name,b.qty,b.real_qty,b.sale_price,b.average_price,b.tax,b.type,b.fin_code as vendor,a.pstng_date
from (select zone_id,zone_name,return_id,receipt_id,second_receipt_id,it_amount,user_type,
            nt_amount,day as pstng_date
       from mall_return_head
      where day >= '2017-01-01'                       
        and day <= '2017-06-30'
    ) as a
    left join 
    (select waybill_id,sale_id from mall_sale_head where day >= '2017-01-01' and day <= '2017-06-30') as c 
    on (a.second_receipt_id = c.sale_id)
    join
    (select zone_id,zone_name,return_id,product_code,product_name,sale_price,
            average_price,qty,real_qty,tax,type,fin_code
       from mall_return_detail as a
      where day >= '2017-01-01'                       
        and day <= '2017-06-30'
        and type in (0,1,2,3)
        and not exists(select zone_id,return_id 
                        from mall_return_detail as b 
                       where day >= '2017-01-01'                       
                         and day <= '2017-06-30'
                         and (fin_code = ' or fin_code is null)
                         and type = 2
                         and a.return_id = b.return_id 
                         and a.zone_id = b.zone_id)
    ) as b
    on (a.zone_id = b.zone_id and a.return_id = b.return_id)



drop table fin_certificate_waybill_rv_detail;
CREATE TABLE `fin_certificate_waybill_rv_detail` (
  `zone_name` varchar(10) DEFAULT NULL COMMENT '区域',
  `waybill_id` varchar(32) DEFAULT NULL COMMENT '运单',
  `receipt_id` varchar(32) DEFAULT NULL COMMENT '签收单',
  `pstng_date` varchar(16) DEFAULT NULL COMMENT '时间',
  `all_money` decimal(16,2) DEFAULT NULL COMMENT '单据金额',
  `money` decimal(16,2) DEFAULT NULL COMMENT '单据实收金额',
  `dx` decimal(16,2) DEFAULT NULL COMMENT '代销金额',
  KEY `idx_zone_name` (`zone_name`),
  KEY `idx_waybill_id` (`waybill_id`),
  KEY `idx_pstng_date` (`pstng_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



drop table fin_certificate_waybill_rv_detail_1;
create table fin_certificate_waybill_rv_detail_1 as 
select a.zone_name,a.waybill_id,a.receipt_id,' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
     case when f.gl_account = '11220202' then a.customer else ' end as customer,' as vendor,f.dd,
     a.money,
     concat_ws(' ',a.pstng_date,f.item_text) text,
     f.project,
     f.project_cate,
     a.pstng_date
from (
      select zone_name,waybill_id,sale_id as receipt_id,round(max(final_amount),2) as money,pstng_date,customer,
             case when pay_type=3 then '现金'
                  when pay_type=1 then '支付宝'
                  when pay_type=5 then '拉卡拉'
                  when pay_type=6 then '微信-扫码'
                  when pay_type=2 then '微信-APP'
                  when pay_type=8 then '支付宝-扫码'
                  when pay_type=9 then '赊销'
                  when pay_type=0 then '现金'
                  end as type
        from mds_fin_waybill_sale_detail
       where user_type = 1
       group by zone_name,waybill_id,sale_id,pstng_date,customer,pay_type
      ) as a
      join dim_fin_certificate_info_u8 f on f.flag=TYPE and a.zone_name=f.area;


drop table fin_certificate_waybill_rv_detail_2;
create table fin_certificate_waybill_rv_detail_2 as 
select a.zone_name,waybill_id,receipt_id,a.tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,' as customer,' as vendor,f.dd,f.project,f.project_cate,
     round(money,2)*pn as money,concat_ws(' ',pstng_date,item_text) text,pstng_date
FROM (
select zone_name,waybill_id,sale_id as receipt_id,pstng_date,tax as tax_rate,
     round(sum(price*real_qty/(1+tax/100)),2) money,
     concat('NO',case when type = 0 then 'WUMART' 
                        when type = 1 then 'JISHOU' 
                        end,'_',tax) flag
from mds_fin_waybill_sale_detail
where type in (0,1)
 and user_type = 1
 and vendor <> '999999'
group by zone_name,waybill_id,sale_id,pstng_date,tax,type 
union all
select zone_name,waybill_id,sale_id as receipt_id,pstng_date,tax as tax_rate,
     round(sum(price*real_qty*(tax/100)/(1+tax/100)),2) money,
     concat(case when type = 0 then 'WUMART' 
                 when type = 1 then 'JISHOU' 
                 end,'_',tax) flag
from mds_fin_waybill_sale_detail
where type in (0,1)
 and user_type = 1
 and vendor <> '999999'
group by zone_name,waybill_id,sale_id,pstng_date,tax,type 
union all
select zone_name,waybill_id,receipt_id,pstng_date,tax as tax_rate,
     round(sum(sale_price*real_qty/(1+tax/100)),2) money,
     concat('SH_NO',case when type = 0 then 'WUMART' 
                           when type = 1 then 'JISHOU' 
                           end,'_',tax) flag
from 
(
select zone_id,
      zone_name,
      return_id,waybill_id,second_receipt_id as receipt_id,product_code,real_qty,pstng_date,sale_price,tax,type,vendor
 from mds_fin_waybill_return_detail
where type in (0,1)
  and user_type = 1
  and vendor <> '999999'
) as a
group by zone_name,waybill_id,receipt_id,pstng_date,tax,type
union all
select zone_name,waybill_id,receipt_id,pstng_date,tax as tax_rate,
     round(sum(sale_price*real_qty*(tax/100)/(1+tax/100)),2) money,
     concat('SH_',case when type = 0 then 'WUMART' 
                         when type = 1 then 'JISHOU' 
                         end,'_',tax) flag
from  
(select zone_id,zone_name,return_id,waybill_id,second_receipt_id as receipt_id,product_code,real_qty,sale_price,tax,type,pstng_date
 from mds_fin_waybill_return_detail
where type in (0,1)
  and user_type = 1
  and vendor <> '999999'
) as b
group by zone_name,waybill_id,receipt_id,pstng_date,tax,type
) a join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area;


drop table fin_certificate_waybill_rv_detail_3;
create table fin_certificate_waybill_rv_detail_3 as 
select a.zone_name,a.waybill_id,a.receipt_id,' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
     a.vendor as customer,' as vendor,f.dd,f.project,f.project_cate,round(a.money,2)*f.pn as money,concat_ws(' ',a.pstng_date,item_text) text,a.pstng_date
from
(
select a.zone_name,a.waybill_id,a.receipt_id,a.pstng_date,round(sum(money),2) money,flag,vendor 
from 
(
select zone_name,waybill_id,sale_id as receipt_id,pstng_date,
     price*real_qty money,
     case when type = 2 then 'DAIXIAO'
          when type = 2 and vendor = '01020007' then 'SH_BSDS'
          end flag,
     vendor
from mds_fin_waybill_sale_detail
where type in (2,4)
 and user_type = 1
 and vendor <> '999999'
) as a 
group by zone_name,waybill_id,receipt_id,pstng_date,flag,vendor
union all
select zone_name,waybill_id,receipt_id,pstng_date,round(sum(money),2) as money,flag,vendor  
from 
(
select zone_name,waybill_id,second_receipt_id as receipt_id,pstng_date,
     sale_price*real_qty money,
     concat('SH_',case when type = 2 then 'DAIXIAO'
                        when type = 2 and vendor = '01020007' then 'BSDS'
                    end) flag,
     vendor
 from mds_fin_waybill_return_detail
where type in (2,4)
  and user_type = 1
  and vendor <> '999999'
) as a 
group by zone_name,waybill_id,receipt_id,pstng_date,vendor,flag
) as a 
join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area;


insert into fin_certificate_waybill_rv_detail
select zone_name,waybill_id,sale_id as receipt_id,pstng_date,
     round(sum(price*real_qty),2) all_money,round(max(final_amount),2) money,
     round(sum(case when type = 2 then price*real_qty else 0 end),2) dx
from mds_fin_waybill_sale_detail
where user_type = 1
 and vendor <> '999999'
group by zone_name,waybill_id,sale_id,pstng_date;


drop table fin_certificate_waybill_rv_detail_4;
create table fin_certificate_waybill_rv_detail_4 as 
select 
      a.zone_name,
      a.waybill_id,
      a.receipt_id,
      a.tax_rate,
      f.doc_type,
      f.dctype,
      f.gl_account doc_number,
      f.dd,
      f.project,
      f.project_cate,
      (round((s_money*(all_money-money-afs_money)/(all_money))/(1+a.tax_rate/100) ,2))*f.pn money,
      concat_ws(' ',a.pstng_date,item_text) text,
      a.pstng_date,
      a.flag,
      ' as customer,
      ' as vendor,
      '1' status
from (
select a.zone_name,a.waybill_id,a.receipt_id,a.tax_rate,a.s_money,a.pstng_date,a.flag,n.all_money,n.money,a.afs_money
from (
select zone_name,pstng_date,waybill_id,sale_id as receipt_id,
     case when type = 0 or type = 1 then tax else 0 end tax_rate,
     concat('NO',
            case when type = 0 then 'WUMART' 
                 when type = 1 then 'JISHOU' 
                 when type = 2 then 'DAIXIAO'
                 end,
            '_',
            case when type = 0 or type = 1 then tax else 0 end,'_ZK') flag,
     sum(price*real_qty) s_money,
     max(afs_amount) afs_money
from mds_fin_waybill_sale_detail
where user_type = 1
 and vendor <> '999999'
group by zone_name,waybill_id,sale_id,pstng_date,type,tax 
) as a
join 
(select waybill_id,receipt_id,pstng_date,all_money,money
 from fin_certificate_waybill_rv_detail
) as n 
on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date and a.waybill_id = n.waybill_id
) as a
join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area;

drop table fin_certificate_waybill_rv_detail_5;
create table fin_certificate_waybill_rv_detail_5 as 
select a.zone_name,
      a.waybill_id,
      a.receipt_id,
      a.tax_rate,
      f.doc_type,
      f.dctype,
      f.gl_account doc_number,
      f.dd,
      f.project,
      f.project_cate,
      (round( (s_money*(all_money-money-afs_money)*(a.tax_rate/100)/(all_money))/(1+a.tax_rate/100) ,2))*f.pn money,
      concat_ws(' ',a.pstng_date,item_text) text,
      a.pstng_date,
      a.flag,
      ' as customer,
      ' as vendor,
      '1' status
from (
select a.zone_name,a.waybill_id,a.receipt_id,a.tax_rate,a.s_money,a.pstng_date,a.flag,n.all_money,n.money,a.afs_money
from (
select zone_name,pstng_date,waybill_id,sale_id as receipt_id,
     case when type = 0 or type = 1 then tax else 0 end tax_rate,
     concat(case when type = 0 then 'WUMART' 
                 when type = 1 then 'JISHOU' 
                 when type = 2 then 'DAIXIAO'
                 end,
            '_',
            case when type = 0 or type = 1 then tax else 0 end,'_ZK') flag,
     sum(price*real_qty) s_money,
     max(afs_amount) afs_money
from mds_fin_waybill_sale_detail
where user_type = 1
 and vendor <> '999999'
group by zone_name,waybill_id,sale_id,pstng_date,type,tax 
) as a
join 
(select waybill_id,receipt_id,pstng_date,all_money,money
 from fin_certificate_waybill_rv_detail
) as n 
on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date and a.waybill_id = n.waybill_id
) as a
join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area;


drop table fin_certificate_rv_detail_all_modify;
create table fin_certificate_rv_detail_all_modify as 
select zone_name,waybill_id,receipt_id,tax_rate,doc_type,dctype,doc_number,
     case when doc_number = '11220202' and zone_name = '北京' and (customer = '' or customer is null) then '01010013'
          when doc_number = '11220202' and zone_name = '天津' and (customer = '' or customer is null) then '02010003'
          when doc_number = '11220202' and zone_name = '杭州' and (customer = '' or customer is null) then '03010001'
          else customer end as customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_waybill_rv_detail_1
union all
select zone_name,waybill_id,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_waybill_rv_detail_2
union all
select zone_name,waybill_id,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_waybill_rv_detail_3
union all
select zone_name,waybill_id,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_waybill_rv_detail_4
union all
select zone_name,waybill_id,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_waybill_rv_detail_5;

drop table fin_certificate_rv_modify;
create table fin_certificate_rv_modify as 
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,'1' status,
  unix_timestamp() created_at,unix_timestamp() updated_at
from (
  select zone_name,receipt_id,' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,f.customer as customer,' as vendor,f.dd,
         case when f.gl_account = '6001010101' or f.gl_account = '6001010203' then '0101' else ' end as project,
         case when f.gl_account = '6001010101' or f.gl_account = '6001010203' then '01' else ' end as project_cate,
         money,concat_ws(' ',pstng_date,f.item_text) text,pstng_date
  from (
      SELECT zone_name,receipt_id,pstng_date,
          'RV_D' flag,
          sub_money money
      from (
          select zone_name,receipt_id,pstng_date,
              round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
          from fin_certificate_rv_detail_all_modify as a
          group by zone_name,receipt_id,pstng_date
      ) a
      where round(sub_money,2) <> 0
  ) a
  join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
  union all
  select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
    from fin_certificate_rv_detail_all_modify as a 
) b;

drop table all_data_01_06;
create table all_data_01_06 as 
select a.*,b.waybill_id
  from 
(select * from fin_certificate_rv_modify) as a 
join 
(select waybill_id,receipt_id from fin_certificate_waybill_rv_detail) as b 
on (a.receipt_id = b.receipt_id)




drop table fin_certificate_rv_modify;
drop table fin_certificate_rv_detail_all_modify;
drop table fin_certificate_waybill_rv_detail_5;
drop table fin_certificate_waybill_rv_detail_4;
drop table fin_certificate_waybill_rv_detail_3;
drop table fin_certificate_waybill_rv_detail_2;
drop table fin_certificate_waybill_rv_detail_1;



#KA_RVKA_RVKA_RVKA_RVKA_RVKA_RVKA_RVKA_RVKA_RVKA_RVKA_RV
drop table fin_certificate_ka_rv_detail_1;
create table fin_certificate_ka_rv_detail_1 as 
select a.zone_name,a.receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
     case when f.gl_account = '112203' then a.customer else '' end as customer,'' as vendor,f.dd,
     a.money,
     concat_ws(' ',a.pstng_date,f.item_text) text,
     f.project,
     f.project_cate,
     a.pstng_date
from (
      select zone_name,sale_id as receipt_id,round(max(final_amount),2) as money,pstng_date,user_fin_code as customer,
             case when pay_type=3 then '现金'
                  when pay_type=1 then '支付宝'
                  when pay_type=5 then '拉卡拉'
                  when pay_type=6 then '微信-扫码'
                  when pay_type=2 then '微信-APP'
                  when pay_type=8 then '支付宝-扫码'
                  when pay_type=9 then '赊销'
                  when pay_type=0 then '现金'
                  end as type
        from mds_fin_waybill_sale_detail
       where pstng_date >= '2017-01-01'                       
         and pstng_date <= '2017-06-30'
         and user_type = 2
         and type in (0,1,2)
       group by zone_name,sale_id,pstng_date,customer,pay_type 
      ) as a
      join dim_fin_certificate_info_u8 f on f.flag=TYPE and a.zone_name=f.area;

drop table fin_certificate_ka_rv_detail_2;
create table fin_certificate_ka_rv_detail_2 as 
select a.zone_name,receipt_id,a.tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,'' as customer,'' as vendor,f.dd,f.project,f.project_cate,
   money*pn as money,concat_ws(' ',pstng_date,item_text) text,pstng_date
FROM (
select zone_name,sale_id as receipt_id,pstng_date,tax as tax_rate,
   round(sum(price*real_qty/(1+tax/100)),2) money,
   concat('NO',case when type = 0 then 'WUMART' 
                      when type = 1 then 'JISHOU' 
                      end,'_',tax) flag
from mds_fin_waybill_sale_detail
where type in (0,1)
and pstng_date >= '2017-01-01'
and pstng_date <= '2017-06-30'
and user_type = 2
group by zone_name,sale_id,pstng_date,tax,type 
union all
select zone_name,sale_id as receipt_id,pstng_date,tax as tax_rate,
   round(sum(price*real_qty*(tax/100)/(1+tax/100)),2) money,
   concat(case when type = 0 then 'WUMART' 
               when type = 1 then 'JISHOU' 
               end,'_',tax) flag
from mds_fin_waybill_sale_detail
where type in (0,1)
and pstng_date >= '2017-01-01'
and pstng_date <= '2017-06-30'
and user_type = 2
group by zone_name,sale_id,pstng_date,tax,type 
union all
select zone_name,second_receipt_id as receipt_id,pstng_date,tax as tax_rate,
   round(sum(sale_price*real_qty/(1+tax/100)),2) money,
   concat('SH_NO',case when type = 0 then 'WUMART' 
                         when type = 1 then 'JISHOU' 
                         end,'_',tax) flag
from 
(
select zone_id,
    zone_name,
    return_id,second_receipt_id,product_code,real_qty,pstng_date,sale_price,tax,type,vendor
from mds_fin_waybill_return_detail
where type in (0,1)
and user_type = 2
and pstng_date >= '2017-01-01'
and pstng_date <= '2017-06-30'
) as a
group by zone_name,second_receipt_id,pstng_date,tax,type
union all
select zone_name,second_receipt_id as receipt_id,pstng_date,tax as tax_rate,
   round(sum(sale_price*real_qty*(tax/100)/(1+tax/100)),2) money,
   concat('SH_',case when type = 0 then 'WUMART' 
                       when type = 1 then 'JISHOU' 
                       end,'_',tax) flag
from  
(select zone_id,zone_name,return_id,second_receipt_id,product_code,real_qty,sale_price,tax,type,pstng_date
from mds_fin_waybill_return_detail
where type in (0,1)
and user_type = 2
and pstng_date >= '2017-01-01'
and pstng_date <= '2017-06-30'
) as b
group by zone_name,second_receipt_id,pstng_date,tax,type
) a join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area;


drop table fin_certificate_ka_rv_detail_3;
create table fin_certificate_ka_rv_detail_3 as 
select a.zone_name,a.receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
     a.vendor as customer,'' as vendor,f.dd,f.project,f.project_cate,a.money*f.pn as money,concat_ws(' ',a.pstng_date,item_text) text,a.pstng_date
from
(
select a.zone_name,a.receipt_id,a.pstng_date,round(sum(money),2) money,flag,vendor 
from 
(
select zone_name,sale_id as receipt_id,pstng_date,
     price*real_qty money,
     case when type = 2 then 'DAIXIAO'
          when type = 2 and vendor = '01020007' then 'SH_BSDS'
          end flag,
     vendor
from mds_fin_waybill_sale_detail
where type = 2
 and pstng_date >= '2017-01-01'
 and pstng_date <= '2017-06-30'
 and user_type = 2
) as a 
group by zone_name,receipt_id,pstng_date,flag,vendor
union all
select zone_name,receipt_id,pstng_date,round(sum(money),2) as money,flag,vendor  
from 
(
select zone_name,second_receipt_id as receipt_id,pstng_date,
     sale_price*real_qty money,
     concat('SH_',case when type = 2 then 'DAIXIAO'
                        when type = 2 and vendor = '01020007' then 'BSDS'
                    end) flag,
     vendor
 from mds_fin_waybill_return_detail
where type = 2
  and user_type = 2
  and pstng_date >= '2017-01-01'
  and pstng_date <= '2017-06-30'
) as a 
group by zone_name,receipt_id,pstng_date,vendor,flag
) as a 
join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area;


insert into fin_certificate_ka_rv_detail
select zone_name,sale_id as receipt_id,pstng_date,
     round(sum(price*real_qty),2) all_money,round(max(final_amount),2) money,
     round(sum(case when type = 2 then price*real_qty else 0 end),2) dx
from mds_fin_waybill_sale_detail
where user_type = 2
 and type in (0,1,2)
 and pstng_date >= '2017-01-01'
 and pstng_date <= '2017-06-30'
group by zone_name,sale_id,pstng_date;

drop table fin_certificate_ka_rv_detail_4;
create table fin_certificate_ka_rv_detail_4 as 
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
      concat_ws(' ',a.pstng_date,item_text) text,
      a.pstng_date,
      a.flag,
      '' as customer,
      '' as vendor,
      '1' status
from (
select a.zone_name,a.receipt_id,a.tax_rate,a.s_money,a.pstng_date,a.flag,n.all_money,n.money,a.afs_money
from (
select zone_name,pstng_date,sale_id as receipt_id,
     case when type = 0 or type = 1 then tax else 0 end tax_rate,
     concat('NO',
            case when type = 0 then 'WUMART' 
                 when type = 1 then 'JISHOU' 
                 when type = 2 then 'DAIXIAO'
                 end,
            '_',
            case when type = 0 or type = 1 then tax else 0 end,'_ZK') flag,
     sum(price*real_qty) s_money,
     max(afs_amount) afs_money
from mds_fin_waybill_sale_detail
where user_type = 2
 and type in (0,1,2)
 and pstng_date >= '2017-01-01'
 and pstng_date <= '2017-06-30'
group by zone_name,sale_id,pstng_date,type,tax 
) as a
join 
(select receipt_id,pstng_date,all_money,money
 from fin_certificate_ka_rv_detail
) as n 
on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date
) as a
join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area;

drop table  fin_certificate_ka_rv_detail_5;
create table fin_certificate_ka_rv_detail_5 as 
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
      concat_ws(' ',a.pstng_date,item_text) text,
      a.pstng_date,
      a.flag,
      '' as customer,
      '' as vendor,
      '1' status
from (
select a.zone_name,a.receipt_id,a.tax_rate,a.s_money,a.pstng_date,a.flag,n.all_money,n.money,a.afs_money
from (
select zone_name,pstng_date,sale_id as receipt_id,
     case when type = 0 or type = 1 then tax else 0 end tax_rate,
     concat(case when type = 0 then 'WUMART' 
                 when type = 1 then 'JISHOU' 
                 when type = 2 then 'DAIXIAO'
                 end,
            '_',
            case when type = 0 or type = 1 then tax else 0 end,'_ZK') flag,
     sum(price*real_qty) s_money,
     max(afs_amount) afs_money
from mds_fin_waybill_sale_detail
where user_type = 2
 and type in (0,1,2)
 and pstng_date >= '2017-01-01'
 and pstng_date <= '2017-06-30'
group by zone_name,sale_id,pstng_date,type,tax 
) as a
join 
(select receipt_id,pstng_date,all_money,money
 from fin_certificate_ka_rv_detail
) as n 
on a.receipt_id=n.receipt_id and a.pstng_date = n.pstng_date
) as a
join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area;



insert into fin_certificate_ka_rv_detail_all 
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_ka_rv_detail_1
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_ka_rv_detail_2
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_ka_rv_detail_3
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_ka_rv_detail_4
union all
select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
from fin_certificate_ka_rv_detail_5;




insert into fin_certificate_ka_rv

create table fin_certificate_ka_rv_modify as 
select zone_name,receipt_id,tax_rate,'KA_RV' as doc_type,dctype,doc_number,dd,project,project_cate,money,text,pstng_date,customer,vendor,'1' status,
  unix_timestamp() created_at,unix_timestamp() updated_at
from (
  select zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account doc_number,f.customer as customer,'' as vendor,f.dd,
         case when f.gl_account = '6001010101' or f.gl_account = '6001010203' then '0101' else '' end as project,
         case when f.gl_account = '6001010101' or f.gl_account = '6001010203' then '01' else '' end as project_cate,
         money,concat_ws(' ',pstng_date,f.item_text) text,pstng_date
  from (
      SELECT zone_name,receipt_id,pstng_date,
          'RV_D' flag,
          sub_money money

      from (
          select zone_name,receipt_id,pstng_date,
              round(round(sum(case when dctype='C' then money else 0 end),2) - round(sum(case when dctype='D' then money else 0 end),2),2) sub_money
          from fin_certificate_ka_rv_detail_all
          group by zone_name,receipt_id,pstng_date
      ) a
      where round(sub_money,2)<>0
  ) a
  join dim_fin_certificate_info_u8 f on f.flag=a.flag and a.zone_name=f.area
  union all
  select zone_name,receipt_id,tax_rate,doc_type,dctype,doc_number,customer,vendor,dd,project,project_cate,money,text,pstng_date
  from fin_certificate_ka_rv_detail_all
) b;




drop table all_ka_data_01_06;
create table all_ka_data_01_06 as 
select a.*,b.waybill_id
  from 
(select * from fin_certificate_ka_rv_modify) as a 
join 
(select waybill_id,sale_id from mall_sale_head where day >= '2017-01-01' and day <= '2017-06-30') as b 
on (a.receipt_id = b.sale_id)

mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm -e "
select 
concat('\'',zone_name),
concat('\'',waybill_id),
concat('\'',receipt_id),
concat('\'',tax_rate),
concat('\'',doc_type),
concat('\'',dctype),
concat('\'',doc_number),
concat('\'',dd),
concat('\'',project),
concat('\'',project_cate),
concat('\'',money),
concat('\'',text),
concat('\'',pstng_date),concat('\'',customer),concat('\'',vendor) from all_data_01_06 where pstng_date = '2017-01-01' "

cd /home/work
iconv -f utf8 -t gbk 01.xls -o 001.xls



iconv -f utf8 -t gbk 01.xls -o 001.xls


mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm -e "select concat('\'',zone_name), from all_data_01_06 where pstng_date = '2017-01-01' " > /home/work/01.xls

sh fin.sh all_data_01_06 2017-01-01 2017-01-31
sh fin.sh all_data_01_06 2017-02-01 2017-02-31
sh fin.sh all_data_01_06 2017-03-01 2017-03-31
sh fin.sh all_data_01_06 2017-04-01 2017-04-31
sh fin.sh all_data_01_06 2017-05-01 2017-05-31
sh fin.sh all_data_01_06 2017-06-01 2017-06-31


sh fin.sh all_ka_data_01_06 2017-01-01 2017-01-31
sh fin.sh all_ka_data_01_06 2017-02-01 2017-02-31
sh fin.sh all_ka_data_01_06 2017-03-01 2017-03-31
sh fin.sh all_ka_data_01_06 2017-04-01 2017-04-31
sh fin.sh all_ka_data_01_06 2017-05-01 2017-05-31
sh fin.sh all_ka_data_01_06 2017-06-01 2017-06-31


   

#ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2ZV2
create table fin_certificate_zv2_detail as 
select a.zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
       a.customer,a.vendor,f.dd,money*f.pn as money,concat_ws(' ',pstng_date,item_text) as text,pstng_date
from (
select zone_name,receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       flag
from 
(
select zone_name,sale_id as receipt_id,average_price*real_qty money,tax as tax_rate,pstng_date,
       concat('ZV2_',case when type = 0 then 'WUMART'
                            when type = 1 then 'JISHOU' end) as flag
  from mds_fin_waybill_sale_detail
 where type in (0,1)
   and pstng_date >= '2017-01-01'
   and pstng_date <= '2017-06-30'
   and user_type = 1
   and vendor <> '999999'
) as a  
group by zone_name,receipt_id,pstng_date,flag
union all
select zone_name,receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       flag
from 
(
select zone_name,sale_id as receipt_id,average_price*real_qty money,tax as tax_rate,pstng_date,
       concat('ZV2_KC_',case when type = 0 then 'WUMART' 
                               when type = 1 then 'JISHOU' end) as flag
  from mds_fin_waybill_sale_detail
 where type in (0,1)
   and pstng_date >= '2017-01-01'
   and pstng_date <= '2017-06-30'
   and user_type = 1
   and vendor <> '999999'
) as a  
group by zone_name,receipt_id,pstng_date,flag
union all
select zone_name,second_receipt_id as receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       concat('ZV2_SH_',case when type = 0 then 'WUMART' 
                               when type = 1 then 'JISHOU' end) as flag
  from 
(select zone_id,zone_name,return_id,second_receipt_id,average_price*real_qty money,tax as tax_rate,type,pstng_date
   from mds_fin_waybill_return_detail
  where type in (0,1)
    and user_type = 1
    and pstng_date >= '2017-01-01'
    and pstng_date <= '2017-06-30'
    and vendor <> '999999'
) as b
group by zone_name,second_receipt_id,pstng_date,type
union all
select zone_name,second_receipt_id as receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       concat('ZV2_SH_KC_',case when type = 0 then 'WUMART' 
                                  when type = 1 then 'JISHOU' end) as flag
  from 
(select zone_id,zone_name,return_id,second_receipt_id,average_price*real_qty money,tax as tax_rate,type,pstng_date
   from mds_fin_waybill_return_detail
  where type in (0,1)
    and user_type = 1
    and pstng_date >= '2017-01-01'
    and pstng_date <= '2017-06-30'
    and vendor <> '999999'
) as b
group by zone_name,second_receipt_id,pstng_date,type
) as a
join dim_fin_certificate_info_u8 f on f.flag=a.flag and zone_name=f.area;



drop table all_zv2_data_01_06;
create table  all_zv2_data_01_06 as 
select a.*,b.waybill_id,'' as project,'' as project_cate
  from 
(select * from fin_certificate_zv2_detail) as a 
join 
(select waybill_id,sale_id from mall_sale_head where day >= '2017-01-01' and day <= '2017-06-30') as b 
on (a.receipt_id = b.sale_id)



sh fin.sh all_zv2_data_01_06 2017-01-01 2017-01-31
sh fin.sh all_zv2_data_01_06 2017-02-01 2017-02-31
sh fin.sh all_zv2_data_01_06 2017-03-01 2017-03-31
sh fin.sh all_zv2_data_01_06 2017-04-01 2017-04-31
sh fin.sh all_zv2_data_01_06 2017-05-01 2017-05-31
sh fin.sh all_zv2_data_01_06 2017-06-01 2017-06-31



#KA_ZV2KA_ZV2KA_ZV2KA_ZV2KA_ZV2KA_ZV2KA_ZV2KA_ZV2KA_ZV2KA_ZV2KA_ZV2
create table fin_certificate_ka_zv2_detail as 
select a.zone_name,receipt_id,'' tax_rate,f.doc_type,f.dctype,f.gl_account as doc_number,
       a.customer,a.vendor,f.dd,money*f.pn as money,concat_ws(' ',pstng_date,item_text) as text,pstng_date
from (
select zone_name,receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       flag
from 
(
select zone_name,sale_id as receipt_id,average_price*real_qty money,tax as tax_rate,pstng_date,
       concat('ZV2_',case when type = 0 then 'WUMART'
                            when type = 1 then 'JISHOU' end) as flag
  from mds_fin_waybill_sale_detail
 where type in (0,1)
   and pstng_date >= '2017-01-01'
   and pstng_date <= '2017-06-30'
   and user_type = 2
) as a  
group by zone_name,receipt_id,pstng_date,flag
union all
select zone_name,receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       flag
from 
(
select zone_name,sale_id as receipt_id,average_price*real_qty money,tax as tax_rate,pstng_date,
       concat('ZV2_KC_',case when type = 0 then 'WUMART' 
                               when type = 1 then 'JISHOU' end) as flag
  from mds_fin_waybill_sale_detail
 where type in (0,1)
   and pstng_date >= '2017-01-01'
   and pstng_date <= '2017-06-30'
   and user_type = 2
) as a  
group by zone_name,receipt_id,pstng_date,flag
union all
select zone_name,second_receipt_id as receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       concat('ZV2_SH_',case when type = 0 then 'WUMART' 
                               when type = 1 then 'JISHOU' end) as flag
  from 
(select zone_id,zone_name,return_id,second_receipt_id,average_price*qty money,tax as tax_rate,type,pstng_date
   from mds_fin_waybill_return_detail
  where type in (0,1)
    and pstng_date >= '2017-01-01'
    and pstng_date <= '2017-06-30'
    and user_type = 2
) as b
group by zone_name,second_receipt_id,pstng_date,type
union all
select zone_name,second_receipt_id as receipt_id,'' as customer,'' as vendor,pstng_date,
       round(sum(money),2) money,
       concat('ZV2_SH_KC_',case when type = 0 then 'WUMART' 
                                  when type = 1 then 'JISHOU' end) as flag
  from 
(select zone_id,zone_name,return_id,second_receipt_id,average_price*qty money,tax as tax_rate,type,pstng_date
   from mds_fin_waybill_return_detail
  where type in (0,1)
    and pstng_date >= '2017-01-01'
    and pstng_date <= '2017-06-30'
    and user_type = 2
) as b
group by zone_name,second_receipt_id,pstng_date,type
) as a
join dim_fin_certificate_info_u8 f on f.flag=a.flag and zone_name=f.area;


drop table all_ka_zv2_data_01_06;
create table  all_ka_zv2_data_01_06 as 
select a.*,b.waybill_id,'' as project,'' as project_cate
  from 
(select * from fin_certificate_ka_zv2_detail) as a 
join 
(select waybill_id,sale_id from mall_sale_head where day >= '2017-01-01' and day <= '2017-06-30') as b 
on (a.receipt_id = b.sale_id);



sh fin.sh all_ka_zv2_data_01_06 2017-01-01 2017-01-31
sh fin.sh all_ka_zv2_data_01_06 2017-02-01 2017-02-31
sh fin.sh all_ka_zv2_data_01_06 2017-03-01 2017-03-31
sh fin.sh all_ka_zv2_data_01_06 2017-04-01 2017-04-31
sh fin.sh all_ka_zv2_data_01_06 2017-05-01 2017-05-31
sh fin.sh all_ka_zv2_data_01_06 2017-06-01 2017-06-31



