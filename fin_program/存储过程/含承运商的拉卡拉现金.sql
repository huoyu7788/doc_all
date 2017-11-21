select a.*,c.f_code
from
(select name,trade_id,money,count_fee,rate,transaction_time,
        lkl_trans_time 
   from mds_fin_third_pay where dt = '2017-03-31' and module = 'LKL_CASH') as a
left join
(select waybill_no, f_code
  from(select waybill_no, wumartcompany_id, zone_id, trans_uid
         from ods.ods_tms_order_waybill where dt = '2017-11-07'
      ) wb
  join(select uid, company_id
         from ods.ods_tms_trans_user where dt = '2017-11-07'
      ) user on user.uid = wb.trans_uid
  join(select company_id, f_code, zone_id
         from ods.ods_tms_trans_company where dt = '2017-11-07'
      ) tc on tc.company_id = user.company_id and wb.zone_id = tc.zone_id
) as c
on (a.trade_id = c.waybill_no)