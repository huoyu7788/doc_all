g&(meuVJt3gO

mysql -h192.168.60.59 -uroot -P5200 -Dlsh_vrm </home/inf/zhaoning/voucher.sql> /home/inf/zhaoning/voucher_data/temp130.log

mysql -h192.168.60.59 -uroot -P5200 -Dlsh_vrm_qa
call fin_voucher_data('2017-08-01','2017-09-15');
call fin_voucher('RE','2017-09-15','2017-09-15');
call fin_voucher('JISHOU_RE','2017-09-15','2017-09-15');
call fin_voucher('ZV2','2017-08-01','2017-09-15');
call fin_voucher('RE_FC','2017-09-15','2017-09-15');
call fin_voucher('RV_FC','2017-09-15','2017-09-15');
call fin_voucher('ZV2_CY','2017-09-15','2017-09-15');
call fin_voucher('ZV2_LS','2017-09-15','2017-09-15');
call fin_voucher('KA_ZV2','2017-09-15','2017-09-15');
call fin_voucher('BSDS_DR','2017-09-15','2017-09-15');
call fin_voucher('BSDS_RV','2017-09-15','2017-09-15');
call fin_voucher('BSDS_WE','2017-09-15','2017-09-15');
call fin_voucher('KA_RV','2017-09-15','2017-09-15');
call fin_voucher('RV','2017-08-01','2017-09-15');


mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm

call fin_voucher_data('2017-09-22','2017-09-22');
call fin_voucher('RE','2017-09-15','2017-09-15');
call fin_voucher('JISHOU_RE','2017-09-15','2017-09-15');
call fin_voucher('ZV2','2017-09-15','2017-09-15');
call fin_voucher('RE_FC','2017-09-15','2017-09-15');
call fin_voucher('RV_FC','2017-09-15','2017-09-15');
call fin_voucher('ZV2_CY','2017-09-15','2017-09-15');
call fin_voucher('ZV2_LS','2017-09-15','2017-09-15');
call fin_voucher('KA_ZV2','2017-09-15','2017-09-15');
call fin_voucher('BSDS_DR','2017-09-15','2017-09-15');
call fin_voucher('BSDS_RV','2017-09-15','2017-09-15');
call fin_voucher('BSDS_WE','2017-09-15','2017-09-15');
call fin_voucher('KA_RV','2017-09-15','2017-09-15');
call fin_voucher('RV','2017-09-15','2017-09-15');

call fin_voucher('CASH','2017-09-15','2017-09-15');
call fin_voucher('THIRD','2017-09-15','2017-09-15');
call fin_voucher('BANK','2017-09-15','2017-09-15');


call fin_voucher('THIRD_BUG','2017-09-15','2017-09-15');
call fin_voucher('CASH_BUG','2017-09-15','2017-09-15');
call fin_voucher('BANK_BUG','2017-09-15','2017-09-15');

call fin_voucher('KZ','2017-09-01','2017-09-11');

call fin_voucher('ALL','','');



补数据
call fin_voucher_data_modify('2017-09-15','2017-09-15');
call fin_voucher_re_modify('2017-09-15','2017-09-15',@re_sum);
call fin_voucher_re_fc_modify('2017-09-15','2017-09-15',@re_fc_sum);

call fin_voucher('JISHOU_RE','2017-09-15','2017-09-15');
call fin_voucher('ZV2','2017-09-15','2017-09-15');
call fin_voucher('RV_FC','2017-09-15','2017-09-15');
call fin_voucher('ZV2_CY','2017-09-15','2017-09-15');
call fin_voucher('ZV2_LS','2017-09-15','2017-09-15');
call fin_voucher('KA_ZV2','2017-09-15','2017-09-15');
call fin_voucher('BSDS_DR','2017-09-15','2017-09-15');
call fin_voucher('BSDS_RV','2017-09-15','2017-09-15');
call fin_voucher('BSDS_WE','2017-09-15','2017-09-15');
call fin_voucher('KA_RV','2017-09-15','2017-09-15');
call fin_voucher('RV','2017-09-15','2017-09-15');

call fin_voucher('CASH','2017-09-15','2017-09-15');
call fin_voucher('THIRD','2017-09-15','2017-09-15');
call fin_voucher('BANK','2017-09-15','2017-09-15');


call fin_voucher('THIRD_BUG','2017-09-15','2017-09-15');
call fin_voucher('CASH_BUG','2017-09-15','2017-09-15');
call fin_voucher('BANK_BUG','2017-09-15','2017-09-15');

call fin_voucher('KZ','2017-09-15','2017-09-15');

call fin_voucher('ALL','','');


call fin_voucher_data(date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('RE',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('ZV2',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('RE_FC',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('RV_FC',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('ZV2_CY',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('ZV2_LS',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('KA_ZV2',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('BSDS_DR',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('BSDS_RV',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('BSDS_WE',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('KA_RV',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));
call fin_voucher('RV',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));





call fin_voucher_data('2017-09-06','2017-09-06');
call fin_voucher('RE','2017-09-06','2017-09-06');
call fin_voucher('JISHOU_RE','2017-09-06','2017-09-06');
call fin_voucher('ZV2','2017-09-06','2017-09-06');
call fin_voucher('RE_FC','2017-09-06','2017-09-06');
call fin_voucher('RV_FC','2017-09-17','2017-09-17');
call fin_voucher('ZV2_CY','2017-09-06','2017-09-06');
call fin_voucher('ZV2_LS','2017-09-06','2017-09-06');
call fin_voucher('KA_ZV2','2017-09-06','2017-09-06');
call fin_voucher('BSDS_DR','2017-09-06','2017-09-06');
call fin_voucher('BSDS_RV','2017-09-06','2017-09-06');
call fin_voucher('BSDS_WE','2017-09-06','2017-09-06');
call fin_voucher('KA_RV','2017-09-06','2017-09-06');
call fin_voucher('RV','2017-09-06','2017-09-06');

call fin_voucher('THIRD','2017-09-15','2017-09-15');
call fin_voucher('CASH','2017-09-15','2017-09-15');
call fin_voucher('BANK','2017-09-15','2017-09-15');

call fin_voucher('THIRD_BUG','2017-09-15','2017-09-15');
call fin_voucher('CASH_BUG','2017-09-15','2017-09-15');
call fin_voucher('BANK_BUG','2017-09-15','2017-09-15');


select name,sum(money) from fin_voucher where pstng_date >= '2017-09-15' and pstng_date <= '2017-09-09' and dctype = 'RV' and money > 0
and doc_number in ('224103','6001010101','6001010201','2221010101','2221010104')
group by name


#call fin_voucher('ALL',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));

/usr/bin/mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm </home/work/voucher/voucher.sql> /home/work/voucher/voucher_data/voucher_`date -d "1 days ago" +"%Y%m%d"`.log

sh /home/work/wumart/u8.sh `date -d "1 days ago" +"%Y-%m-%d"` `date -d "1 days ago" +"%Y-%m-%d"` > /home/work/wumart/log/u8_log_`date -d "1 days ago" +"%Y%m%d"`.log




/usr/bin/mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm </home/work/voucher/voucher_2.sql> /home/work/voucher/voucher_data/voucher_`date -d "2 days ago" +"%Y%m%d"`.log

sh /home/work/wumart/u8.sh `date -d "2 days ago" +"%Y-%m-%d"` `date -d "2 days ago" +"%Y-%m-%d"` > /home/work/wumart/log/u8_log_`date -d "2 days ago" +"%Y%m%d"`.log





