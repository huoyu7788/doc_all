
#测试环境
mysql -h192.168.60.59 -uroot -P5200 -Dlsh_vrm_qa
call fin_voucher_data(@pstng_date_start,@pstng_date_end);
call fin_voucher('RE',@pstng_date_start,@pstng_date_end);
call fin_voucher('JISHOU_RE',@pstng_date_start,@pstng_date_end);
call fin_voucher('ZV2',@pstng_date_start,@pstng_date_end);
call fin_voucher('RE_FC',@pstng_date_start,@pstng_date_end);
call fin_voucher('RV_FC',@pstng_date_start,@pstng_date_end);
call fin_voucher('ZV2_CY',@pstng_date_start,@pstng_date_end);
call fin_voucher('ZV2_LS',@pstng_date_start,@pstng_date_end);
call fin_voucher('KA_ZV2',@pstng_date_start,@pstng_date_end);
call fin_voucher('BSDS_DR',@pstng_date_start,@pstng_date_end);
call fin_voucher('BSDS_RV',@pstng_date_start,@pstng_date_end);
call fin_voucher('BSDS_WE',@pstng_date_start,@pstng_date_end);
call fin_voucher('KA_RV',@pstng_date_start,@pstng_date_end);
call fin_voucher('RV',@pstng_date_start,@pstng_date_end);


#正式环境
mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm

call fin_voucher_data(@pstng_date_start,@pstng_date_end);
call fin_voucher('RE',@pstng_date_start,@pstng_date_end);
call fin_voucher('JISHOU_RE',@pstng_date_start,@pstng_date_end);
call fin_voucher('ZV2',@pstng_date_start,@pstng_date_end);
call fin_voucher('RE_FC',@pstng_date_start,@pstng_date_end);
call fin_voucher('RV_FC',@pstng_date_start,@pstng_date_end);
call fin_voucher('ZV2_CY',@pstng_date_start,@pstng_date_end);
call fin_voucher('ZV2_LS',@pstng_date_start,@pstng_date_end);
call fin_voucher('KA_ZV2',@pstng_date_start,@pstng_date_end);
call fin_voucher('BSDS_DR',@pstng_date_start,@pstng_date_end);
call fin_voucher('BSDS_RV',@pstng_date_start,@pstng_date_end);
call fin_voucher('BSDS_WE',@pstng_date_start,@pstng_date_end);
call fin_voucher('KA_RV',@pstng_date_start,@pstng_date_end);
call fin_voucher('RV',@pstng_date_start,@pstng_date_end);

call fin_voucher('CASH',@pstng_date_start,@pstng_date_end);
call fin_voucher('THIRD',@pstng_date_start,@pstng_date_end);
call fin_voucher('BANK',@pstng_date_start,@pstng_date_end);


call fin_voucher('THIRD_BUG',@pstng_date_start,@pstng_date_end);
call fin_voucher('CASH_BUG',@pstng_date_start,@pstng_date_end);
call fin_voucher('BANK_BUG',@pstng_date_start,@pstng_date_end);


call fin_voucher('ALL','','');


#执行脚本 提前一天
#call fin_voucher('ALL',date_sub(curdate(),interval 1 day),date_sub(curdate(),interval 1 day));

/usr/bin/mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm </home/work/voucher/voucher.sql> /home/work/voucher/voucher_data/voucher_`date -d "1 days ago" +"%Y%m%d"`.log

sh /home/work/wumart/u8.sh `date -d "1 days ago" +"%Y-%m-%d"` `date -d "1 days ago" +"%Y-%m-%d"` > /home/work/wumart/log/u8_log_`date -d "1 days ago" +"%Y%m%d"`.log



#执行脚本 提前两天
/usr/bin/mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm </home/work/voucher/voucher_2.sql> /home/work/voucher/voucher_data/voucher_`date -d "2 days ago" +"%Y%m%d"`.log

sh /home/work/wumart/u8.sh `date -d "2 days ago" +"%Y-%m-%d"` `date -d "2 days ago" +"%Y-%m-%d"` > /home/work/wumart/log/u8_log_`date -d "2 days ago" +"%Y%m%d"`.log


#测试生成文件
mysql -h192.168.60.59 -uroot -P5200 -Dlsh_vrm </home/inf/zhaoning/voucher.sql> /home/inf/zhaoning/voucher_data/temp130.log



