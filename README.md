# doc_all
#thirdpay.tar.gz
tar -zxvf thirdpay.tar.gz .

#voucher.tar.gz

tar -zxvf voucher.tar.gz .
cd voucher
mkdir voucher_data

0 6 * * * /usr/bin/mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm </home/work/voucher/voucher_2.sql> /home/work/voucher/voucher_data/voucher_`date -d "2 days ago" +"\%Y\%m\%d"`.log

#wumart.tar.gz

tar -zxvf wumart.tar.gz
cd wumart
mkdir data
mkdir log

0 7 * * * sh /home/work/wumart/u8.sh `date -d "2 days ago" +"\%Y-\%m-\%d"` `date -d "2 days ago" +"\%Y-\%m-\%d"` > /home/work/wumart/log/u8_log_`date -d "2 days ago" +"\%Y\%m\%d"`.log

