#!bin/bash

#############################################
#
# Author: zhaoning
# Date: 2017-08-22
# Describe:create voucher data
#
#############################################
 source /home/work/.bash_profile
      
#时间处理
if [ $1"0" == "0" ]
then

dateago=`date -d "${date} 1 days ago" +"%Y%m%d"`

datepath=`date -d "${date} 1 days ago" +"%Y%m/%d"`

day=`date -d "${date} 1 days ago" +"%Y-%m-%d"`

else

dateago=`date -d "$1 0 days ago" +"%Y%m%d"`

datepath=`date -d "$1 0 days ago" +"%Y%m/%d"`

day=`date -d "$1 0 days ago" +"%Y-%m-%d"`

fi

echo $datepath



#本地地址

localdir=/home/work/zhaoning/voucher/voucher_data/
localdir=/home/work/voucher/voucher_data/
#删除目录

#rm -rf $localdir/temp_${dateago}

#创建目录

#mkdir -p $localdir/temp_${dateago}


#mysql -h192.168.60.59 -uroot -P5200 -Dlsh_vrm_qa </home/inf/zhaoning/voucher/voucher.sql> $localdir/voucher_${dateago}.log  
mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm </home/work/voucher/voucher.sql> $localdir/voucher_${dateago}.log
#mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 -Dlsh_vrm </home/work/voucher/voucher_modify.sql> $localdir/voucher_20170912.log
if [$? -ne 0 ];then
    echo "失败"
    #SendMail -t zhaoning -o "lsh_vrm data load  is err " -u  "${dateago}"
else
   echo "成功"
fi





