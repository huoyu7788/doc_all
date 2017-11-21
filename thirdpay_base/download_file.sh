#!bin/bash

#从服务器上下载一个文件
#Date:2016-11-23

user=$1
IP=$2
remotedir=$3
localdir=$4

scp $user@$IP:$remotedir/* $localdir
#echo `$user@$IP:$remotedir/* $localdir`
#scp inf@192.168.60.152:/home/payment/bill/alipay/201612/06/* /home/inf/thirdpay/bill/alipay/201612/06/

#正常下载为1,否则为0 
 if [ $? -eq 0 ]; then
      echo "1"
 else echo "0"
 fi

