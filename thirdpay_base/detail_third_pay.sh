#!bin/bash

#############################################
#
# Author: zhaoning
# Date: 2016-12-06
# Describe:从192.168.60.152服务器下载并且解析文件
#
#############################################
 source /home/inf/.bash_profile
      
#服务器ip地址  
IP=192.168.60.152    

#用户
#user=payment
user=inf
#密码
#password=payment123


#下载文件类型
pay_type=$1


#服务器路径
basepath=/home/payment/bill


#本地路径
destdir=/home/inf/thirdpay/bill


#时间处理
if [ $2"0" == "0" ]
then

dateago=`date -d "${date} 1 days ago" +"%Y%m%d"`

datepath=`date -d "${date} 1 days ago" +"%Y%m/%d"`

day=`date -d "${date} 1 days ago" +"%Y-%m-%d"`

else

dateago=`date -d "$2 0 days ago" +"%Y%m%d"`

datepath=`date -d "$2 0 days ago" +"%Y%m/%d"`

day=`date -d "$2 0 days ago" +"%Y-%m-%d"`

fi

echo $datepath


#远程服务器地址

remotedir=$basepath/$pay_type/$datepath

#本地地址

localdir=$destdir/$pay_type/$datepath

#删除目录

rm -rf $destdir/$pay_type/$datepath

#创建目录

mkdir -p $destdir/$pay_type/$datepath


#正常下载为1 下载失败为0

 checkfile=`sh /home/inf/thirdpay/download_file.sh $user $IP $remotedir $localdir`

#验证如果checkfile表示正常下载，否则下载失败


 if [ 1 == $checkfile ]; then

echo "开始计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"`
   
   #处理拉卡拉
   
   if [ "lklpay" == $pay_type ]; then
      
      hive -S -e " load data local inpath '$localdir/lkl_bill_${dateago}.txt' overwrite into table ods.ods_fin_third_pay_lkl partition (dt='$day') "
     
      SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_lkl -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D
   
      SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_lkl -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D
   
   fi
   
   #处理微信app、gzh
   
   if [ "wxpay" == $pay_type ]; then
     
       hive -S -e " load data local inpath '$localdir/wx_app_bill_${dateago}.txt' overwrite into table ods.ods_fin_third_pay_wxapp partition (dt='$day') "
          
       SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_wxapp -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D
 
       SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_wxapp -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D
      
       hive -S -e " load data local inpath '$localdir/wx_h5_bill_${dateago}.txt' overwrite into table ods.ods_fin_third_pay_wxgzh partition (dt='$day') "
      
       SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_wxgzh -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D
       
       SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_wxgzh -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D
   
   fi
   #处理支付宝/微信扫码
   if [ "qfpay" == $pay_type ]; then

      hive -S -e " load data local inpath '$localdir/qf_alism_bill_${day}.txt' overwrite into table ods.ods_fin_third_pay_alism partition (dt='$day') "
      
      SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_alism -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D    
     
      SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_alism -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

      hive -S -e " load data local inpath '$localdir/qf_wxsm_bill_${day}.txt' overwrite into table ods.ods_fin_third_pay_wxsm partition (dt='$day') "

      SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_wxsm -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

      SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_wxsm -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D
   
   fi

   #处理支付宝
   if [ "alipay" == $pay_type ]; then
       
       unzip -q $localdir/* -d $localdir/

       convmv -f GBK -t utf8 --notest -r $localdir/
       
       hive -S -e " load data local inpath '$localdir/20881212878624920156_${dateago}_业务明细.csv' overwrite into table ods.ods_fin_third_pay_zfb partition (dt='$day') "
       
       SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_zfb -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D   
    
       SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_zfb -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

   fi

   #处理支付宝
   if [ "ztpay" == $pay_type ]; then
       
       sed -i 's/|/,/g' $localdir/${dateago}.txt
       
       hive -S -e " load data local inpath '$localdir/${dateago}.txt' overwrite into table ods.ods_fin_third_pay_zt partition (dt='$day') "
       
       SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_zt -p delete -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D   
    
       SetTagStatus -d ${day} -l day -b ods.ods_fin_third_pay_zt -p create -n zhaoning -v F71596DBD2C47F3F1BF1CF305A6D3E8D

   fi

 echo "执行成功"
   
 else

   echo "执行失败"

   SendMail -t zhaoning -o "$pay_type${dateago}$ data load  is err " -u  "$pay_type${dateago}"


 fi

echo "结束计算数据 ：" `date +"%Y-%m-%d %H:%M:%S"`







