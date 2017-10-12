#!bin/bash

echo "u8.sh begin"

#时间处理
 if [ $1"0" == "0" ]
     then
   
     dateago=`date -d "${date} 0 days ago" +"%Y%m%d"`
   
     datapath=`date -d "${date} 0 days ago" +"%Y/%m/%d"`
   
     day=`date -d "${date} 0 days ago" +"%Y-%m-%d"`

     else

     dateago=`date -d "$1 0 days ago" +"%Y%m%d"`
   
     datapath=`date -d "$1 0 days ago" +"%Y/%m/%d"`

     day=`date -d "$1 0 days ago" +"%Y-%m-%d"`
 fi
 echo $dateago 
 echo $datapath
 echo $day

path=/home/work/wumart
voucherpath=/home/work/web/vss/webroot/voucher

if [ ! -d "${path}/log" ]; then
    mkdir ${path}/log
fi

if [ ! -d "${path}/data/${datapath}" ]; then
    mkdir -p ${path}/data/${datapath}
fi

#java -jar ${path}/u8.jar C092 RE $1 $2    
#java -jar ${path}/u8.jar C112 RE $1 $2    
#java -jar ${path}/u8.jar C116 RE $1 $2    
#java -jar ${path}/u8.jar C117 RE $1 $2    


#java -jar ${path}/u8.jar C092 JISHOU_RE $1 $2    
#java -jar ${path}/u8.jar C112 JISHOU_RE $1 $2    
#java -jar ${path}/u8.jar C116 JISHOU_RE $1 $2    
#java -jar ${path}/u8.jar C117 JISHOU_RE $1 $2    

#java -jar ${path}/u8.jar C092 BD_RE $1 $2    

#java -jar ${path}/u8.jar C092 RE_F $1 $2    
#java -jar ${path}/u8.jar C112 RE_F $1 $2    
#java -jar ${path}/u8.jar C116 RE_F $1 $2    

#java -jar ${path}/u8.jar C092 RV $1 $2    
#java -jar ${path}/u8.jar C112 RV $1 $2    
#java -jar ${path}/u8.jar C116 RV $1 $2    

#java -jar ${path}/u8.jar C092 BD_RV $1 $2    
 
#java -jar ${path}/u8.jar C092 BSDS_RV $1 $2    

#java -jar ${path}/u8.jar C092 ZV1 $1 $2    
#java -jar ${path}/u8.jar C112 ZV1 $1 $2    
#java -jar ${path}/u8.jar C116 ZV1 $1 $2    

#java -jar ${path}/u8.jar C092 ZV1_SH $1 $2    
#java -jar ${path}/u8.jar C112 ZV1_SH $1 $2    
#java -jar ${path}/u8.jar C116 ZV1_SH $1 $2    

#java -jar ${path}/u8.jar C092 BD_ZV1 $1 $2    

#java -jar ${path}/u8.jar C092 ZV2 $1 $2    
#java -jar ${path}/u8.jar C112 ZV2 $1 $2    
#java -jar ${path}/u8.jar C116 ZV2 $1 $2    

#java -jar ${path}/u8.jar C092 ZV2_CY $1 $2    
#java -jar ${path}/u8.jar C112 ZV2_CY $1 $2    
#java -jar ${path}/u8.jar C116 ZV2_CY $1 $2    

#java -jar ${path}/u8.jar C092 ZV2_LS $1 $2    
#java -jar ${path}/u8.jar C112 ZV2_LS $1 $2    
#java -jar ${path}/u8.jar C116 ZV2_LS $1 $2    

#java -jar ${path}/u8.jar C092 RV_F $1 $2    
#java -jar ${path}/u8.jar C112 RV_F $1 $2    
#java -jar ${path}/u8.jar C116 RV_F $1 $2    

#java -jar ${path}/u8.jar C092 KA_RV $1 $2    
#java -jar ${path}/u8.jar C092 KA_ZV2 $1 $2    

#java -jar ${path}/u8.jar C092 KZ $1 $2
#java -jar ${path}/u8.jar C112 KZ $1 $2
#java -jar ${path}/u8.jar C116 KZ $1 $2

#java -jar ${path}/u8.jar C092 BD_ZV2 $1 $2    

#java -jar ${path}/u8.jar C092 DR $1 $2    

#java -jar ${path}/u8.jar C117 WE $1 $2    

#java -jar ${path}/u8.jar C092 DM_RV $1 $2    

#java -jar ${path}/u8.jar C092 DM_ZV $1 $2    

 java -jar ${path}/u8.jar C092 BR $1 $2    
 java -jar ${path}/u8.jar C112 BR $1 $2    
 java -jar ${path}/u8.jar C113 BR $1 $2    
 java -jar ${path}/u8.jar C116 BR $1 $2    

#cd ${path}/data/${datapath}

# rm ${voucherpath}/fin_voucher_${dateago}.zip

# zip -r ${voucherpath}/fin_voucher_${dateago}.zip ./*

#if [ -f ${voucherpath}/fin_voucher_${dateago}.zip ]; then

#   echo "fin_voucher_${dateago}.zip create success"

#else

#   echo "fin_voucher_${dateago}.zip create error"
  # SendMail -t zhaoning -o "create fin_voucher_date_${dateago}.tar err " -u  "fin_voucher_date_${day}.tar"

#fi

echo "u8.sh end"


