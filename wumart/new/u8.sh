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

if [ ! -d "./log" ]; then
    mkdir ./log
fi

if [ ! -d "./data/${datapath}" ]; then
    mkdir -p ./data/${datapath}
fi

#java -jar U8.jar C092 RE ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C112 RE ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C116 RE ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C117 RE ${day} $2 1>> ./log/u8_log_${dateago}


#java -jar U8.jar C092 JISHOU_RE ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C112 JISHOU_RE ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C116 JISHOU_RE ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C117 JISHOU_RE ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 BD_RE ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 RE_F ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C112 RE_F ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C116 RE_F ${day} $2 1>> ./log/u8_log_${dateago}

 java -jar U8.jar C092 RV ${day} $2 1>> ./log/u8_log_${dateago}
 java -jar U8.jar C112 RV ${day} $2 1>> ./log/u8_log_${dateago}
 java -jar U8.jar C116 RV ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 BD_RV ${day} $2 1>> ./log/u8_log_${dateago}
 
#java -jar U8.jar C092 BSDS_RV ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 ZV1 ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C112 ZV1 ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C116 ZV1 ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 ZV1_SH ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C112 ZV1_SH ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C116 ZV1_SH ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 BD_ZV1 ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 ZV2 ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C112 ZV2 ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C116 ZV2 ${day} $2 1>> ./log/u8_log_${dateago}

 java -jar U8.jar C092 ZV2_CY ${day} $2 1>> ./log/u8_log_${dateago}
 java -jar U8.jar C112 ZV2_CY ${day} $2 1>> ./log/u8_log_${dateago}
 java -jar U8.jar C116 ZV2_CY ${day} $2 1>> ./log/u8_log_${dateago}

 java -jar U8.jar C092 ZV2_LS ${day} $2 1>> ./log/u8_log_${dateago}
 java -jar U8.jar C112 ZV2_LS ${day} $2 1>> ./log/u8_log_${dateago}
 java -jar U8.jar C116 ZV2_LS ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 BD_ZV2 ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 DR ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C117 WE ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 DM_RV ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 DM_ZV ${day} $2 1>> ./log/u8_log_${dateago}

#java -jar U8.jar C092 BR ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C112 BR ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C113 BR ${day} $2 1>> ./log/u8_log_${dateago}
#java -jar U8.jar C116 BR ${day} $2 1>> ./log/u8_log_${dateago}

echo "u8.sh end"
