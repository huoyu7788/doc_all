#!bin/bash

temp_date="$1"
echo "wumart_sap.sh begin"

if [ ! -d "./log" ]; then
  mkdir ./log
fi

log_time=`date +"%Y%m%d%H%M%S"`

 java -jar wumart_sap_test.jar C092 RE ${temp_date} 1> ./log/wumart_sap_re_test_log_${log_time}
 java -jar wumart_sap_test.jar C112 RE ${temp_date} 1>> ./log/wumart_sap_re_test_log_${log_time}
 java -jar wumart_sap_test.jar C116 RE ${temp_date} 1>> ./log/wumart_sap_re_test_log_${log_time}
 java -jar wumart_sap_test.jar C117 RE ${temp_date} 1>> ./log/wumart_sap_re_test_log_${log_time}

 java -jar wumart_sap_test.jar C092 RE_F ${temp_date} 1> ./log/wumart_sap_ref_test_log_${log_time}
 java -jar wumart_sap_test.jar C112 RE_F ${temp_date} 1>> ./log/wumart_sap_ref_test_log_${log_time}

 java -jar wumart_sap_test.jar C092 RV ${temp_date} 1> ./log/wumart_sap_test_rv_log_${log_time}
 java -jar wumart_sap_test.jar C112 RV ${temp_date} 1>> ./log/wumart_sap_rv_test_log_${log_time}
 java -jar wumart_sap_test.jar C116 RV ${temp_date} 1>> ./log/wumart_sap_rv_test_log_${log_time}


 java -jar wumart_sap_test.jar C092 ZV1 ${temp_date} 1> ./log/wumart_sap_zv1_test_log_${log_time}
 java -jar wumart_sap_test.jar C112 ZV1 ${temp_date} 1>> ./log/wumart_sap_zv1_test_log_${log_time}
 java -jar wumart_sap_test.jar C116 ZV1 ${temp_date} 1>> ./log/wumart_sap_zv1_test_log_${log_time}

 java -jar wumart_sap_test.jar C092 ZV2 ${temp_date} 1> ./log/wumart_sap_test_zv2_log_${log_time}
 java -jar wumart_sap_test.jar C112 ZV2 ${temp_date} 1>> ./log/wumart_sap_test_zv2_log_${log_time}
 java -jar wumart_sap_test.jar C116 ZV2 ${temp_date} 1>> ./log/wumart_sap_test_zv2_log_${log_time}

#java -jar wumart_sap_test.jar C092 BR1 ${temp_date} 1> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C092 BR2 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C092 BR3 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C092 BR4 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}

#java -jar wumart_sap_test.jar C112 BR5 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C112 BR6 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C112 BR7 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C112 BR8 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}

#java -jar wumart_sap_test.jar C116 BR9 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C116 BR10 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C116 BR11 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C116 BR12 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}

#java -jar wumart_sap_test.jar C092 BR13 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C112 BR14 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C116 BR15 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C113 BR16 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}

#java -jar wumart_sap_test.jar C092 BR17 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C112 BR18 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
#java -jar wumart_sap_test.jar C116 BR19 ${temp_date} 1>> ./log/wumart_sap_br_test_log_${log_time}
echo "wumart_sap.sh end"
