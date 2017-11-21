#!/bin/sh

beg_date=`date -d "$1" +%s`
end_date=`date -d "$2" +%s`

if [[ -z $1 ]]||[[ -z $2 ]];then
        echo "Usage: $0 YYYYMMDD YYYYMMDD"
        exit 0;
fi
if [[ ${beg_date} > ${end_date} ]];then
        echo "The end_date < beg_date ;Please input the right date,example: $0 20140101 20140301"
        exit 0;
fi
for (( i=${beg_date};i<=${end_date};i=i+86400))
do
        date -d @${i} +%Y-%m-%d
        date -d @${i} +%Y%m%d 


        
done
