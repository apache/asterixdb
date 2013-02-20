NC_ID=$1

#if [ $NC_ID == 'ANY' ]
#then
#  NC_ID="." 
#fi 
#
#USER=`who am i | tr -s " " | cut -d " " -f1`
#PARENT_ID=`ps -ef  | tr -s " " | grep nc_join | grep -v grep | grep -v ssh | grep $NC_ID | cut -d " " -f2 | head -n 1` 
#PID=`ps -ef | tr -s " " | grep hyracks | grep -v grep | grep -v nc_join |  grep $PARENT_ID | cut -d " " -f2 | head -n 1`
#kill -9 $PID
#


INFO=`ps -ef | grep nc_join | grep -v grep | grep -v ssh| grep $NC_ID | head -n 1`
PARENT_ID=`echo  $INFO | cut -d " "  -f2`
PID_INFO=`ps -ef |  grep hyracks | grep -v grep | grep -v nc_join |  grep $PARENT_ID`
PID=`echo $PID_INFO | cut -d " " -f2`
kill -9 $PID
