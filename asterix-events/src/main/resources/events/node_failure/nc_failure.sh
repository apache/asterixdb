NC_ID=$1

INFO=`ps -ef | grep nc_join | grep -v grep | grep -v ssh| grep $NC_ID | head -n 1`
PARENT_ID=`echo  $INFO | cut -d " "  -f2`
PID_INFO=`ps -ef |  grep asterix | grep -v grep | grep -v nc_join |  grep $PARENT_ID`
PID=`echo $PID_INFO | cut -d " " -f2`
kill -15 $PID
