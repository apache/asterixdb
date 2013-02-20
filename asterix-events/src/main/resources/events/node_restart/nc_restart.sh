CC_HOST=$1
NC_ID=$2
SLEEP_TIME=$3

if [ $NC_ID == 'ANY' ]
then
  NC_ID="." 
  PARENT_ID=`ps -ej | tr -s " " | grep nc_join | grep -v grep | grep -v ssh |  cut -d " " -f2 | head -n 1` 
  PARENT_PROCESS_ENTRY=`ps -ef | grep $PARENT_ID | grep -v grep   | head -n 1`
  NC_ID=`echo ${PARENT_PROCESS_ENTRY##* }`
  echo "NCid is $NC_ID" >> ~/try.txt
else 
  PARENT_ID=`ps -ej | tr -s " " | grep nc_join | grep -v grep | grep -v ssh | grep $NC_ID | cut -d " " -f2 | head -n 1` 
fi 

PID=`ps -ej | tr -s " " | grep hyracks | grep -v grep | grep -v nc_join |  grep $PARENT_ID | cut -d " " -f2 | head -n 1`
kill -9 $PID

sleep $3

$HYRACKS_HOME/hyracks-server/target/hyracks-server-0.2.2-SNAPSHOT-binary-assembly/bin/hyracksnc -node-id $NC_ID -cc-host $CC_HOST -cc-port 1099 -cluster-net-ip-address $IP_LOCATION  -data-ip-address $IP_LOCATION
