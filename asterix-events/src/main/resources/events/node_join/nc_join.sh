CC_HOST=$1
NC_ID=$2
IO_DEVICES=$3
if [ ! -d $LOG_DIR ]; 
then 
  mkdir -p $LOG_DIR
fi
$ASTERIX_HOME/bin/asterixnc -node-id $NC_ID -cc-host $CC_HOST -cc-port 1099 -cluster-net-ip-address $IP_LOCATION  -data-ip-address $IP_LOCATION -iodevices $IO_DEVICES -result-ip-address $IP_LOCATION &> $LOG_DIR/${NC_ID}.log
