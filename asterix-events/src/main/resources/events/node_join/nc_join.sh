CC_HOST=$1
NC_ID=$2
if [ ! -d $LOG_DIR ]; 
then 
  mkdir -p $LOG_DIR
fi
if [ ! -z $3 ];
then
  JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,address=$3,server=y,suspend=n"
fi  
$HYRACKS_HOME/bin/hyracksnc -node-id $NC_ID -cc-host $CC_HOST -cc-port 1099 -cluster-net-ip-address $IP_LOCATION  -data-ip-address $IP_LOCATION &> $LOG_DIR/${NC_ID}.log
