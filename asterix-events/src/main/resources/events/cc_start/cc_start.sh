if [ ! -d $LOG_DIR ]; 
then 
  mkdir -p $LOG_DIR
fi
if [ ! -z $1 ];
then
  JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,address=$1,server=y,suspend=n"
fi  
$HYRACKS_HOME/bin/hyrackscc -client-net-ip-address $CLIENT_NET_IP -client-net-port 1098 -cluster-net-ip-address $CLUSTER_NET_IP -cluster-net-port 1099 -http-port 8888  &> $LOG_DIR/cc.log
