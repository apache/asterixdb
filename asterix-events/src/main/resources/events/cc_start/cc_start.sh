if [ ! -d $LOG_DIR ]; 
then 
  mkdir -p $LOG_DIR
fi
cd $WORKING_DIR
#export JAVA_OPTS=$CC_JAVA_OPTS
$ASTERIX_HOME/bin/asterixcc -client-net-ip-address $CLIENT_NET_IP -client-net-port 1098 -cluster-net-ip-address $CLUSTER_NET_IP -cluster-net-port 1099 -http-port 8888  &> $LOG_DIR/cc.log
