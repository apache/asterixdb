if [ ! -d $LOG_DIR ]; 
then 
  mkdir -p $LOG_DIR
fi
cd $WORKING_DIR
$ASTERIX_HOME/bin/asterixcc -client-net-ip-address $CLIENT_NET_IP -client-net-port $CLIENT_NET_PORT -cluster-net-ip-address $CLUSTER_NET_IP -cluster-net-port $CLUSTER_NET_PORT -http-port $HTTP_PORT  &> $LOG_DIR/cc.log
