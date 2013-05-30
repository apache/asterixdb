#!/bin/bash

export JAVA_HOME=/usr/local/java/vms/java

LOGSDIR=/home/onose/hyracks-rainbow/logs
HYRACKS_HOME=/home/onose/src/hyracks

IPADDR=`/sbin/ifconfig eth0 | grep "inet addr" | awk '{print $2}' | cut -f 2 -d ':'`
NODEID=`ypcat hosts | grep rainbow | grep -w $IPADDR | awk '{print $2}'`

export JAVA_OPTS="-DNodeControllerDataPath=/tmp/ncX/"

echo $HYRACKS_HOME/hyracks-server/target/hyracks-server-0.1.3.1-binary-assembly/bin/hyracksnc -cc-host 128.195.52.177 -cc-port 2222 -data-ip-address $IPADDR -node-id $NODEID
$HYRACKS_HOME/hyracks-server/target/hyracks-server-0.1.3.1-binary-assembly/bin/hyracksnc -cc-host 128.195.52.177 -cc-port 2222 -data-ip-address $IPADDR -node-id $NODEID &> $LOGSDIR/$NODEID.log &
