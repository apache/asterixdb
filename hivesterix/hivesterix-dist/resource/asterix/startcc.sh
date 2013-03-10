#!/bin/bash

LOGSDIR=/mnt/data/sda/space/yingyi/hyracks/logs
HYRACKS_HOME=/home/yingyib/hyracks_asterix_stabilization

export JAVA_OPTS="-Xmx2g  -Djava.rmi.server.hostname=128.195.14.4"

cd $LOGSDIR
echo $HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyrackscc -client-net-ip-address 128.195.14.4 -cluster-net-ip-address 10.1.0.1 -client-net-port 3099 -cluster-net-port 1099 -max-heartbeat-lapse-periods 999999 &> $LOGSDIR/cc-asterix.log&
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyrackscc -client-net-ip-address 128.195.14.4 -cluster-net-ip-address 10.1.0.1 -client-net-port 3099 -cluster-net-port 1099 -max-heartbeat-lapse-periods 999999 &> $LOGSDIR/cc-asterix.log&
