#!/bin/bash

LOGSDIR=/mnt/data/sda/space/yingyi/hyracks/logs
HYRACKS_HOME=/home/yingyib/hyracks-0.1.5

export JAVA_OPTS="-Djava.rmi.server.hostname=128.195.14.4 -Xdebug -Xrunjdwp:transport=dt_socket,address=7001,server=y,suspend=n"

$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyrackscc -port 3099  &> $LOGSDIR/cc-asterix.log&
