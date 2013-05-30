#!/bin/bash

HYRACKS_HOME=/home/onose/src/hyracks

export JAVA_OPTS="-Djava.rmi.server.hostname=128.195.52.122 -DAsterixConfigFileName=test.properties -DAsterixWebServerPort=20001 -Djava.net.preferIPv4Stack=true"

$HYRACKS_HOME/hyracks-server/target/hyracks-server-0.1.3.1-binary-assembly/bin/hyrackscc -port 2222 &> logs/cc.log &
