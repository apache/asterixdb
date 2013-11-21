#!/bin/bash
#/*
# Copyright 2009-2013 by The Regents of the University of California
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# you may obtain a copy of the License from
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*/

export JAVA_HOME=/usr/local/java/vms/java

LOGSDIR=/home/onose/hyracks-asterix/logs
HYRACKS_HOME=/home/onose/src/hyracks

IPADDR=`/sbin/ifconfig eth0 | grep "inet addr" | awk '{print $2}' | cut -f 2 -d ':'`
NODEID=`ypcat hosts | grep asterix | grep -w $IPADDR | awk '{print $2}'`

export JAVA_OPTS="-Xmx10g -Djava.net.preferIPv4Stack=true -Djava.io.tmpdir=/mnt/data/sdd/space/onose/tmp"

echo $HYRACKS_HOME/hyracks-server/target/hyracks-server-0.1.3.1-binary-assembly/bin/hyracksnc -cc-host 10.1.0.1 -cc-port 2222 -data-ip-address $IPADDR -node-id $NODEID
$HYRACKS_HOME/hyracks-server/target/hyracks-server-0.1.3.1-binary-assembly/bin/hyracksnc -cc-host 10.1.0.1 -cc-port 2222 -data-ip-address $IPADDR -node-id $NODEID &> $LOGSDIR/$NODEID.log &
