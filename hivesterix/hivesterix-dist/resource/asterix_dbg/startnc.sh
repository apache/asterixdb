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

LOGSDIR=/mnt/data/sda/space/yingyi/hyracks/logs
HYRACKS_HOME=/home/yingyib/hyracks-0.1.5

IPADDR=`/sbin/ifconfig eth0 | grep "inet addr" | awk '{print $2}' | cut -f 2 -d ':'`
NODEID=`ypcat hosts | grep asterix | grep "$IPADDR " | awk '{print $2}'`

export JAVA_OPTS="-Xmx10G -agentpath:/home/yingyib/yjp-9.5.6/bin/linux-x86-64/libyjpagent.so=listen=28001"

echo $HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host 128.195.14.4 -cc-port 3099 -data-ip-address $IPADDR -node-id $NODEID
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host 128.195.14.4 -cc-port 3099  -data-ip-address $IPADDR -node-id $NODEID -iodevices "/mnt/data/sda/space/yingyi/tmp/,/mnt/data/sdb/space/yingyi/tmp/,/mnt/data/sdc/space/yingyi/tmp/,/mnt/data/sdd/space/yingyi/tmp/" &> $LOGSDIR/$NODEID.log &
