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

LOGSDIR=/mnt/data/sda/space/yingyi/hyracks/logs
HYRACKS_HOME=/home/yingyib/hyracks_asterix_stabilization

export JAVA_OPTS="-Xmx2g  -Djava.rmi.server.hostname=128.195.14.4"

cd $LOGSDIR
echo $HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyrackscc -client-net-ip-address 128.195.14.4 -cluster-net-ip-address 10.1.0.1 -client-net-port 3099 -cluster-net-port 1099 -max-heartbeat-lapse-periods 999999 &> $LOGSDIR/cc-asterix.log&
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyrackscc -client-net-ip-address 128.195.14.4 -cluster-net-ip-address 10.1.0.1 -client-net-port 3099 -cluster-net-port 1099 -max-heartbeat-lapse-periods 999999 &> $LOGSDIR/cc-asterix.log&
