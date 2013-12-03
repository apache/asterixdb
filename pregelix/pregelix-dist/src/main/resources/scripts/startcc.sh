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
#
#------------------------------------------------------------------------
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
# ------------------------------------------------------------------------
#

hostname

#Import cluster properties
. conf/cluster.properties

#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CCHOST=`bin/getip.sh`

#Remove the temp dir
rm -rf $CCTMP_DIR
mkdir $CCTMP_DIR

#Remove the logs dir
rm -rf $CCLOGS_DIR
mkdir $CCLOGS_DIR

#Export JAVA_HOME and JAVA_OPTS
export JAVA_HOME=$JAVA_HOME

#get the OS
OS_NAME=`uname -a|awk '{print $1}'`
LINUX_OS='Linux'

if [ $OS_NAME = $LINUX_OS ];
then
        MEM_SIZE=`cat /proc/meminfo |grep MemTotal|awk '{print $2}'`
	MEM_SIZE=$(($MEM_SIZE * 1000))
else
        MEM_SIZE=`sysctl -a | grep "hw.memsize ="|awk '{print $3}'`
fi

MEM_SIZE=$(($MEM_SIZE * 3 / 4))

#Set JAVA_OPTS
export JAVA_OPTS=$CCJAVA_OPTS" -Xmx"$MEM_SIZE

PREGELIX_HOME=`pwd`

#Enter the temp dir
cd $CCTMP_DIR

cmd=( "${PREGELIX_HOME}/bin/pregelixcc" )
cmd+=( -client-net-ip-address $CCHOST -cluster-net-ip-address $CCHOST
       -heartbeat-period 5000 -max-heartbeat-lapse-periods 4 
       -default-max-job-attempts 0 )

if [ -n "$CC_CLIENTPORT" ]; then
    cmd+=( -client-net-port $CC_CLIENTPORT )
fi
if [ -n "$CC_CLUSTERPORT" ]; then
    cmd+=( -cluster-net-port $CC_CLUSTERPORT )
fi
if [ -n "$CC_HTTPPORT" ]; then
    cmd+=( -http-port $CC_HTTPPORT )
fi
if [ -n "$JOB_HISTORY_SIZE" ]; then
    cmd+=( -job-history-size $JOB_HISTORY_SIZE )
fi
if [ -f "${PREGELIX_HOME}/conf/topology.xml"  ]; then
    cmd+=( -cluster-topology "${PREGELIX_HOME}/conf/topology.xml" )
fi

printf "\n\n\n********************************************\nStarting CC with command %s\n\n" "${cmd[*]}" >> "$CCLOGS_DIR/cc.log"
#Start the pregelix CC
${cmd[@]} &>> "$CCLOGS_DIR/cc.log" &
