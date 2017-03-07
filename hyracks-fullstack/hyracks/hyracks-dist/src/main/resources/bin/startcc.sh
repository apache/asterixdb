#!/bin/bash
#/*
 # Licensed to the Apache Software Foundation (ASF) under one
 # or more contributor license agreements.  See the NOTICE file
 # distributed with this work for additional information
 # regarding copyright ownership.  The ASF licenses this file
 # to you under the Apache License, Version 2.0 (the
 # "License"); you may not use this file except in compliance
 # with the License.  You may obtain a copy of the License at
 #
 #   http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing,
 # software distributed under the License is distributed on an
 # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 # KIND, either express or implied.  See the License for the
 # specific language governing permissions and limitations
 # under the License.
 #*/
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
export JAVA_OPTS=$CCJAVA_OPTS

#Launch hyracks cc script
chmod -R 755 $HYRACKS_HOME
if [ -f "conf/topology.xml"  ]; then
#Launch hyracks cc script with topology
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyrackscc -client-listen-address $CCHOST -address $CCHOST -client-listen-port $CC_CLIENTPORT -cluster-listen-port $CC_CLUSTERPORT -heartbeat-max-misses 999999 -job-history-size 0 -cluster-topology "conf/topology.xml" &> $CCLOGS_DIR/cc.log &
else
#Launch hyracks cc script without toplogy
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyrackscc -client-listen-address $CCHOST -address $CCHOST -client-listen-port $CC_CLIENTPORT -cluster-listen-port $CC_CLUSTERPORT -heartbeat-max-misses 999999 -job-history-size 0 &> $CCLOGS_DIR/cc.log &
fi
