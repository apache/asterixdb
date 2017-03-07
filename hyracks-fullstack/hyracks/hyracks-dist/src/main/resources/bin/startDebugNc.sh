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

#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CURRENT_PATH=`pwd`
CCHOST=`ssh ${CCHOST_NAME} "cd ${CURRENT_PATH}; bin/getip.sh"`

#Import cluster properties
. conf/cluster.properties
. conf/debugnc.properties

#Clean up temp dir

rm -rf $NCTMP_DIR2
mkdir $NCTMP_DIR2

#Clean up log dir
rm -rf $NCLOGS_DIR2
mkdir $NCLOGS_DIR2


#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS2 | tr "," "\n")
for io_dir in $io_dirs
do
    rm -rf $io_dir
    mkdir $io_dir
done

#Set JAVA_HOME
export JAVA_HOME=$JAVA_HOME

#Get OS
IPADDR=`bin/getip.sh`

#Get node ID
NODEID=`hostname | cut -d '.' -f 1`
NODEID=${NODEID}2

#Set JAVA_OPTS
export JAVA_OPTS=$NCJAVA_OPTS2

cd $HYRACKS_HOME
HYRACKS_HOME=`pwd`

#Enter the temp dir
cd $NCTMP_DIR2

#Launch hyracks nc
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cluster-address $CCHOST -cluster-port $CC_CLUSTERPORT -address $IPADDR  -data-listen-address $IPADDR -node-id $NODEID -iodevices "${IO_DIRS2}" &> $NCLOGS_DIR2/$NODEID.log &
