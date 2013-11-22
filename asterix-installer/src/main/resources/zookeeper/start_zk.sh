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
ZK_HOME=$1
ZK_ID=$2
JAVA_HOME=$3
mkdir $ZK_HOME/data
echo $2 > $ZK_HOME/data/myid
CLASSPATH=$ZK_HOME/lib/zookeeper-3.4.5.jar:$ZK_HOME/lib/log4j-1.2.15.jar:$ZK_HOME/lib/slf4j-api-1.6.1.jar:$ZK_HOME/conf:$ZK_HOME/conf/log4j.properties
ZK_CONF=$ZK_HOME/zk.cfg
export JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8400,server=y,suspend=n"
$JAVA_HOME/bin/java $JAVA_OPTS -Dlog4j.configuration="file:$ZK_HOME/conf/log4j.properties" -cp $CLASSPATH org.apache.zookeeper.server.quorum.QuorumPeerMain $ZK_CONF
