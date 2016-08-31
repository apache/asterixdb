#!/bin/bash
# ------------------------------------------------------------
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
# ------------------------------------------------------------

if [ -z "$JAVA_HOME" -a -x /usr/libexec/java_home ]; then
  JAVA_HOME=$(/usr/libexec/java_home)
  export JAVA_HOME
fi

[ -z "$JAVA_HOME" ] && {
  echo "JAVA_HOME not set"
  exit 1
}
"$JAVA_HOME/bin/java" -version 2>&1 | grep -q '1\.[89]' || {
  echo "JAVA_HOME must be at version 1.8 or later:"
  "$JAVA_HOME/bin/java" -version
  exit 2
}
DIRNAME=$(dirname $0)
[ $(echo $DIRNAME | wc -l) -ne 1 ] && {
  echo "Paths with spaces are not supported"
  exit 3
}

CLUSTERDIR=$(cd $DIRNAME/..; echo $PWD)
INSTALLDIR=$(cd $CLUSTERDIR/../..; echo $PWD)
LOGSDIR=$CLUSTERDIR/logs

echo "CLUSTERDIR=$CLUSTERDIR"
echo "INSTALLDIR=$INSTALLDIR"
echo
cd $CLUSTERDIR
mkdir -p $LOGSDIR
$INSTALLDIR/bin/${HELPER_COMMAND} get_cluster_state -quiet \
    && echo "ERROR: sample cluster address (localhost:${LISTEN_PORT}) already in use" && exit 1

(
  echo "--------------------------"
  date
  echo "--------------------------"
) | tee -a $LOGSDIR/blue-service.log | tee -a $LOGSDIR/red-service.log >> $LOGSDIR/cc.log
echo "Starting sample cluster..."
$INSTALLDIR/bin/${NC_SERVICE_COMMAND} -logdir - -config-file $CLUSTERDIR/conf/blue.conf >> $LOGSDIR/blue-service.log 2>&1 &
$INSTALLDIR/bin/${NC_SERVICE_COMMAND} -logdir - >> $LOGSDIR/red-service.log 2>&1 &
$INSTALLDIR/bin/${CC_COMMAND} -config-file $CLUSTERDIR/conf/cc.conf >> $LOGSDIR/cc.log 2>&1 &
echo "Waiting for sample cluster (localhost:${LISTEN_PORT}) to be ready..."
if $INSTALLDIR/bin/${HELPER_COMMAND} wait_for_cluster -quiet -timeout 30;
then
  echo "Sample cluster (localhost:${LISTEN_PORT}) is ready..."
else
  echo "ERROR: cluster did not start successfully"
fi
echo "See output in $LOGSDIR/"
