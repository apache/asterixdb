#!/bin/bash
# ----------------------------------------------------------------------------
#  Copyright 2001-2006 The Apache Software Foundation.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# ----------------------------------------------------------------------------
#
#   Copyright (c) 2001-2006 The Apache Software Foundation.  All rights
#   reserved.

if [ -z "$JAVA_HOME" -a -x /usr/libexec/java_home ]; then
  JAVA_HOME=$(/usr/libexec/java_home)
  export JAVA_HOME
fi

[ -z "$JAVA_HOME" ] && {
  echo "JAVA_HOME not set"
  exit 1
}
"$JAVA_HOME/bin/java" -version || {
  echo "JAVA_HOME not valid"
  exit 2
}

DIRNAME=$(dirname $0)
[ $(echo $DIRNAME | wc -l) -ne 1 ] && {
  echo "Paths with spaces are not supported"
  exit 3
}

CLUSTERDIR=$(cd $DIRNAME/..; echo $PWD)
CBASDIR=$(cd $CLUSTERDIR/../..; echo $PWD)
LOGSDIR=$CLUSTERDIR/logs

echo "CLUSTERDIR=$CLUSTERDIR"
echo "CBASDIR=$CBASDIR"

cd $CLUSTERDIR
mkdir -p $LOGSDIR

(
  echo "--------------------------"
  date
  echo "--------------------------"
) | tee -a $LOGSDIR/blue-service.log | tee -a $LOGSDIR/red-service.log >> $LOGSDIR/cc.log

$CBASDIR/bin/asterixncservice -logdir - -config-file $CLUSTERDIR/conf/blue.conf >> $LOGSDIR/blue-service.log 2>&1 &
$CBASDIR/bin/asterixncservice -logdir - >> $LOGSDIR/red-service.log 2>&1 &
$CBASDIR/bin/asterixcc -config-file $CLUSTERDIR/conf/cc.conf >> $LOGSDIR/cc.log 2>&1 &

echo -n "Waiting for cluster to start."
while [ 1 ]; do
  curl -s -o /dev/null http://localhost:19002 && break
  echo -n "."
  sleep 1s
done
echo ".done"
echo
echo "See output in $LOGSDIR/"
