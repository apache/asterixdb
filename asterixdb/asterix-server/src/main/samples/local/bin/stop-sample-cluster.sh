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
$INSTALLDIR/bin/${HELPER_COMMAND} get_cluster_state -quiet
if [ $? -ne 1 ]; then
  $INSTALLDIR/bin/${HELPER_COMMAND} shutdown_cluster_all
else
  echo "WARNING: sample cluster does not appear to be running, will attempt to wait for"
  echo "         CCDriver to terminate if running."
fi

first=1
while [ -n "$($JAVA_HOME/bin/jps | awk '/CCDriver/')" ]; do
  if [ $first ]; then
    echo
    echo -n "Waiting for CCDriver to terminate."
    unset first
  fi
  sleep 2s
  echo -n .
done
[ ! $first ] && echo ".done." || true
