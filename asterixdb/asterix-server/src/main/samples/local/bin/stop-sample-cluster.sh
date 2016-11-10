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

function usage() {
  echo
  echo Usage: $(basename $0) [-f[orce]]
  echo
  echo "  -f[orce]  : Forcibly terminates any running ${PRODUCT} processes (after shutting down cluster, if running)"
}

while [ -n "$1" ]; do
  case $1 in
    -f|-force) force=1;;
    -help|--help|-usage|--usage) usage; exit 0;;
    *) echo "ERROR: unknown argument '$1'"; usage; exit 1;;
    esac
  shift
done

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
  first=1
  tries=0
  echo -n "INFO: Waiting up to 60s for cluster to shutdown"
  while [ -n "$($JAVA_HOME/bin/jps | awk '/ (CCDriver|NCDriver|NCService)$/')" ]; do
    if [ $tries -ge 60 ]; then
      echo "...timed out!"
      break
    fi
    sleep 1s
    echo -n .
    tries=$(expr $tries + 1)
  done
  echo ".done." || true
else
  echo "WARNING: sample cluster does not appear to be running"
fi

if $JAVA_HOME/bin/jps | grep ' \(CCDriver\|NCDriver\|NCService\)$' > /tmp/$$_jps; then
  echo -n "WARNING: ${PRODUCT} processes remain after cluster shutdown; "
  if [ $force ]; then
    echo "-f[orce] specified, forcibly terminating ${PRODUCT} processes:"
    cat /tmp/$$_jps | while read line; do
      echo -n "   - $line..."
      echo $line | awk '{ print $1 }' | xargs -n1 kill -9
      echo "killed"
    done
  else
    echo "re-run with -f|-force to forcibly terminate all ${PRODUCT} processes:"
    cat /tmp/$$_jps | sed 's/^/  - /'
  fi
fi
rm /tmp/$$_jps
