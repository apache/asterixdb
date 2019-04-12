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
  echo Usage: $(basename $0)
  echo
}

while [ -n "$1" ]; do
  case $1 in
    -f|-force);; # ignored, this is always the case
    -help|--help|-usage|--usage) usage; exit 0;;
    *) echo "ERROR: unknown argument '$1'"; usage; exit 1;;
  esac
  shift
done

if [ -z "$JAVA_HOME" -a -x /usr/libexec/java_home ]; then
  JAVA_HOME=$(/usr/libexec/java_home)
  export JAVA_HOME
fi

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  Darwin*) darwin=true
           if [ -z "$JAVA_VERSION" ] ; then
             JAVA_VERSION="CurrentJDK"
           else
             echo "Using Java version: $JAVA_VERSION"
           fi
           if [ -z "$JAVA_HOME" ]; then
              if [ -x "/usr/libexec/java_home" ]; then
                  JAVA_HOME=`/usr/libexec/java_home`
              else
                  JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/${JAVA_VERSION}/Home
              fi
           fi
           ;;
esac

if [ -z "$JAVA_HOME" ] ; then
  if [ -r /etc/gentoo-release ] ; then
    JAVA_HOME=`java-config --jre-home`
  fi
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
  [ -n "$CLASSPATH" ] && CLASSPATH=`cygpath --path --unix "$CLASSPATH"`
fi

# If a specific java binary isn't specified search for the standard 'java' binary
if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD=`which java`
  fi
fi

export JAVA_VERSION=$($JAVACMD -version 2>&1 | head -1 | awk '{ print $3 }' | tr -d '"')
case $JAVA_VERSION in
  1.8*|1.9*|10*|11*)
    ;;
  *)
  echo JAVA_HOME must be at version 1.8 or later, but is: $JAVA_VERSION
  exit 2
esac

DIRNAME=$(dirname "$0")
[ $(echo $DIRNAME | wc -l) -ne 1 ] && {
  echo "Paths with spaces are not supported"
  exit 3
}

CLUSTERDIR=$(cd "$DIRNAME/.."; echo $PWD)
INSTALLDIR=$(cd "$CLUSTERDIR/../.."; echo $PWD)
LOGSDIR=$CLUSTERDIR/logs

echo "CLUSTERDIR=$CLUSTERDIR"
echo "INSTALLDIR=$INSTALLDIR"
echo "LOGSDIR=$LOGSDIR"
echo
cd "$CLUSTERDIR"
mkdir -p "$LOGSDIR"
"$INSTALLDIR/bin/${HELPER_COMMAND}" get_cluster_state -quiet \
    && echo "ERROR: sample cluster address (localhost:${LISTEN_PORT}) already in use" && exit 1

if ps -ef | grep 'java.*org\.apache\.hyracks\.control\.[cn]c\.\([CN]CDriver\|service\.NCService\)' > /tmp/$$_pids; then
  echo "WARNING: ${PRODUCT} processes are already running:"
  cat /tmp/$$_pids |  sed 's/^ *[0-9]* \([0-9]*\).*org\.apache\.hyracks\.control\.[cn]c[^ ]*\.\([^ ]*\) .*/\1 - \2/'
  rm /tmp/$$_pids
fi

rm /tmp/$$_pids
(
  echo "--------------------------"
  date
  echo "--------------------------"
) | tee -a "$LOGSDIR/blue-service.log" | tee -a "$LOGSDIR/red-service.log" >> "$LOGSDIR/cc.log"
echo "INFO: Starting sample cluster..."
"$INSTALLDIR/bin/${NC_SERVICE_COMMAND}" -logdir - -config-file "$CLUSTERDIR/conf/blue.conf" >> "$LOGSDIR/blue-service.log" 2>&1 &
"$INSTALLDIR/bin/${NC_SERVICE_COMMAND}" -logdir - >> "$LOGSDIR/red-service.log" 2>&1 &
"$INSTALLDIR/bin/${CC_COMMAND}" -config-file "$CLUSTERDIR/conf/cc.conf" >> "$LOGSDIR/cc.log" 2>&1 &
"$INSTALLDIR/bin/${HELPER_COMMAND}" wait_for_cluster -timeout 30
exit $?
