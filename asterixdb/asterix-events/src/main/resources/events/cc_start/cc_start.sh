#!/usr/bin/env bash
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

if [ ! -d $LOG_DIR ];
then 
  mkdir -p $LOG_DIR
fi
ccArgs='-client-net-ip-address '$CLIENT_NET_IP
ccArgs=$ccArgs' -client-net-port '$CLIENT_NET_PORT
ccArgs=$ccArgs' -cluster-net-ip-address '$CLUSTER_NET_IP
ccArgs=$ccArgs' -cluster-net-port '$CLUSTER_NET_PORT
ccArgs=$ccArgs' -http-port '$HTTP_PORT

if [ ! -z $HEARTBEAT_PERIOD ]
then
ccArgs=$ccArgs' -heartbeat-period '$HEARTBEAT_PERIOD
fi

if [ ! -z $MAX_HEARTBEAT_LAPSE_PERIODS ]
then
ccArgs=$ccArgs' -max-heartbeat-lapse-periods '$MAX_HEARTBEAT_LAPSE_PERIODS
fi

if [ ! -z $PROFILE_DUMP_PERIOD ]
then
ccArgs=$ccArgs' -profile-dump-period '$PROFILE_DUMP_PERIOD
fi

if [ ! -z $DEFAULT_MAX_JOB_ATTEMPTS ]
then
ccArgs=$ccArgs' -default-max-job-attempts '$DEFAULT_MAX_JOB_ATTEMPTS
fi

if [ ! -z $JOB_HISTORY_SIZE ]
then
ccArgs=$ccArgs' -job-history-size '$JOB_HISTORY_SIZE
fi

if [ ! -z $RESULT_TIME_TO_LIVE ]
then
ccArgs=$ccArgs' "-result-time-to-live '$RESULT_TIME_TO_LIVE
fi

if [ ! -z $RESULT_SWEEP_THRESHOLD ]
then
ccArgs=$ccArgs' -result-sweep-threshold '$RESULT_SWEEP_THRESHOLD
fi

if [ ! -z $CC_ROOT ]
then
ccArgs=$ccArgs' -cc-root '$CC_ROOT
fi
cd $WORKING_DIR
DATE=`date`

cat <<EOF >> $LOG_DIR/cc.log
--------------------------------------------------------------------------------
LOG START: $DATE
--------------------------------------------------------------------------------
EOF
$ASTERIX_HOME/bin/asterixcc echo $ccArgs >> $LOG_DIR/cc.log 2>&1