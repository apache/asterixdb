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

CC_HOST=$1
NC_ID=$2
IO_DEVICES=$3
INITIAL_RUN_FLAG=$4
if [ ! -d $LOG_DIR ]; 
then 
  mkdir -p $LOG_DIR
fi

cd $WORKING_DIR

$ASTERIX_HOME/bin/asterixnc -node-id $NC_ID -cc-host $CC_HOST -cc-port $CLUSTER_NET_PORT  -cluster-net-ip-address $IP_LOCATION  -data-ip-address $IP_LOCATION -iodevices $IO_DEVICES -result-ip-address $IP_LOCATION -- $INITIAL_RUN_FLAG &> $LOG_DIR/${NC_ID}.log
