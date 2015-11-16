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
cd $WORKING_DIR
$ASTERIX_HOME/bin/asterixcc -client-net-ip-address $CLIENT_NET_IP -client-net-port $CLIENT_NET_PORT -cluster-net-ip-address $CLUSTER_NET_IP -cluster-net-port $CLUSTER_NET_PORT -http-port $HTTP_PORT &> $LOG_DIR/cc.log
