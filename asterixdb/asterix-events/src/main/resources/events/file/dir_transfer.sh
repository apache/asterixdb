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

USERNAME=$1
DIR_TO_TRANSFER=$2
DEST_HOST=$3
DEST_DIR=$4
ssh -l $USERNAME $DEST_HOST "mkdir -p $DEST_DIR"
echo "scp -r $DIR_TO_TRANSFER $USERNAME@$DEST_HOST:$DEST_DIR/" 
scp -r $DIR_TO_TRANSFER $USERNAME@$DEST_HOST:$DEST_DIR/
