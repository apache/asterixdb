#!/bin/bash
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

HYRACKS_HOME=/home/onose/src/hyracks

export JAVA_OPTS="-Djava.rmi.server.hostname=128.195.52.177 -DAsterixConfigFileName=asterix-rainbow.properties -DAsterixWebServerPort=20001"

$HYRACKS_HOME/hyracks-server/target/hyracks-server-0.1.3.1-binary-assembly/bin/hyrackscc -port 2222 &> logs/cc.log &
