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
#/bin/bash

#export JAVA_OPTS="-Xmx1024m -DAsterixConfigFileName=asterix-idefix.properties"
#export JAVA_OPTS="-agentlib:hprof=cpu=samples,file=/tmp/q9.dump -Xmx1024m -DAsterixConfigFileName=asterix-idefix.properties"
export JAVA_OPTS="-DAsterixConfigFileName=test.properties -Djava.util.logging.config.file=/home/nicnic/Work/Asterix/hyracks/logging.properties"
export HYRACKS_HOME="/home/nicnic/workspace/hyracks/tags/hyracks-0.1.5"

bash ${HYRACKS_HOME}/hyracks-server/target/appassembler/bin/hyracksnc -cc-host 127.0.0.1 -data-ip-address 127.0.0.1 -node-id "nc1" $* 
