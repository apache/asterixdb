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

#PID=`/usr/local/java/vms/java/bin/jps | grep NCDriver | awk '{print $1}'`

PID=`ps -ef|grep yingyib|grep java|grep hyracks|awk '{print $2}'`

echo $PID
kill -9 $PID

#PID=`ps -ef|grep yingyib|grep java|grep datanode|awk '{print $2}'`

#echo $PID
#kill -9 $PID


#PID=`ps -ef|grep yingyib|grep java|grep tasktracker|awk '{print $2}'`

#echo $PID
#kill -9 $PID

rm -rf /data/yingyi/tmp/*
rm -rf /data/yingyi/tmp/*
rm -rf /data/yingyi/tmp/*
rm -rf /data/yingyi/tmp/*
