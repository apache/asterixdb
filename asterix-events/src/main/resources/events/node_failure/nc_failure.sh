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
NC_ID=$1

INFO=`ps -ef | grep nc_join | grep -v grep | grep -v ssh| grep $NC_ID | head -n 1`
PARENT_ID=`echo  $INFO | cut -d " "  -f2`
PID_INFO=`ps -ef |  grep asterix | grep -v grep | grep -v nc_join |  grep $PARENT_ID`
PID=`echo $PID_INFO | cut -d " " -f2`
kill -15 $PID

cmd_output=$(jps|grep $PID)
while [ ${#cmd_output} -ne 0 ]
do
  sleep 1
  kill -15 $PID
  cmd_output=$(jps|grep $PID)
done
