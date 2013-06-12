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
CC_HOST=$1
NC_ID=$2
SLEEP_TIME=$3

if [ $NC_ID == 'ANY' ]
then
  NC_ID="." 
  PARENT_ID=`ps -ej | tr -s " " | grep nc_join | grep -v grep | grep -v ssh |  cut -d " " -f2 | head -n 1` 
  PARENT_PROCESS_ENTRY=`ps -ef | grep $PARENT_ID | grep -v grep   | head -n 1`
  NC_ID=`echo ${PARENT_PROCESS_ENTRY##* }`
  echo "NCid is $NC_ID" >> ~/try.txt
else 
  PARENT_ID=`ps -ej | tr -s " " | grep nc_join | grep -v grep | grep -v ssh | grep $NC_ID | cut -d " " -f2 | head -n 1` 
fi 

PID=`ps -ej | tr -s " " | grep hyracks | grep -v grep | grep -v nc_join |  grep $PARENT_ID | cut -d " " -f2 | head -n 1`
kill -9 $PID

sleep $3

$HYRACKS_HOME/hyracks-server/target/hyracks-server-0.2.2-SNAPSHOT-binary-assembly/bin/hyracksnc -node-id $NC_ID -cc-host $CC_HOST -cc-port 1099 -cluster-net-ip-address $IP_LOCATION  -data-ip-address $IP_LOCATION
