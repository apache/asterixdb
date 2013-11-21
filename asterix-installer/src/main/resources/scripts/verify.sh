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
INSTANCE_NAME=$1
MASTER_NODE=$2
shift 2
numargs=$#
for ((i=1 ; i <= numargs ; i=i+2))
do
 host=$1
 nc_id=$2
 INFO=$(ssh $host "ps -ef | grep nc_join | grep -v grep | grep -v ssh| grep $nc_id" | head -n 1 )
 PARENT_ID=`echo  $INFO | cut -d " "  -f2`
 PID_INFO=$(ssh $host "ps -ef |  grep asterix | grep -v grep | grep -v nc_join |  grep $PARENT_ID") 
 PID=`echo $PID_INFO | cut -d " " -f2`
 echo "NC:$host:$nc_id:$PID"
 shift 2
done

CC_PARENT_ID_INFO=$(ssh $MASTER_NODE "ps -ef  | grep asterix | grep cc_start | grep -v ssh")
CC_PARENT_ID=`echo $CC_PARENT_ID_INFO | tr -s " " | cut -d " " -f2` 
CC_ID_INFO=$(ssh $MASTER_NODE "ps -ef | grep asterix | grep $CC_PARENT_ID | grep -v bash")
CC_ID=`echo $CC_ID_INFO |  tr -s " " | cut -d " " -f2`
echo "CC:$MASTER_NODE:N/A:$CC_ID"
