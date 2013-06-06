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
USERNAME=$1
if [ ! -d $MANAGIX_HOME/logs ];
then
   mkdir -p $MANAGIX_HOME/logs
fi
LOGDIR=$MANAGIX_HOME/logs
if [ $DAEMON == "false" ]; then 
  if [ -z $USERNAME ]
  then
    cmd_output=$(ssh $IP_LOCATION "$ENV $SCRIPT $ARGS" 2>&1 >/dev/null) 
    echo "ssh $IP_LOCATION $ENV $SCRIPT $ARGS" >> $LOGDIR/execute.log
    echo "$cmd_output"
  else
    echo "ssh -l $USERNAME $IP_LOCATION $ENV $SCRIPT $ARGS" >> $LOGDIR/execute.log
    cmd_output=$(ssh -l $USERNAME $IP_LOCATION "$ENV $SCRIPT $ARGS" 2>&1 >/dev/null) 
    echo "$cmd_output"
  fi  
else 
  if [ -z $USERNAME ];
  then
     echo "ssh $IP_LOCATION $ENV $SCRIPT $ARGS &" >> $LOGDIR/execute.log
     ssh $IP_LOCATION "$ENV $SCRIPT $ARGS" &
  else
     echo "ssh -l $USERNAME $IP_LOCATION $ENV $SCRIPT $ARGS &" >> $LOGDIR/execute.log
     ssh -l $USERNAME $IP_LOCATION "$ENV $SCRIPT $ARGS" &
  fi   
fi
