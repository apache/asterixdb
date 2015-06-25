#!/usr/bin/env bash

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
