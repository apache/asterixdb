#!/bin/bash

# get the PIDs of java processes we started
if [ "$JAVA_HOME" != "" ]
then
    PIDS=`$JAVA_HOME/bin/jps`
else
    PIDS=`jps`
fi

while IFS='\n' read -ra MYPIDS; do
    for i in "${MYPIDS[@]}"; do
        pid=`echo $i | grep 'CCDriver\|NCDriver\|VirtualClusterDriver' | awk '{print $1}'`
        name=`echo $i | grep 'CCDriver\|NCDriver\|VirtualClusterDriver' | awk '{print $2}'`
        if [ "$pid" != "" ]
        then
            echo "Stopping $name: $pid"
            kill -9 $pid
        fi
    done
done <<< "$PIDS"
