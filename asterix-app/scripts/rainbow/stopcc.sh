#!/bin/bash

PID=`/usr/local/java/vms/java/bin/jps | grep CCDriver | awk '{print $1}'`

echo $PID
kill -9 $PID
