#!/bin/bash

PID=`/usr/local/java/vms/java/bin/jps | grep NCDriver | awk '{print $1}'`

echo $PID
kill -9 $PID
