#!/bin/bash

PID=`/usr/bin/jps | grep CCDriver | awk '{print $1}'`

echo $PID
kill -9 $PID
