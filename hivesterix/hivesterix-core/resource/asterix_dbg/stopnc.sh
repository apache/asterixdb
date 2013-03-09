#!/bin/bash

#PID=`/usr/local/java/vms/java/bin/jps | grep NCDriver | awk '{print $1}'`

PID=`ps -ef|grep yingyib|grep java|grep hyracks|awk '{print $2}'`

echo $PID
kill -9 $PID

PID=`ps -ef|grep yingyib|grep java|grep datanode|awk '{print $2}'`

echo $PID
kill -9 $PID


PID=`ps -ef|grep yingyib|grep java|grep tasktracker|awk '{print $2}'`

echo $PID
kill -9 $PID

rm -rf /mnt/data/sda/space/yingyi/tmp/*
rm -rf /mnt/data/sdb/space/yingyi/tmp/*
rm -rf /mnt/data/sdc/space/yingyi/tmp/*
rm -rf /mnt/data/sdd/space/yingyi/tmp/*
