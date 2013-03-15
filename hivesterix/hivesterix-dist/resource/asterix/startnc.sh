#!/bin/bash

export JAVA_HOME=/usr/local/java/vms/java

LOGSDIR=/mnt/data/sda/space/yingyi/hyracks/logs
HYRACKS_HOME=/home/yingyib/hyracks_asterix_stabilization

IPADDR=`/sbin/ifconfig eth0 | grep "inet addr" | awk '{print $2}' | cut -f 2 -d ':'`
NODEID=`ypcat hosts | grep asterix | grep "$IPADDR " | awk '{print $2}'`

rm -rf /mnt/data/sda/space/yingyi/tmp/*
rm -rf /mnt/data/sdb/space/yingyi/tmp/*
rm -rf /mnt/data/sdc/space/yingyi/tmp/*
rm -rf /mnt/data/sdd/space/yingyi/tmp/*


export JAVA_OPTS="-Xmx10G"

cd $LOGSDIR
echo $HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host 128.195.14.4 -cc-port 3099 -data-ip-address $IPADDR -node-id $NODEID
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host 10.1.0.1 -cc-port 1099 -cluster-net-ip-address $IPADDR -data-ip-address $IPADDR -node-id $NODEID -iodevices "/mnt/data/sda/space/yingyi/tmp/,/mnt/data/sdb/space/yingyi/tmp/,/mnt/data/sdc/space/yingyi/tmp/,/mnt/data/sdd/space/yingyi/tmp/" -frame-size 32768&> $LOGSDIR/$NODEID.log &
