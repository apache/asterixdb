hostname
. conf/cluster.properties

#Kill process
PID=`ps -ef|grep ${USER}|grep java|grep hyracks|awk '{print $2}'`
echo $PID
kill -9 $PID

#Clean up CC temp dir
rm -rf $CCTMP_DIR/*
