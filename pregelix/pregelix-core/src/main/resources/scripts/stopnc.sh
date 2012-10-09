hostname
. conf/cluster.properties

#Kill process
PID=`ps -ef|grep ${USER}|grep java|grep hyracks|awk '{print $2}'`
echo $PID
kill -9 $PID

#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS | tr "," "\n")
for io_dir in $io_dirs
do
	rm -rf $io_dir/*
done

#Clean up NC temp dir
rm -rf $NCTMP_DIR/*