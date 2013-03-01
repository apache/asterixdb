#asterix/stopall.sh

#$HADOOP_HOME/bin/stop-all.sh
#$HADOOP_HOME/bin/start-all.sh
#sleep 10

#asterix/startall.sh

LOG=perflog/result.log
echo "">$LOG

for file in $1/*.hive 
do
    ../asterix/stopall.sh
    $HADOOP_HOME/bin/stop-all.sh
	sleep 10
	../asterix/startall.sh
	$HADOOP_HOME/bin/start-dfs.sh
	sleep 10
	$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave

	START=$(date +%s)
 	echo $file  
 	../bin/hive -f $file > perflog/$file
 	END=$(date +%s)
	DIFF=$(( $END - $START ))
	echo $file	 $DIFF>>$LOG
done
