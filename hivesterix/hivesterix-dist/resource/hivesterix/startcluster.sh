../asterix/stopall.sh
$HADOOP_HOME/bin/stop-all.sh
sleep 10
../asterix/startall.sh
$HADOOP_HOME/bin/start-dfs.sh
sleep 10
$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave
rm -rf metastore*

