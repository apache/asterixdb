#/*
# Copyright 2009-2013 by The Regents of the University of California
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# you may obtain a copy of the License from
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*/
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
