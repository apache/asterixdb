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
WORKING_DIR=$1
HADOOP_VERSION=$2
HDFS_URL=$3
HDFS_PATH=$4
export HADOOP_HOME=$WORKING_DIR/hadoop-$HADOOP_VERSION
echo "$HADOOP_HOME/bin/hadoop fs -rmr $HDFS_URL/$HDFS_PATH"
$HADOOP_HOME/bin/hadoop fs -rmr $HDFS_URL/$HDFS_PATH
