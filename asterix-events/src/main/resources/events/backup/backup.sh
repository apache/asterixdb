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
ASTERIX_INSTANCE_NAME=$2
ASTERIX_IODEVICES=$3
NODE_STORE=$4
ASTERIX_ROOT_METADATA_DIR=$5
TXN_LOG_DIR=$6
BACKUP_ID=$7
BACKUP_DIR=$8
BACKUP_TYPE=$9
NODE_ID=${10}

nodeIODevices=$(echo $ASTERIX_IODEVICES | tr "," "\n")

if [ $BACKUP_TYPE == "hdfs" ];
then
  HDFS_URL=${11}
  HADOOP_VERSION=${12}
  export HADOOP_HOME=$WORKING_DIR/hadoop-$HADOOP_VERSION
  index=1
  for nodeIODevice in $nodeIODevices
  do
    STORE_DIR=$nodeIODevice/$NODE_STORE
    NODE_BACKUP_DIR=$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/
   
    # make the destination directory 
    $HADOOP_HOME/bin/hadoop fs -mkdir $STORE_DIR $HDFS_URL/$NODE_BACKUP_DIR

    # copy store directory
    $HADOOP_HOME/bin/hadoop fs -copyFromLocal $STORE_DIR $HDFS_URL/$NODE_BACKUP_DIR/

    # copy asterix root metadata directory and log directory from the primary(first) iodevice
    if [ $index -eq 1 ];
    then
      # copy asterix root metadata directory
      $HADOOP_HOME/bin/hadoop fs -copyFromLocal $nodeIODevice/$ASTERIX_ROOT_METADATA_DIR $HDFS_URL/$NODE_BACKUP_DIR/

      # copy log directory 
      $HADOOP_HOME/bin/hadoop fs -copyFromLocal $TXN_LOG_DIR $HDFS_URL/$NODE_BACKUP_DIR/
    fi

    index=`expr $index + 1`
  done
else 
  index=1
  for nodeIODevice in $nodeIODevices
  do
    STORE_DIR=$nodeIODevice/$NODE_STORE
    NODE_BACKUP_DIR=$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID

    # create the backup directory, if it does not exists
    if [ ! -d $NODE_BACKUP_DIR ];
    then
      mkdir -p $NODE_BACKUP_DIR
    fi

    # copy store directory
    cp -r $STORE_DIR $NODE_BACKUP_DIR/

    # copy asterix root metadata directory and log directory from the primary(first) iodevice
    if [ $index -eq 1 ];
    then
      cp -r $nodeIODevice/$ASTERIX_ROOT_METADATA_DIR  $NODE_BACKUP_DIR/

      # copy log directory
      cp -r $TXN_LOG_DIR $NODE_BACKUP_DIR/
    fi

    index=`expr $index + 1`
  done
fi
