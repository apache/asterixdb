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
HDFS_URL=${11}
HADOOP_VERSION=${12}
HADOOP_HOME=$WORKING_DIR/hadoop-$HADOOP_VERSION

iodevices=$(echo $ASTERIX_IODEVICES | tr "," "\n")

index=1
for iodevice in $iodevices
do
  NODE_BACKUP_DIR=$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID

  # remove the existing store directory
  STORE_DIR=$iodevice/$NODE_STORE

  # if STORE_DIR ends with a /, remove it
  if [ ${STORE_DIR:${#STORE_DIR}-1} == "/" ];
  then
    STORE_DIR=${STORE_DIR:0:${#STORE_DIR}-1}
  fi

  DEST_STORE_DIR=${STORE_DIR%/*}
  SOURCE_STORE_DIR=${NODE_STORE##*/}
  rm -rf $DEST_STORE_DIR/$SOURCE_STORE_DIR

  # remove the existing log directory
  DEST_LOG_DIR=$TXN_LOG_DIR
  rm -rf $DEST_LOG_DIR/*
  TXN_LOG_DIR_NAME=${TXN_LOG_DIR%/*}

  # remove the existing asterix metadata directory
  rm -rf $iodevice/$ASTERIX_ROOT_METADATA_DIR

  if [ $BACKUP_TYPE == "hdfs" ];
  then
      # RESTORE FROM HDFS BACKUP

      # copy store directory
      $HADOOP_HOME/bin/hadoop fs -copyToLocal $HDFS_URL/$NODE_BACKUP_DIR/$SOURCE_STORE_DIR  $DEST_STORE_DIR/ 

      # copy asterix metadata root directory and txn log directory
      if [ $index -eq 1 ];
      then
        $HADOOP_HOME/bin/hadoop fs -copyToLocal $HDFS_URL/$NODE_BACKUP_DIR/$ASTERIX_ROOT_METADATA_DIR $iodevice/

        # copy transaction logs directory
        $HADOOP_HOME/bin/hadoop fs -copyToLocal $HDFS_URL/$NODE_BACKUP_DIR/$TXN_LOG_DIR_NAME $$TXN_LOG_DIR/
      fi

  else

      # RESTORE FROM LOCAL BACKUP
      # copy store directory
      cp  -r $NODE_BACKUP_DIR/$SOURCE_STORE_DIR  $DEST_STORE_DIR/ 

      # copy asterix metadata root directory and txn log directory
      if [ $index -eq 1 ];
      then
        cp -r $NODE_BACKUP_DIR/$ASTERIX_ROOT_METADATA_DIR $iodevice/

        # copy transaction logs directory
        cp -r $NODE_BACKUP_DIR/$TXN_LOG_DIR_NAME $TXN_LOG_DIR/
      fi

  fi
  index=`expr $index + 1`
done
