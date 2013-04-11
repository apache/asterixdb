WORKING_DIR=$1
ASTERIX_INSTANCE_NAME=$2
<<<<<<< .working
ASTERIX_IODEVICES=$3
NODE_STORE=$4
ASTERIX_ROOT_METADATA_DIR=$5
TXN_LOG_DIR_NAME=$6
BACKUP_ID=$7
BACKUP_DIR=$8
BACKUP_TYPE=$9
NODE_ID=${10}
=======
ASTERIX_DATA_DIR=$3
BACKUP_ID=$4
BACKUP_DIR=$5
BACKUP_TYPE=$6
NODE_ID=$7
>>>>>>> .merge-right.r1677

<<<<<<< .working
nodeIODevices=$(echo $ASTERIX_IODEVICES | tr "," "\n")
=======
nodeStores=$(echo $ASTERIX_DATA_DIR | tr "," "\n")
>>>>>>> .merge-right.r1677

<<<<<<< .working
if [ $BACKUP_TYPE == "hdfs" ];
then
  HDFS_URL=${11}
  HADOOP_VERSION=${12}
  export HADOOP_HOME=$WORKING_DIR/hadoop-$HADOOP_VERSION
  index=1
  for nodeIODevice in $nodeIODevices
  do
    STORE_DIR=$nodeIODevice/$NODE_STORE
    TXN_LOG_DIR=$nodeIODevice/$TXN_LOG_DIR_NAME
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
    TXN_LOG_DIR=$nodeIODevice/$TXN_LOG_DIR_NAME
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
=======
if [ $BACKUP_TYPE == "hdfs" ];
then
  HDFS_URL=$8
  HADOOP_VERSION=$9
  export HADOOP_HOME=$WORKING_DIR/hadoop-$HADOOP_VERSION
  for nodeStore in $nodeStores
  do
    MANGLED_DIR_NAME=`echo $nodeStores | tr / _`
    NODE_BACKUP_DIR=$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$MANGLED_DIR_NAME
    echo "$HADOOP_HOME/bin/hadoop fs -copyFromLocal $nodeStore/$NODE_ID/$ASTERIX_INSTANCE_NAME/ $HDFS_URL/$NODE_BACKUP_DIR/" >> ~/backup.log
    $HADOOP_HOME/bin/hadoop fs -copyFromLocal $nodeStore/$NODE_ID/$ASTERIX_INSTANCE_NAME/ $HDFS_URL/$NODE_BACKUP_DIR/
  done
else 
  for nodeStore in $nodeStores
  do
    MANGLED_DIR_NAME=`echo $nodeStores | tr / _`
    NODE_BACKUP_DIR=$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$MANGLED_DIR_NAME
    if [ ! -d $NODE_BACKUP_DIR ];
    then
      mkdir -p $NODE_BACKUP_DIR
    fi
    echo "cp -r  $nodeStore/$NODE_ID/$ASTERIX_INSTANCE_NAME/* $NODE_BACKUP_DIR/" >> ~/backup.log
    cp -r  $nodeStore/$NODE_ID/$ASTERIX_INSTANCE_NAME/* $NODE_BACKUP_DIR/
  done
fi
>>>>>>> .merge-right.r1677
