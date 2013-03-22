WORKING_DIR=$1
ASTERIX_INSTANCE_NAME=$2
ASTERIX_DATA_DIR=$3
BACKUP_ID=$4
BACKUP_DIR=$5
BACKUP_TYPE=$6
NODE_ID=$7

nodeStores=$(echo $ASTERIX_DATA_DIR | tr "," "\n")

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
