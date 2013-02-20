WORKING_DIR=$1
ASTERIX_INSTANCE_NAME=$2
ASTERIX_DATA_DIR=$3
BACKUP_ID=$4
HDFS_URL=$5
HADOOP_VERSION=$6
HDFS_BACKUP_DIR=$7
NODE_ID=$8

export HADOOP_HOME=$WORKING_DIR/hadoop-$HADOOP_VERSION

nodeStores=$(echo $ASTERIX_DATA_DIR | tr "," "\n")
for nodeStore in $nodeStores
do
  NODE_BACKUP_DIR=$HDFS_BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$nodeStore
  DEST_DIR=$nodeStore/$NODE_ID/$ASTERIX_INSTANCE_NAME
  if [ ! -d $DEST_DIR ]
  then 
    mkdir -p $DEST_DIR
  else 
    rm -rf $DEST_DIR/*
  fi
  echo "$HADOOP_HOME/bin/hadoop fs -copyToLocal $HDFS_URL/$NODE_BACKUP_DIR/  $DEST_DIR/" >> ~/restore.log 
  $HADOOP_HOME/bin/hadoop fs -copyToLocal $HDFS_URL/$NODE_BACKUP_DIR/*  $DEST_DIR/ 
done
