WORKING_DIR=$1
ASTERIX_INSTANCE_NAME=$2
ASTERIX_DATA_DIR=$3
BACKUP_ID=$4
BACKUP_DIR=$5
BACKUP_TYPE=$6
NODE_ID=$7
HDFS_URL=$8
HADOOP_VERSION=$9

export HADOOP_HOME=$WORKING_DIR/hadoop-$HADOOP_VERSION
nodeStores=$(echo $ASTERIX_DATA_DIR | tr "," "\n")

for nodeStore in $nodeStores
do
  MANGLED_BACKUP_DIR=`echo $nodeStore | tr / _`
  NODE_BACKUP_DIR=$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$MANGLED_BACKUP_DIR
  DEST_DIR=$nodeStore/$NODE_ID/$ASTERIX_INSTANCE_NAME
  if [ ! -d $DEST_DIR ]
  then 
    mkdir -p $DEST_DIR
  else 
    rm -rf $DEST_DIR/*
  fi
 
  if [ $BACKUP_TYPE == "hdfs" ];
  then
    $HADOOP_HOME/bin/hadoop fs -copyToLocal $HDFS_URL/$NODE_BACKUP_DIR/*  $DEST_DIR/ 
  else
    cp  -r $NODE_BACKUP_DIR/*  $DEST_DIR/ 
  fi
done
