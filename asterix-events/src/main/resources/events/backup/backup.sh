echo $@ >> ~/backup.log
WORKING_DIR=$1
ASTERIX_INSTANCE_NAME=$2
ASTERIX_IODEVICES=$3
NODE_STORE=$4
ASTERIX_ROOT_METADATA_DIR=$5
BACKUP_ID=$6
BACKUP_DIR=$7
BACKUP_TYPE=$8
NODE_ID=$9

nodeIODevices=$(echo $ASTERIX_IODEVICES | tr "," "\n")

if [ $BACKUP_TYPE == "hdfs" ];
then
  HDFS_URL=${10}
  HADOOP_VERSION=${11}
  export HADOOP_HOME=$WORKING_DIR/hadoop-$HADOOP_VERSION
  index=1
  for nodeIODevice in $nodeIODevices
  do
    STORE_DIR=$nodeIODevice/$NODE_STORE
    MANGLED_DIR_NAME=`echo $STORE_DIR | tr / _`
    NODE_BACKUP_DIR=$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$MANGLED_DIR_NAME
    $HADOOP_HOME/bin/hadoop fs -copyFromLocal $STORE_DIR/ $HDFS_URL/$NODE_BACKUP_DIR/
    if [ $index -eq 1 ];
    then
      $HADOOP_HOME/bin/hadoop fs -copyFromLocal $nodeIODevice/$ASTERIX_ROOT_METADATA_DIR $HDFS_URL/$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/
    fi
    index=`expr $index + 1`
  done
else 
  index=1
  for nodeIODevice in $nodeIODevices
  do
    STORE_DIR=$nodeIODevice/$NODE_STORE
    MANGLED_DIR_NAME=`echo $STORE_DIR | tr / _`
    NODE_BACKUP_DIR=$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$MANGLED_DIR_NAME
    if [ ! -d $NODE_BACKUP_DIR ];
    then
      mkdir -p $NODE_BACKUP_DIR
    fi
    cp -r  $STORE_DIR/* $NODE_BACKUP_DIR/
    if [ $index -eq 1 ];
    then
      cp -r $nodeIODevice/$ASTERIX_ROOT_METADATA_DIR $BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/
    fi
    index=`expr $index + 1`
  done
fi
