WORKING_DIR=$1
ASTERIX_INSTANCE_NAME=$2
ASTERIX_IODEVICES=$3
NODE_STORE=$4
ASTERIX_ROOT_METADATA_DIR=$5
BACKUP_ID=$6
BACKUP_DIR=$7
BACKUP_TYPE=$8
NODE_ID=$9
HDFS_URL=${10}
HADOOP_VERSION=${11}

export HADOOP_HOME=$WORKING_DIR/hadoop-$HADOOP_VERSION
iodevices=$(echo $ASTERIX_IODEVICES | tr "," "\n")

index=1
for iodevice in $iodevices
do
  MANGLED_BACKUP_DIR=`echo $iodevice | tr / _`
  NODE_BACKUP_DIR=$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$MANGLED_BACKUP_DIR
  DEST_DIR=$iodevice/$NODE_STORE/
  if [ ! -d $DEST_DIR ]
  then 
    mkdir -p $DEST_DIR
  else 
    rm -rf $DEST_DIR
  fi

  if [ ! -d $iodevice/$ASTERIX_ROOT_METADATA_DIR ]
  then
    mkdir -p $iodevice/$ASTERIX_ROOT_METADATA_DIR
  else 
    rm -rf $iodevice/$ASTERIX_ROOT_METADATA_DIR
  fi 

  if [ $BACKUP_TYPE == "hdfs" ];
  then
    $HADOOP_HOME/bin/hadoop fs -copyToLocal $HDFS_URL/$NODE_BACKUP_DIR/*  $DEST_DIR/ 
    if [ $index -eq 1 ];
    then
      $HADOOP_HOME/bin/hadoop fs -copyToLocal $HDFS_URL/$BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$ASTERIX_ROOT_METADATA_DIR $iodevice/
    fi
  else
    echo "cp  -r $NODE_BACKUP_DIR/*  $DEST_DIR/" >> ~/restore.log
    cp  -r $NODE_BACKUP_DIR/*  $DEST_DIR/ 
    if [ $index -eq 1 ];
    then
      echo "cp -r $BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$ASTERIX_ROOT_METADATA_DIR $iodevice/" >> ~/restore.log
      cp -r $BACKUP_DIR/$ASTERIX_INSTANCE_NAME/$BACKUP_ID/$NODE_ID/$ASTERIX_ROOT_METADATA_DIR $iodevice/
    fi
  fi
  index=`expr $index + 1`
done
