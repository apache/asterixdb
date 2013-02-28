USERNAME=$1
FILE_TO_TRANSFER=$2
DEST_HOST=$3
DEST_DIR=$4
POST_ACTION=$5
ssh -l $USERNAME $DEST_HOST "mkdir -p $DEST_DIR"
echo "scp $FILE_TO_TRANSFER $USERNAME@$DEST_HOST:$DEST_DIR/" 
scp $FILE_TO_TRANSFER $USERNAME@$DEST_HOST:$DEST_DIR/
if [ $POST_ACTION == "unpack" ]
 then 
 filename=`echo ${FILE_TO_TRANSFER##*/}`
 fileType=`echo ${FILE_TO_TRANSFER##*.}`
 if [ $fileType == "tar" ]
 then 
   echo "ssh -l $USERNAME $DEST_HOST cd $DEST_DIR && tar xf $filename"
   ssh -l $USERNAME $DEST_HOST "cd $DEST_DIR && tar xf $filename"
 else if [ $fileType == "zip" ]
   then
     echo "ssh -l $USERNAME $DEST_HOST unzip -o -q -d $DEST_DIR $DEST_DIR/$filename"
     ssh -l $USERNAME $DEST_HOST "unzip -o -q -d $DEST_DIR $DEST_DIR/$filename"
     ssh -l $USERNAME $DEST_HOST "chmod -R 755  $DEST_DIR"
   fi 
 fi
fi
