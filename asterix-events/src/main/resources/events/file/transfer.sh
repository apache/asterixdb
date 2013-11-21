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
