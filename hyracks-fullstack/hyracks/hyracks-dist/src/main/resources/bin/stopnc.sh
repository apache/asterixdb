#/*
 # Licensed to the Apache Software Foundation (ASF) under one
 # or more contributor license agreements.  See the NOTICE file
 # distributed with this work for additional information
 # regarding copyright ownership.  The ASF licenses this file
 # to you under the Apache License, Version 2.0 (the
 # "License"); you may not use this file except in compliance
 # with the License.  You may obtain a copy of the License at
 #
 #   http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing,
 # software distributed under the License is distributed on an
 # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 # KIND, either express or implied.  See the License for the
 # specific language governing permissions and limitations
 # under the License.
 #*/
hostname
. conf/cluster.properties

#Kill process
PID=`ps -ef|grep ${USER}|grep java|grep 'Dapp.name=hyracksnc'|awk '{print $2}'`

if [ "$PID" == "" ]; then
  PID=`ps -ef|grep ${USER}|grep java|grep 'hyracks'|awk '{print $2}'`
fi

if [ "$PID" == "" ]; then
  USERID=`id | sed 's/^uid=//;s/(.*$//'`
  PID=`ps -ef|grep ${USERID}|grep java|grep 'Dapp.name=hyracksnc'|awk '{print $2}'`
fi

echo $PID
kill -9 $PID

#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS | tr "," "\n")
for io_dir in $io_dirs
do
    rm -rf $io_dir/*
done

#Clean up NC temp dir
rm -rf $NCTMP_DIR/*
