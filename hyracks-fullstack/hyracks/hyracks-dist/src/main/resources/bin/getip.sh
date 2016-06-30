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
#get the OS
OS_NAME=`uname -a|awk '{print $1}'`
LINUX_OS='Linux'

if [ $OS_NAME = $LINUX_OS ];
then
        #Get IP Address
        IPADDR=`/sbin/ifconfig eth0 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        if [ "$IPADDR" = "" ]
        then
        IPADDR=`/sbin/ifconfig em1 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        fi
    if [ "$IPADDR" = "" ]
        then
        IPADDR=`/sbin/ifconfig lo | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        fi
else
        IPADDR=`/sbin/ifconfig en1 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
    if [ "$IPADDR" = "" ]
        then
                IPADDR=`/sbin/ifconfig lo0 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        fi

fi
echo $IPADDR
