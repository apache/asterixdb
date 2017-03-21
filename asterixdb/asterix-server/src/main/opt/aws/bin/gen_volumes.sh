#!/bin/bash
# ------------------------------------------------------------
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
# ------------------------------------------------------------

# Number of instance-store volumes
count=$1

# Destination of the generated file for the volume list
dest=$2

# There is no instance-stores.
if [ $count -le 0 ]
then
    printf "vols: []\n" >$dest
    exit 0
fi

# Generates a list of instance-local volumes.
printf "vols:\n" >$dest

for i in $(seq 1 $count)
do
  printf '  - { device_name: /dev/xvd%c, ephemeral: ephemeral%d }\n' `printf "\x$(printf %x \`expr 97 + $i\`)"` \
    $(($i-1)) >> $dest
done
