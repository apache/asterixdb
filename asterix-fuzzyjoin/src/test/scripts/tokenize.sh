#!/bin/bash
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


DIR=`dirname $0`; if [ "${DIR:0:1}" == "." ]; then DIR=`pwd`"${DIR:1}"; fi
source $DIR/conf.sh

ARGS=1                   # Required number of arguments
E_BADARGS=85             # Wrong number of arguments passed to script.
if [ $# -lt "$ARGS" ]
then
  echo "Usage:   `basename $0` dataset"
  echo "Example: `basename $0` dblp-small"
  exit $E_BADARGS
fi

$SSJOIN/tokenizer $DATA/$1/raw-000/part-00000 $2
mkdir $DATA/$1/$IN
mv $DATA/$1/raw-000/part-00000.bin $DATA/$1/$IN/part-00000

