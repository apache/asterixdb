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


TMP=/tmp/verify_

if [ $# -lt "2" ]
then
  echo "Usage: `basename $0` result.txt result.T.txt [-u]"
  exit $E_BADARGS
fi

args=( $@ )
for i in 0 1
do
    f=${args[$i]}
    ### Assume second argument is alredy sorted and with no duplicates
    if [ "$i" -eq "0" ]
    then
        sort $3 $f > $TMP$i
    else
        rm $TMP$i 2> /dev/null
        ln -s $PWD/$f $TMP$i
    fi
    if [ "$?" -ne "0" ]
    then
        echo Fail -- preprocessing
        exit 1
    fi

    l[$i]=`wc --lines $TMP$i | cut --delimiter=" " --fields=1`
    if [ "$?" -ne "0" ]
    then
        echo Fail -- preprocessing
        exit 1
    fi
done


### Test 1
if [ "${l[0]}" -ne "${l[1]}" ]
then
    echo $1 ${l[0]}
    echo $2 ${l[1]}
    echo Fail -- different number of tokens
    exit 1
fi

### Test 2
diff --brief ${TMP}0 ${TMP}1
if [ "$?" -ne "0" ]
then
    echo Fail -- differnt tokens
    exit 1
fi
rm ${TMP}0 ${TMP}1

echo Pass
