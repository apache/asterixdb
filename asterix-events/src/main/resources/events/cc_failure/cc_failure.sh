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
#kill -9 `ps -ef  | grep hyracks | grep -v grep | cut -d "/" -f1 | tr -s " " | cut -d " " -f2`
CC_PARENT_ID_INFO=`ps -ef  | grep asterix | grep cc_start | grep -v ssh`
CC_PARENT_ID=`echo $CC_PARENT_ID_INFO | tr -s " " | cut -d " " -f2`
CC_ID_INFO=`ps -ef | grep asterix | grep $CC_PARENT_ID | grep -v bash`
CC_ID=`echo $CC_ID_INFO |  tr -s " " | cut -d " " -f2`
kill -9 $CC_ID
