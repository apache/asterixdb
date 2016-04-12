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


DATA="/home/rares/data/fuzzyjoin/dblp/records-024"
FUZZYJOIN="/home/rares/fuzzyjoin/fuzzyjoin-core/target/fuzzyjoin-core-0.0.2-SNAPSHOT.jar"

echo "-- - Step 0: Project and append length - --"

# java -cp $FUZZYJOIN org.apache.fuzzyjoin.FuzzyJoinAppendLength $DATA/part-00000 $DATA/part-00000-len

date

echo "== START =="

echo "-- - Step 1: Sort by length - --"

# time sort -n -k 5 -t ":" $DATA/part-00000-len > $DATA/part-00000-len-sorted

echo "-- - Step 2: Tokenize - --"

# time java -cp $FUZZYJOIN org.apache.fuzzyjoin.FuzzyJoinTokenize $DATA/part-00000-len-sorted $DATA/part-00000-tokens $DATA/part-00000-tokenized

echo "-- - Step 3: RID pairs - --"

time java -Xmx8g -cp $FUZZYJOIN org.apache.fuzzyjoin.FuzzyJoinMemory .8 $DATA/part-00000-tokenized > $DATA/part-00000-ridpairs

echo "== END =="

date


### SSJoin ###
# cut -d ":" -f 3,4 records-000/part-0000* > ! ssjoin.raw-000/part-00000
# ~/workspace/ssjoin-bin/txtformat ssjoin.raw-000/part-00000 ssjoin.norm-000/part-00000 l
# sed 's/_\+/ /g' ssjoin.norm-000/part-00000 > ! ssjoin.space-000/part-00000
# ~/workspace/ssjoin-bin/tokenizer ssjoin.space-000/part-00000
# ~/workspace/ssjoin-bin/ppjoinplus j .8 ssjoin.space-000/part-00000.bin > /dev/null
# java -jar /fuzzyjoin/fuzzyjoin-core/target/fuzzyjoin-core-0.0.2-SNAPSHOT.jar .8 ssjoin.space-000/part-00000.bin > /dev/null
