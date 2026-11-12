#!/usr/bin/env bash
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

# Converted from the Jenkins job asterix-verify-txnlog.
set -x

WORKSPACE=${WORKSPACE:-$GITHUB_WORKSPACE}

function shutdown {
    $MY_INSTALLATION/opt/local/bin/stop-sample-cluster.sh -f
}
trap "shutdown" EXIT

function kill_cc_and_ncs {
    if ps -ef | grep 'java.*org\.apache\.hyracks\.control\.[cn]c\.\([CN]CDriver\|service\.NCService\)' > /tmp/$$_pids; then
        cat /tmp/$$_pids | while read line; do
             echo -n "   - $line..."
             echo $line | awk '{ print $2 }' | xargs -n1 kill -9
             echo "killed"
        done
    fi
}


if [ -d $WORKSPACE/patch-parent/asterixdb/asterix-server/target/asterix-server-*-binary-assembly/apache-* ]; then
 export PARENT_INSTALLATION=$WORKSPACE/patch-parent/asterixdb/asterix-server/target/asterix-server-*-binary-assembly/apache-*
elif [ -d $WORKSPACE/patch-parent/asterixdb/asterix-server/target/apache-*/apache-* ]; then
 export PARENT_INSTALLATION=$WORKSPACE/patch-parent/asterixdb/asterix-server/target/apache-*/apache-*
else
  export PARENT_INSTALLATION=$WORKSPACE/patch-parent/asterixdb/asterix-server/target/asterix-server-*-binary-assembly

fi

if [ -d $WORKSPACE/patch/asterixdb/asterix-server/target/asterix-server-*-binary-assembly/apache-* ]; then
  export MY_INSTALLATION=$WORKSPACE/patch/asterixdb/asterix-server/target/asterix-server-*-binary-assembly/apache-*
elif [ -d $WORKSPACE/patch/asterixdb/asterix-server/target/apache-*/apache-* ]; then
 export MY_INSTALLATION=$WORKSPACE/patch/asterixdb/asterix-server/target/apache-*/apache-*
else
  export MY_INSTALLATION=$WORKSPACE/patch/asterixdb/asterix-server/target/asterix-server-*-binary-assembly  
fi

$PARENT_INSTALLATION/opt/local/bin/start-sample-cluster.sh || exit 1

curl -X POST -v -u admin:admin -H "Accept: application/x-adm" "http://localhost:19002/query/service" --data format=json --data-urlencode 'statement=drop  dataverse test if exists;
  create  dataverse test;

  use test;


  create type test.AddressType as
  {
    number : bigint,
    street : string,
    city : string
  };

  create type test.AllType as
  {
    id : bigint,
    string : string,
    float : float,
    double : double,
    boolean : boolean,
    int8 : tinyint,
    int16 : smallint,
    int32 : integer,
    int64 : bigint,
    unorderedList : {{string}},
    orderedList : [string],
    record : AddressType,
    date : date,
    time : time,
    datetime : datetime,
    duration : duration,
    point : point,
    point3d : point3d,
    line : line,
    rectangle : rectangle,
    polygon : polygon,
    circle : circle,
    binary : binary,
    uuid : uuid
  };
  create type test.fooType as {
    id: bigint
  };
  create dataset test.`All`(AllType) primary key id;
  create dataset test.`Big`(fooType) primary key id;
  insert into test.`All` { "id": 10, "string": "Nancy", "float": 32.5, "double": -2013.5938237483274, "boolean": true, "int8": 125, "int16": 32765, "int32": 294967295, "int64": 1700000000000000000, "unorderedList": {{ "reading", "writing" }}, "orderedList": [ "Brad", "Scott" ], "record": { "number": 8389, "street": "Hill St.", "city": "Mountain View" }, "date": date("-2011-01-27"), "time": time("12:20:30.000Z"), "datetime": datetime("-1951-12-27T12:20:30.000Z"), "duration": duration("P10Y11M12DT10H50M30S"), "point": point("41.0,44.0"), "point3d": point3d("44.0,13.0,41.0"), "line": line("10.1,11.1 10.2,11.2"), "rectangle": rectangle("5.1,11.8 87.6,15.6548"), "polygon": polygon("1.2,1.3 2.1,2.5 3.5,3.6 4.6,4.8"), "circle": circle("10.1,11.1 10.2"), "binary": hex("ABCDEF0123456789"), "uuid": uuid("5c848e5c-6b6a-498f-8452-8847a2957421") } ; ' || exit 1
  DATA=`cat $WORKSPACE/patch/asterixdb/asterix-app/src/test/resources/runtimets/results/big-object/big_object_bulkload/big_object_bulkload.1.adm `
  curl -X POST -u admin:admin -H "Content-Type: application/json" "http://localhost:19002/query/service"  -d @- <<CURL_DATA
  use test;
  insert into test.Big
  $DATA ;
CURL_DATA
(($?)) && exit 1

  curl -u admin:admin -H "Accept: application/x-adm" "http://localhost:19002/query/service" --data format=json --data-urlencode 'statement= select id, string, float, double, boolean, int8, int16, int32, int64, unorderedList, orderedList, record, date,  print_time(`time`,"hh:mm:ss.nnn") as time, print_datetime(`datetime`, "YYYY-MM-DDThh:mm:ss.nnn") as datetime, duration, point, point3d, line, rectangle, polygon, circle, binary, uuid from test.`All`;' | python3 -c 'import sys, json;
all = json.loads(
"""
{ "id": 10, "string": "Nancy", "float": 32.5, "double": -2013.5938237483274, "boolean": true, "int8": 125, "int16": 32765, "int32": 294967295, "int64": 1700000000000000000, "unorderedList": [ "reading", "writing" ], "orderedList": [ "Brad", "Scott" ], "record": { "number": 8389, "street": "Hill St.", "city": "Mountain View" }, "date": "-2011-01-27", "time": "12:20:30.000", "datetime": "-1951-12-27T12:20:30.000", "duration": "P10Y11M12DT10H50M30S", "point": [41.0, 44.0], "point3d": [44.0, 13.0, 41.0], "line": [ [10.1, 11.1], [10.2, 11.2] ], "rectangle": [ [5.1, 11.8], [87.6, 15.6548] ], "polygon": [ [1.2, 1.3], [2.1, 2.5], [3.5, 3.6], [4.6, 4.8] ], "circle": [ [10.1, 11.1], 10.2 ], "binary": "ABCDEF0123456789", "uuid": "5c848e5c-6b6a-498f-8452-8847a2957421" }
""")
test = json.load(sys.stdin)["results"][0]
print(json.dumps(test, indent=4))
print(json.dumps(all, indent=4))
if test == all:
   sys.exit(0)
else:
   sys.exit(1)
' || exit 1

kill_cc_and_ncs

cp -r $PARENT_INSTALLATION/opt/local/data/ $MY_INSTALLATION/opt/local/
cp -r $PARENT_INSTALLATION/opt/local/conf/ $MY_INSTALLATION/opt/local/

$MY_INSTALLATION/opt/local/bin/start-sample-cluster.sh || exit 1

curl -u admin:admin -H "Accept: application/x-adm" "http://localhost:19002/query/service" --data format=json --data-urlencode 'statement= select id, string, float, double, boolean, int8, int16, int32, int64, unorderedList, orderedList, record, date,  print_time(`time`,"hh:mm:ss.nnn") as time, print_datetime(`datetime`, "YYYY-MM-DDThh:mm:ss.nnn") as datetime, duration, point, point3d, line, rectangle, polygon, circle, binary, uuid from test.`All`;' | python3 -c 'import sys, json;
all = json.loads(
"""
{ "id": 10, "string": "Nancy", "float": 32.5, "double": -2013.5938237483274, "boolean": true, "int8": 125, "int16": 32765, "int32": 294967295, "int64": 1700000000000000000, "unorderedList": [ "reading", "writing" ], "orderedList": [ "Brad", "Scott" ], "record": { "number": 8389, "street": "Hill St.", "city": "Mountain View" }, "date": "-2011-01-27", "time": "12:20:30.000", "datetime": "-1951-12-27T12:20:30.000", "duration": "P10Y11M12DT10H50M30S", "point": [41.0, 44.0], "point3d": [44.0, 13.0, 41.0], "line": [ [10.1, 11.1], [10.2, 11.2] ], "rectangle": [ [5.1, 11.8], [87.6, 15.6548] ], "polygon": [ [1.2, 1.3], [2.1, 2.5], [3.5, 3.6], [4.6, 4.8] ], "circle": [ [10.1, 11.1], 10.2 ], "binary": "ABCDEF0123456789", "uuid": "5c848e5c-6b6a-498f-8452-8847a2957421" }
""")
test = json.load(sys.stdin)["results"][0]
print(json.dumps(test, indent=4))
print(json.dumps(all, indent=4))
if test == all:
   sys.exit(0)
else:
   sys.exit(1)
' || exit 1

$MY_INSTALLATION/opt/local/bin/stop-sample-cluster.sh -f
