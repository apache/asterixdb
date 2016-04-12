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

import os
import json
import sys

import requests
from requests import ConnectionError, HTTPError

def bootstrap():
    asterix_host = "localhost"
    asterix_port = 19002

    # First, let's get the path to this script, we'll need it to configure the demo.
    base = os.path.dirname(os.path.realpath(__file__))
    print "Running Tweetbook Demo from",base

    # First, we bootstrap our request query
    print "Preparing Dataset..."
    query_statement = open("static/data/query.txt").read().split("load")
    query_statement[1] = "load" + query_statement[1].replace("FULL_PATH",base + "/static/data")

    ddl = {
        'ddl': query_statement[0]
    }

    load = {
        "statements" : "use dataverse twitter; " + query_statement[1]
    }

    http_header = {
        'content-type': 'application/json'
    }

    # Now we run our query
    print "Running query...\n"

    ddl_url = "http://" + asterix_host + ":" + str(asterix_port) + "/ddl"
    update_url = "http://" + asterix_host + ":" + str(asterix_port) + "/update"

    try:
        requests.get(ddl_url, params=ddl)
        response = requests.get(update_url, params=load, headers=http_header)
    except (ConnectionError, HTTPError):
        print "Encountered connection error; stopping execution"
        sys.exit(1)

    return True
