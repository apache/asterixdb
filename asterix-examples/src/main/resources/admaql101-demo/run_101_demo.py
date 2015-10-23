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

import admaql101
import requests
from bottle import route, run, template, get, debug, static_file, request, response

debug(True)
http_header = { "content-type": "application/json" }

# Core Routing
@route('/')
def jsontest():
    return template('demo')

@route('/static/<filename:path>')
def send_static(filename):
    return static_file(filename, root='static')

# API Helpers
def build_response(endpoint, data):
    api_endpoint = "http://localhost:19002/" + endpoint
    response = requests.get(api_endpoint, params=data, headers=http_header)
    try:
        return response.json();
    except ValueError:
        return []

# API Endpoints    
@route('/query')
def run_asterix_query():
    return (build_response("query", dict(request.query)))
    
@route('/query/status')
def run_asterix_query_status():
    return (build_response("query/status", dict(request.query)))

@route('/query/result')
def run_asterix_query_result():
    return (build_response("query/result", dict(request.query)))


@route('/ddl')
def run_asterix_ddl():
    return (build_response("ddl", dict(request.query)))

@route('/update')
def run_asterix_update():
    return (build_response("update", dict(request.query)))
    
res = admaql101.bootstrap()
run(host='localhost', port=8081, debug=True)
