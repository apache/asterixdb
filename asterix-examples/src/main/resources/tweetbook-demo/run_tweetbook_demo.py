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

import tweetbook_bootstrap
from urllib2 import URLError, urlopen
from urllib import urlencode
from json import loads, dumps
from bottle import route, run, template, static_file, request

# Core Routing
@route('/')
def jsontest():
    return template('tweetbook')

@route('/static/<filename:path>')
def send_static(filename):
    return static_file(filename, root='static')

# API Helpers
def build_response(endpoint, data):
    api_endpoint = "http://localhost:19002/" + endpoint

    try:
        # Encode data into url string
        urlresponse = urlopen(api_endpoint + '?' + urlencode(data))

        # There are some weird bits passed in from the Asterix JSON. 
        # We will remove them here before we pass the result string 
        # back to the frontend.
        urlresult = ""
        CHUNK = 16 * 1024
        while True:
            chunk = urlresponse.read(CHUNK)
            if not chunk: break
            urlresult += chunk

        # Create JSON dump of resulting response
        return loads(urlresult)

    except ValueError, e:
        pass

    except URLError, e:

        # Here we report possible errors in request fulfillment.
        if hasattr(e, 'reason'):
            print 'Failed to reach a server.'
            print 'Reason: ', e.reason

        elif hasattr(e, 'code'):
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code

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
    
res = tweetbook_bootstrap.bootstrap()
run(host='localhost', port=8080, debug=True)
