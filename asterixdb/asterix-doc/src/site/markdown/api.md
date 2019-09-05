<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->

# HTTP API to AsterixDB #

## <a id="toc">Table of Contents</a>

* [Query Service API](#queryservice)
* [Query Status API](#querystatus)
* [Query Result API](#queryresult)

## <a id="queryservice">POST /query/service</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns result for query as JSON.
  The response is a JSON object that contains some result metadata along with either an embedded result or an opaque
  handle that can be used to navigate to the result (see the decription of the `mode` parameter for more details).

__Parameters__

* `statement` - Specifies at least one valid SQL++ statement to run. The statements need to be urlencoded. Required.
* `pretty` - If the parameter `pretty` is given with the value `true`, the result will be indented. (Optional)
* `client_context_id` - A user-defined sequence of characters that the API receives and returns unchanged. This can be
  used e.g. to match individual requests, jobs, and responses. Another option could be to use it for groups of requests
  if an application decides to put e.g. an group identifier into that field to route groups of responses to a
  particular response processor.
* `mode` - Result delivery mode. Possible values are `immediate`, `deferred`, `async` (default: `immediate`).
  If the delivery mode is `immediate` the query result is returned with the response.
  If the delivery mode is `deferred` the response contains a handle to the <a href="#queryresult">result</a>.
  If the delivery mode is `async` the response contains a handle to the query's <a href="#querystatus">status</a>.
* `readonly` - Reject DDL and DML statements, only accept the following kinds:
  [SELECT](sqlpp/manual.html#SELECT_statements), [USE](sqlpp/manual.html#Declarations),
  [DECLARE FUNCTION](sqlpp/manual.html#Declarations), and [SET](sqlpp/manual.html#Performance_tuning)
* `args` - (SQL++ only) A JSON array where each item is a value of a [positional query parameter](sqlpp/manual.html#Parameter_references)
* `$parameter_name` - (SQL++ only) a JSON value of a [named query parameter](sqlpp/manual.html#Parameter_references).

__Command (immediate result delivery)__

    $ curl -v --data-urlencode "statement=select 1;" \
              --data pretty=true                     \
              --data client_context_id=xyz           \
              http://localhost:19002/query/service

__Sample response__

    > POST /query/service HTTP/1.1
    > Host: localhost:19002
    > User-Agent: curl/7.43.0
    > Accept: */*
    > Content-Length: 57
    > Content-Type: application/x-www-form-urlencoded
    >
    < HTTP/1.1 200 OK
    < transfer-encoding: chunked
    < connection: keep-alive
    < content-type: application/json; charset=utf-8
    <
    {
        "requestID": "5f72e78c-482a-45bf-b174-6443c8273025",
        "clientContextID": "xyz",
        "signature": "*",
        "results": [ {
            "$1" : 1
        } ]
        ,
        "status": "success",
        "metrics": {
            "elapsedTime": "20.263371ms",
            "executionTime": "19.889389ms",
            "resultCount": 1,
            "resultSize": 15
        }
    }

__Command (<a id="deferred">deferred result delivery</a>)__

    $ curl -v --data-urlencode "statement=select 1;" \
              --data mode=deferred                   \
              http://localhost:19002/query/service

__Sample response__

    > POST /query/service HTTP/1.1
    > Host: localhost:19002
    > User-Agent: curl/7.43.0
    > Accept: */*
    > Content-Length: 37
    > Content-Type: application/x-www-form-urlencoded
    >
    < HTTP/1.1 200 OK
    < transfer-encoding: chunked
    < connection: keep-alive
    < content-type: application/json; charset=utf-8
    <
    {
        "requestID": "6df7afb4-5f83-49b6-8c4b-f11ec84c4d7e",
        "signature": "*",
        "handle": "http://localhost:19002/query/service/result/7-0",
        "status": "success",
        "metrics": {
            "elapsedTime": "12.270570ms",
            "executionTime": "11.948343ms",
            "resultCount": 0,
            "resultSize": 0
        }
    }

__Command (<a id="async>">async result delivery</a>)__

    $ curl -v --data-urlencode "statement=select 1;" \
              --data mode=async                      \
              http://localhost:19002/query/service

__Sample response__

    > POST /query/service HTTP/1.1
    > Host: localhost:19002
    > User-Agent: curl/7.43.0
    > Accept: */*
    > Content-Length: 34
    > Content-Type: application/x-www-form-urlencoded
    >
    < HTTP/1.1 200 OK
    < transfer-encoding: chunked
    < connection: keep-alive
    < content-type: application/json; charset=utf-8
    <
    {
        "requestID": "c5858420-d821-4c0c-81a4-2364386827c2",
        "signature": "*",
        "status": "running",
        "handle": "http://localhost:19002/query/service/status/9-0",
        "metrics": {
            "elapsedTime": "9.727006ms",
            "executionTime": "9.402282ms",
            "resultCount": 0,
            "resultSize": 0
        }
    }


## <a id="querystatus">GET /query/service/status</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns status of an `async` query request.
  The response is a JSON object that has a similar structure to the responses for the <a
  href="#queryservice">/query/service</a> endpoint.
  Possible status values for the status are `running`, `success`, `timeout`, `failed`, and `fatal`.
  If the status value is `success`, the response also contains a handle to the <a href="#queryresult">result</a>.
  URLs for this endpoint are usually not constructed by the application, they are simply extracted from the `handle`
  field of the response to a request to the <a href="#queryservice">/query/service</a> endpoint.

__Command__

  This example shows a request/reponse for the (opaque) status handle that was returned by the <a href="#async">async
  result delivery</a> example.

    $ curl -v http://localhost:19002/query/service/status/9-0

__Sample response__

    > GET /query/service/status/9-0 HTTP/1.1
    > Host: localhost:19002
    > User-Agent: curl/7.43.0
    > Accept: */*
    >
    < HTTP/1.1 200 OK
    < transfer-encoding: chunked
    < connection: keep-alive
    < content-type: application/json; charset=utf-8
    <
    {
        "status": "success",
        "handle": "http://localhost:19002/query/service/result/9-0"
    }

## <a id="queryresult">GET /query/service/result</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns result set for an `async` or `deferred` query request.
  The response is a plain result without a wrapping JSON object.
  URLs for this endpoint are usually not constructed by the application, they are simply extracted from the `handle`
  field of the response to a request to the <a href="#queryservice">/query/service</a> or the <a
  href="#querystatus">/query/service/status</a> endpoint.

__Command__

  This example shows a request/reponse for the (opaque) result handle that was returned by the <a
  href="#deferred">deferred result delivery</a> example.

    $ curl -v http://localhost:19002/query/service/result/7-0

__Sample response__

    > GET /query/service/result/7-0 HTTP/1.1
    > Host: localhost:19002
    > User-Agent: curl/7.43.0
    > Accept: */*
    >
    < HTTP/1.1 200 OK
    < transfer-encoding: chunked
    < connection: keep-alive
    < content-type: application/json
    <
    [ { "$1": 1 }
     ]

