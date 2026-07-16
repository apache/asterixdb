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
* [Connector API](#connector)
* [Administration APIs](#admin)
    * [Cluster Controller Details API](#ccdetails)
    * [Active Entity Statistics API](#activestats)
    * [Network Diagnostics API](#netdiagnostics)

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
* `dataverse` - Default dataverse for this statement (Optional). If the specified dataverse does not exist then this setting is ignored.
* `mode` - Result delivery mode. Possible values are `immediate`, `deferred`, `async` (default: `immediate`).
  If the delivery mode is `immediate` the query result is returned with the response.
  If the delivery mode is `deferred` the response contains a handle to the <a href="#queryresult">result</a>.
  If the delivery mode is `async` the response contains a handle to the query's <a href="#querystatus">status</a>.
* `readonly` - Reject DDL and DML statements, only accept the following kinds:
  [SELECT](sqlpp/manual.html#SELECT_statements), [USE](sqlpp/manual.html#Declarations),
  [DECLARE FUNCTION](sqlpp/manual.html#Declarations), and [SET](sqlpp/manual.html#Performance_tuning)
* `args` - (SQL++ only) A JSON array where each item is a value of a [positional query parameter](sqlpp/manual.html#Parameter_references)
* `$parameter_name` - (SQL++ only) a JSON value of a [named query parameter](sqlpp/manual.html#Parameter_references).
* `result-ttl` - Specifies how long the result set should be retained for `async` or `deferred` queries before being
  automatically discarded. The value can be specified as a duration string (e.g., `30m`, `1h`, `2d`) or as milliseconds.
  If not specified, the system default `result.ttl` configuration is used. This parameter is ignored for `immediate` mode.

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

## <a id="connector">GET /connector</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns the file splits (partition locations) for a dataset. This endpoint is primarily used by
  external runtime systems such as Pregelix or IMRU to pull data in parallel from existing AsterixDB datasets.
  The response includes the dataset's primary keys, record type schema, and an array of file splits with their
  IP addresses and file paths.

__Parameters__

* `dataverseName` - The name of the dataverse containing the dataset. Required.
* `datasetName` - The name of the dataset. Required.

__Command__

    $ curl -v "http://localhost:19002/connector?dataverseName=TinySocial&datasetName=FacebookUsers"

__Sample response__

    > GET /connector?dataverseName=TinySocial&datasetName=FacebookUsers HTTP/1.1
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
        "keys": "id",
        "type": {
            "type": "RECORD",
            "name": "FacebookUserType",
            "open": true,
            "fields": [
                { "name": "id", "type": "INT64" },
                { "name": "alias", "type": "STRING" },
                { "name": "name", "type": "STRING" },
                { "name": "user-since", "type": "DATETIME" },
                { "name": "friend-ids", "type": { "type": "UNORDEREDLIST", "item": "INT64" } },
                { "name": "employment", "type": { "type": "ORDEREDLIST", "item": { "type": "RECORD", ... } } }
            ]
        },
        "splits": [
            { "ip": "127.0.0.1", "path": "/data/storage/partition_0/TinySocial/FacebookUsers/0/FacebookUsers" },
            { "ip": "127.0.0.1", "path": "/data/storage/partition_1/TinySocial/FacebookUsers/0/FacebookUsers" }
        ]
    }

__Error response__

If the dataverse or dataset does not exist:

    {
        "error": "Dataset FacebookUsers does not exist in dataverse TinySocial"
    }

---

# <a id="admin">Administration APIs</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

The following endpoints provide administrative functionality for monitoring and managing the AsterixDB cluster.

## <a id="ccdetails">GET /admin/cluster/cc</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns detailed information about the Cluster Controller (CC). This endpoint provides access to
  CC configuration, runtime statistics, and thread dumps for debugging purposes.

__Sub-endpoints__

* `GET /admin/cluster/cc` - Returns summary information about the CC
* `GET /admin/cluster/cc/config` - Returns the CC configuration parameters
* `GET /admin/cluster/cc/stats` - Returns runtime statistics for the CC
* `GET /admin/cluster/cc/threaddump` - Returns a thread dump of the CC JVM

__Command (CC summary)__

    $ curl -v http://localhost:19002/admin/cluster/cc

__Sample response__

    > GET /admin/cluster/cc HTTP/1.1
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
        "configUri": "/admin/cluster/cc/config",
        "statsUri": "/admin/cluster/cc/stats",
        "threadDumpUri": "/admin/cluster/cc/threaddump"
    }

__Command (CC configuration)__

    $ curl -v http://localhost:19002/admin/cluster/cc/config

__Sample response__

    {
        "cc.address.host": "127.0.0.1",
        "cc.address.http.port": 19002,
        "cc.address.console.port": 19001,
        "cc.cluster.port": 19000,
        "cc.heartbeat.period": 10000,
        "cc.heartbeat.max.misses": 5,
        ...
    }

__Command (CC statistics)__

    $ curl -v http://localhost:19002/admin/cluster/cc/stats

__Sample response__

    {
        "heap-used": 268435456,
        "heap-max": 4294967296,
        "thread-count": 45,
        "system-load-average": 2.5,
        "gc-count": 12,
        "gc-time": 150
    }

__Command (CC thread dump)__

    $ curl -v http://localhost:19002/admin/cluster/cc/threaddump

__Sample response__

    {
        "date": "2024-01-15T10:30:00Z",
        "threads": [
            {
                "name": "main",
                "state": "RUNNABLE",
                "stack": [
                    "java.lang.Thread.dumpThreads(Native Method)",
                    "java.lang.Thread.getAllStackTraces(Thread.java:1610)",
                    ...
                ]
            },
            ...
        ]
    }

## <a id="activestats">GET /admin/active</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns statistics for all active entities in the cluster, such as running feeds.
  Active entities are long-running processes that continuously ingest or process data.
  The statistics include ingestion rates, processing metrics, and entity-specific information.

__Parameters__

* `{timeout}` - Optional path parameter specifying the cache expiry time in milliseconds for statistics.
  If the cached statistics are older than this value, they will be refreshed. Default is 2000ms.

__Command__

    $ curl -v http://localhost:19002/admin/active

__Sample response__

    > GET /admin/active HTTP/1.1
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
        "TinySocial.TwitterFeed": {
            "runtime": {
                "intake-rate": 1250,
                "store-rate": 1248,
                "records-ingested": 50000
            },
            "state": "ACTIVE",
            "start-time": "2024-01-15T08:00:00Z"
        }
    }

__Command (with custom timeout)__

    $ curl -v http://localhost:19002/admin/active/5000

This refreshes statistics if they are older than 5000ms.

__Sample response (no active entities)__

    { }

## <a id="netdiagnostics">GET /admin/net</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns network diagnostics information for a Node Controller (NC).
  This NC-level endpoint provides details about the network multiplexer/demultiplexer state,
  including channel information and connection statistics. This is useful for debugging
  network-related issues in the cluster.

__Note:__ This endpoint is available on Node Controllers (default port 19003), not the Cluster Controller.

__Command__

    $ curl -v http://localhost:19003/admin/net

__Sample response__

    > GET /admin/net HTTP/1.1
    > Host: localhost:19003
    > User-Agent: curl/7.43.0
    > Accept: */*
    >
    < HTTP/1.1 200 OK
    < transfer-encoding: chunked
    < connection: keep-alive
    < content-type: application/json; charset=utf-8
    <
    {
        "mux-demux": {
            "channels": [
                {
                    "remote-address": "127.0.0.1:19000",
                    "state": "CONNECTED",
                    "pending-writes": 0,
                    "pending-reads": 0
                }
            ],
            "total-connections": 5,
            "active-connections": 5
        }
    }

