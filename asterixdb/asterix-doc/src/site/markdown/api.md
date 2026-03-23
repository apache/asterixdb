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
* [Administration APIs](#admin)
    * [Cluster Information API](#clusterinfo)
    * [Node Details API](#nodedetails)
    * [Running Requests API](#runningrequests)
    * [Diagnostics API](#diagnostics)
    * [Version API](#version)
    * [Shutdown API](#shutdown)
    * [Rebalance API](#rebalance)
    * [Storage API](#storage)
    * [UDF API](#udf)

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

---

# <a id="admin">Administration APIs</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

The following endpoints provide administrative operations for monitoring and managing an AsterixDB cluster.

## <a id="clusterinfo">GET /admin/cluster</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns the current state of the cluster, including information about the cluster controller (CC),
  node controllers (NCs), and configuration. The response includes URIs for accessing detailed information about
  individual nodes, shutdown operations, and diagnostics.

__Command__

    $ curl -v http://localhost:19002/admin/cluster

__Sample response__

    > GET /admin/cluster HTTP/1.1
    > Host: localhost:19002
    > User-Agent: curl/7.43.0
    > Accept: */*
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    {
        "cc": {
            "configUri": "http://localhost:19002/admin/cluster/cc/config",
            "statsUri": "http://localhost:19002/admin/cluster/cc/stats",
            "threadDumpUri": "http://localhost:19002/admin/cluster/cc/threaddump"
        },
        "config": { ... },
        "ncs": [
            {
                "node_id": "nc1",
                "state": "ACTIVE",
                "configUri": "http://localhost:19002/admin/cluster/node/nc1/config",
                "statsUri": "http://localhost:19002/admin/cluster/node/nc1/stats",
                "threadDumpUri": "http://localhost:19002/admin/cluster/node/nc1/threaddump"
            }
        ],
        "state": "ACTIVE",
        "shutdownUri": "http://localhost:19002/admin/shutdown",
        "fullShutdownUri": "http://localhost:19002/admin/shutdown?all=true",
        "versionUri": "http://localhost:19002/admin/version",
        "diagnosticsUri": "http://localhost:19002/admin/diagnostics"
    }

__Cluster Summary__

To get a brief summary of the cluster state, use the `/admin/cluster/summary` endpoint:

    $ curl http://localhost:19002/admin/cluster/summary

## <a id="nodedetails">GET /admin/cluster/node</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns detailed information about node controllers in the cluster.

__Endpoints__

* `GET /admin/cluster/node` - List all node controllers with their URIs
* `GET /admin/cluster/node/{node_id}` - Get information about a specific node
* `GET /admin/cluster/node/{node_id}/config` - Get configuration of a specific node
* `GET /admin/cluster/node/{node_id}/stats` - Get runtime statistics of a specific node
* `GET /admin/cluster/node/{node_id}/threaddump` - Get thread dump of a specific node

__Command (node configuration)__

    $ curl -v http://localhost:19002/admin/cluster/node/nc1/config

__Sample response__

    > GET /admin/cluster/node/nc1/config HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    {
        "node_id": "nc1",
        "iodevices": "/tmp/nc1/iodevice1",
        "net_thread_count": 1,
        ...
    }

__Command (node statistics)__

    $ curl -v http://localhost:19002/admin/cluster/node/nc1/stats

__Sample response__

    > GET /admin/cluster/node/nc1/stats HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    {
        "heap_init_size": 268435456,
        "heap_used_size": 52428800,
        "heap_committed_size": 134217728,
        "heap_max_size": 536870912,
        "system_load_average": 1.5,
        ...
    }

## <a id="runningrequests">GET /admin/requests/running</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns a list of currently running requests (queries) in the cluster. This endpoint also supports
  cancelling running requests via the DELETE method.

__Command (list running requests)__

    $ curl -v http://localhost:19002/admin/requests/running

__Sample response__

    > GET /admin/requests/running HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    [
        {
            "uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "requestTime": "2024-01-15T10:30:00.000Z",
            "elapsedTime": "5.2s",
            "statement": "SELECT * FROM LargeDataset;",
            "clientContextID": "client-123"
        }
    ]

__Cancel a running request__

To cancel a running request, use the DELETE method with either the `request_id` or `client_context_id` parameter:

    $ curl -X DELETE "http://localhost:19002/admin/requests/running?request_id=a1b2c3d4-e5f6-7890-abcd-ef1234567890"

__Parameters for DELETE__

* `request_id` - The UUID of the request to cancel
* `client_context_id` - The client context ID of the request to cancel

__Response codes for DELETE__

* `200 OK` - Request was successfully cancelled
* `400 Bad Request` - Neither request_id nor client_context_id was provided
* `403 Forbidden` - Request cannot be cancelled (not cancellable)
* `404 Not Found` - Request not found

## <a id="diagnostics">GET /admin/diagnostics</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns comprehensive diagnostic information about the entire cluster, including thread dumps,
  configuration, and statistics for both the cluster controller and all node controllers.

__Command__

    $ curl -v http://localhost:19002/admin/diagnostics

__Sample response__

    > GET /admin/diagnostics HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    {
        "cc": {
            "threaddump": { ... },
            "config": { ... },
            "stats": { ... }
        },
        "ncs": [
            {
                "node_id": "nc1",
                "threaddump": { ... },
                "config": { ... },
                "stats": { ... }
            }
        ],
        "date": "Mon Jan 15 10:30:00 UTC 2024"
    }

## <a id="version">GET /admin/version</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns the build version and properties of the running AsterixDB instance.

__Command__

    $ curl -v http://localhost:19002/admin/version

__Sample response__

    > GET /admin/version HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: text/plain; charset=utf-8
    <
    {
        "build.version": "0.9.9-SNAPSHOT",
        "build.branch": "master",
        "build.commit.id": "abcdef1234567890",
        "build.commit.time": "2024-01-10T08:00:00Z",
        "build.timestamp": "2024-01-10T09:00:00Z"
    }

## <a id="shutdown">POST /admin/shutdown</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Initiates a graceful shutdown of the AsterixDB cluster. The cluster will stop accepting new queries
  and shut down after completing any in-progress operations.

__Parameters__

* `all` - If set to `true`, also terminates the NC service processes. (Optional, default: `false`)

__Command (shutdown cluster controller only)__

    $ curl -X POST http://localhost:19002/admin/shutdown

__Command (full shutdown including NC services)__

    $ curl -X POST "http://localhost:19002/admin/shutdown?all=true"

__Sample response__

    > POST /admin/shutdown HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 202 Accepted
    < content-type: application/json; charset=utf-8
    <
    {
        "status": "SHUTTING_DOWN",
        "date": "Mon Jan 15 10:30:00 UTC 2024",
        "cluster": {
            "ncs": [
                {
                    "node_id": "nc1",
                    "pid": 12345,
                    "state": "ACTIVE"
                }
            ],
            "state": "ACTIVE"
        }
    }

__Note:__ This endpoint returns HTTP 202 (Accepted) immediately and initiates shutdown asynchronously.

## <a id="rebalance">POST /admin/rebalance</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Triggers data rebalancing for datasets across the cluster. This redistributes data partitions
  to achieve better load balancing after adding or removing nodes.

__Parameters__

* `dataverseName` - The dataverse containing the dataset to rebalance (Optional)
* `datasetName` - The specific dataset to rebalance (Optional)
* `nodes` - Comma-separated list of target nodes for rebalancing (Optional)
* `forceRebalance` - Force rebalance even if data distribution seems optimal (Optional, default: `false`)

__Usage__

* If neither `dataverseName` nor `datasetName` is provided, all non-metadata datasets will be rebalanced.
* If only `dataverseName` is provided, all datasets in that dataverse will be rebalanced.
* If both `dataverseName` and `datasetName` are provided, only that specific dataset will be rebalanced.

__Command (rebalance a specific dataset)__

    $ curl -X POST "http://localhost:19002/admin/rebalance?dataverseName=Commerce&datasetName=Orders"

__Command (rebalance all datasets in a dataverse)__

    $ curl -X POST "http://localhost:19002/admin/rebalance?dataverseName=Commerce"

__Sample response__

    > POST /admin/rebalance HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    {
        "status": "success",
        "rebalanced": ["Commerce.Orders", "Commerce.Customers"]
    }

## <a id="storage">GET /admin/storage</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Returns storage information for partitions and replicas on a node controller. This is an NC-level
  endpoint that provides details about data partitions, their replicas, and storage statistics.

__Note:__ This endpoint is available on Node Controllers (NCs), not on the Cluster Controller (CC).

__Endpoints__

* `GET /admin/storage` - List all partitions and their replicas
* `GET /admin/storage/partition/{partition_id}` - Get information about a specific partition
* `GET /admin/storage/stats` - Get storage statistics for all resources

__Command (list partitions)__

    $ curl -v http://localhost:19003/admin/storage

__Sample response__

    > GET /admin/storage HTTP/1.1
    > Host: localhost:19003
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    [
        {
            "partition": 0,
            "replicas": [
                {
                    "location": "127.0.0.1:2001",
                    "status": "IN_SYNC",
                    "nodeId": "nc1"
                }
            ]
        },
        {
            "partition": 1,
            "replicas": [
                {
                    "location": "127.0.0.1:2002",
                    "status": "IN_SYNC",
                    "nodeId": "nc2"
                }
            ]
        }
    ]

__Command (storage statistics)__

    $ curl -v http://localhost:19003/admin/storage/stats

__POST operations__

The storage API also supports POST operations for managing replicas (typically used internally):

* `POST /admin/storage/addReplica` - Add a replica (params: `partition`, `host`, `port`, `nodeId`)
* `POST /admin/storage/removeReplica` - Remove a replica (params: `partition`, `host`, `port`, `nodeId`)
* `POST /admin/storage/promote` - Promote a partition (params: `partition`)
* `POST /admin/storage/release` - Release a partition (params: `partition`)

## <a id="udf">POST /admin/udf</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

__Description__ Manages User-Defined Functions (UDFs) and external libraries. This endpoint allows uploading,
  updating, and deleting UDF libraries.

__Note:__ This endpoint is restricted to loopback connections only for security reasons.

__Endpoints__

* `POST /admin/udf/{dataverse}/{library_name}` - Create or update a UDF library
* `DELETE /admin/udf/{dataverse}/{library_name}` - Delete a UDF library

__Parameters__

* `type` - The language type of the UDF (e.g., `JAVA`, `PYTHON`)
* The request body should contain the library file (JAR for Java, ZIP for Python)

__Command (upload a Java UDF library)__

    $ curl -X POST \
           -H "Content-Type: application/octet-stream" \
           --data-binary @mylib.jar \
           "http://localhost:19002/admin/udf/Commerce/mylib?type=JAVA"

__Command (delete a UDF library)__

    $ curl -X DELETE "http://localhost:19002/admin/udf/Commerce/mylib"

__Response codes__

* `200 OK` - Library was successfully created/updated/deleted
* `400 Bad Request` - Invalid parameters or malformed request
* `403 Forbidden` - Request not from loopback address

For more information about creating and using UDFs, see the [User-Defined Functions](udf.html) documentation.
