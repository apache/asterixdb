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
    * [Cluster State API](#clusterstate)
    * [Cluster Controller Details API](#ccdetails)
    * [Node Controller Details API](#nodedetails)
    * [Diagnostics API](#diagnostics)
    * [Running Requests API](#runningrequests)
    * [Active Entity Statistics API](#activestats)
    * [Version API](#version)
    * [Plan Cache API](#plancache)
    * [Rebalance API](#rebalance)
    * [Shutdown API](#shutdown)
    * [Storage API](#storage)
    * [Network Diagnostics API](#netdiagnostics)
    * [UDF Library API](#udf)

## <a id="queryservice">POST /query/service</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

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

__Command (<a id="async">async result delivery</a>)__

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


## <a id="querystatus">GET /query/service/status</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

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

## <a id="queryresult">GET /query/service/result</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Returns result set for an `async` or `deferred` query request.
  The response is a plain result without a wrapping JSON object.
  URLs for this endpoint are usually not constructed by the application, they are simply extracted from the `handle`
  field of the response to a request to the <a href="#queryservice">/query/service</a> or the <a
  href="#querystatus">/query/service/status</a> endpoint.

__Command__

  This example shows a request/response for the (opaque) result handle that was returned by the <a
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

## <a id="connector">GET /connector</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Returns the file splits (partition locations) of a dataset, along with its primary keys and its record
  type. It is mostly used by external runtimes, e.g. Pregelix or IMRU, to pull data in parallel from existing AsterixDB
  datasets. As a side effect the dataset is flushed to disk before the splits are returned, so that the files referenced
  by the response contain all data that had been ingested when the request was made.

__Parameters__

* `dataverseName` - The dataverse that contains the dataset. Required. May be repeated to address a multi-part
  dataverse name, e.g. `dataverseName=a&dataverseName=b` for the dataverse `a.b`.
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
            "type": "org.apache.asterix.om.types.ARecordType",
            "name": "FacebookUserType",
            "open": true,
            "fields": [
                { "id": { "type": "AInt64" } },
                { "alias": { "type": "AString" } },
                { "name": { "type": "AString" } }
            ]
        },
        "splits": [
            { "ip": "127.0.0.1", "path": "storage/partition_0/TinySocial/FacebookUsers/0/FacebookUsers" },
            { "ip": "127.0.0.1", "path": "storage/partition_1/TinySocial/FacebookUsers/0/FacebookUsers" }
        ]
    }

__Error responses__

  Errors that are detected before the request is dispatched are reported in an `error` field of an otherwise normal
  `200 OK` response; only unexpected failures produce a `500 Internal Server Error`.

    { "error": "Parameter dataverseName or datasetName is null," }

    { "error": "Dataset FacebookUsers does not exist in dataverse TinySocial" }

---

# <a id="admin">Administration APIs</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

The following endpoints are used to monitor and manage a running AsterixDB cluster.

__Endpoint locations__

  Most administration endpoints are served by the Cluster Controller (CC), on the port configured by the `api.port`
  setting (default `19002`). The <a href="#storage">Storage</a>, <a href="#netdiagnostics">Network Diagnostics</a> and
  <a href="#udf">UDF Library</a> APIs are served by each Node Controller (NC) instead, on the port configured by the
  `nc.api.port` setting (default `19004`; subsequent NCs on the same host use subsequent ports). The examples below use
  the default ports.

## <a id="clusterstate">GET /admin/cluster</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Returns the state of the cluster: the state and partitions of each Node Controller, the identity of the
  metadata node, the effective values of all common configuration options, and URIs for the other administration
  endpoints.

__Endpoints__

* `GET /admin/cluster` - Full cluster state description
* `GET /admin/cluster/summary` - Brief cluster state summary

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
        "config": {
            "active.memory.global.budget": 67108864,
            "compiler.parallelism": 0,
            "log.level": "WARN",
            ...
        },
        "metadata_node": "asterix_nc1",
        "ncs": [
            {
                "node_id": "asterix_nc1",
                "state": "ACTIVE",
                "partitions": [ { "partition_id": "partition_0", "active": true } ],
                "configUri": "http://localhost:19002/admin/cluster/node/asterix_nc1/config",
                "statsUri": "http://localhost:19002/admin/cluster/node/asterix_nc1/stats",
                "threadDumpUri": "http://localhost:19002/admin/cluster/node/asterix_nc1/threaddump"
            }
        ],
        "state": "ACTIVE",
        "shutdownUri": "http://localhost:19002/admin/shutdown",
        "fullShutdownUri": "http://localhost:19002/admin/shutdown?all=true",
        "versionUri": "http://localhost:19002/admin/version",
        "diagnosticsUri": "http://localhost:19002/admin/diagnostics"
    }

  The possible values of the cluster `state` are `ACTIVE`, `PENDING`, `RECOVERING`, `UNUSABLE`, `REBALANCE_REQUIRED`
  and `SHUTTING_DOWN`. The `state` of an individual node is one of `ACTIVE`, `PARTIALLY_ACTIVE`, `INACTIVE` and
  `FAILED`.

__Command (cluster summary)__

    $ curl -v http://localhost:19002/admin/cluster/summary

__Sample response__

    {
        "metadata_node": "asterix_nc1",
        "partitions": {
            "0": {
                "partitionId": 0,
                "nodeId": "asterix_nc1",
                "activeNodeId": "asterix_nc1",
                "iodeviceNum": 0,
                "active": true,
                "pendingActivation": false
            }
        },
        "state": "ACTIVE"
    }

__Note:__ `/admin/cluster` also accepts `POST /admin/cluster/partition/master` (parameters `partition`, `node`) and
  `POST /admin/cluster/metadataNode` (parameter `node`), which reassign a partition master and the metadata node
  respectively. These are intended for internal use during recovery and failover; use them with care.

## <a id="ccdetails">GET /admin/cluster/cc</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Returns details about the Cluster Controller: its configuration, its JVM runtime statistics, and a
  thread dump of its JVM.

__Endpoints__

* `GET /admin/cluster/cc` - The URIs of the sub-resources below
* `GET /admin/cluster/cc/config` - CC configuration and JVM/OS description
* `GET /admin/cluster/cc/stats` - CC runtime statistics
* `GET /admin/cluster/cc/threaddump` - Thread dump of the CC JVM

  Any other sub-path returns `404 Not Found`.

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
        "configUri": "http://localhost:19002/admin/cluster/cc/config",
        "statsUri": "http://localhost:19002/admin/cluster/cc/stats",
        "threadDumpUri": "http://localhost:19002/admin/cluster/cc/threaddump"
    }

__Command (CC configuration)__

    $ curl -v http://localhost:19002/admin/cluster/cc/config

__Sample response__

  The configuration is reported per section, under a `config` array; the surrounding fields describe the JVM and the
  operating system the CC runs on.

    {
        "os_name": "Linux",
        "arch": "amd64",
        "os_version": "5.15.0",
        "num_processors": 10,
        "vm_name": "OpenJDK 64-Bit Server VM",
        "vm_version": "21.0.10",
        "vm_vendor": "Eclipse Adoptium",
        "classpath": [ ... ],
        "pid": 74119,
        "config": [
            { "section": "cc", "properties": { "address": "127.0.0.1", "api.port": 19002, ... } },
            { "section": "common", "properties": { "active.memory.global.budget": 67108864, ... } }
        ],
        "app.args": [ ... ]
    }

__Command (CC statistics)__

    $ curl -v http://localhost:19002/admin/cluster/cc/stats

__Sample response__

    {
        "date": "Fri Aug 07 16:30:17 IST 2026",
        "heap_init_size": 268435456,
        "heap_used_size": 134036224,
        "heap_committed_size": 176160768,
        "heap_max_size": 4294967296,
        "nonheap_init_size": 7667712,
        "nonheap_used_size": 58202112,
        "nonheap_committed_size": 60686336,
        "nonheap_max_size": -1,
        "thread_count": 67,
        "peak_thread_count": 67,
        "started_thread_count": 67,
        "system_load_average": 2.36,
        "gcs": [
            { "name": "G1 Young Generation", "collection-count": 8, "collection-time": 19 }
        ]
    }

__Command (CC thread dump)__

    $ curl -v http://localhost:19002/admin/cluster/cc/threaddump

__Sample response__

    {
        "date": "Fri Aug 07 16:32:04 IST 2026",
        "threads": [
            {
                "id": 1,
                "name": "main",
                "state": "TIMED_WAITING",
                "blocked_count": 8,
                "stack": [
                    "java.base@21.0.10/java.lang.Thread.sleep0(Native Method)",
                    ...
                ]
            }
        ]
    }

## <a id="nodedetails">GET /admin/cluster/node</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Returns details about the Node Controllers in the cluster. The sub-resources mirror those of the
  <a href="#ccdetails">Cluster Controller Details API</a>, but are scoped to a single node.

__Endpoints__

* `GET /admin/cluster/node` - The `ncs` array of the <a href="#clusterstate">cluster state</a>
* `GET /admin/cluster/node/{node_id}` - The cluster state entry of a single node
* `GET /admin/cluster/node/{node_id}/config` - Configuration and JVM/OS description of the node
* `GET /admin/cluster/node/{node_id}/stats` - Runtime statistics of the node
* `GET /admin/cluster/node/{node_id}/threaddump` - Thread dump of the node's JVM

__Response codes__

* `200 OK` - Details were retrieved
* `404 Not Found` - Unknown node id, or unrecognized sub-path
* `503 Service Unavailable` - The node is known but currently unreachable

__Command (node list)__

    $ curl -v http://localhost:19002/admin/cluster/node

__Sample response__

    {
        "ncs": [
            {
                "node_id": "asterix_nc1",
                "state": "ACTIVE",
                "partitions": [ { "partition_id": "partition_0", "active": true } ],
                "configUri": "http://localhost:19002/admin/cluster/node/asterix_nc1/config",
                "statsUri": "http://localhost:19002/admin/cluster/node/asterix_nc1/stats",
                "threadDumpUri": "http://localhost:19002/admin/cluster/node/asterix_nc1/threaddump"
            }
        ]
    }

__Command (node configuration)__

    $ curl -v http://localhost:19002/admin/cluster/node/asterix_nc1/config

__Sample response__

    > GET /admin/cluster/node/asterix_nc1/config HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    {
        "node_id": "asterix_nc1",
        "os_name": "Linux",
        "arch": "amd64",
        "num_processors": 10,
        "vm_name": "OpenJDK 64-Bit Server VM",
        "vm_version": "21.0.10",
        "classpath": [ ... ],
        "pid": 74120,
        "config": [
            {
                "section": "nc",
                "properties": {
                    "address": "127.0.0.1",
                    "iodevices": "/tmp/asterix_nc1/iodevice",
                    "net.thread.count": 1,
                    ...
                }
            },
            { "section": "common", "properties": { ... } }
        ]
    }

__Command (node statistics)__

    $ curl -v http://localhost:19002/admin/cluster/node/asterix_nc1/stats

__Sample response__

    > GET /admin/cluster/node/asterix_nc1/stats HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    {
        "node_id": "asterix_nc1",
        "date": 1786100442672,
        "heartbeat_time": 1786100439438,
        "heap_init_size": 268435456,
        "heap_used_size": 277905384,
        "heap_committed_size": 419430400,
        "heap_max_size": 8589934592,
        "application_memory_budget": 4294967296,
        "application_cpu_core_budget": 10,
        "thread_count": 24,
        "peak_thread_count": 24,
        "system_load_average": 2.16,
        "net_payload_bytes_read": 0,
        "net_payload_bytes_written": 0,
        "ipc_messages_sent": 0,
        "disk_read": -1,
        "disk_write": -1,
        "gcs": [
            { "name": "G1 Young Generation", "collection_count": 3, "collection_time": 36 }
        ]
    }

  The statistics are sampled periodically; each value reported here is the most recent sample.

## <a id="diagnostics">GET /admin/diagnostics</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Collects the configuration, statistics and thread dump of the Cluster Controller and of every Node
  Controller into a single response. This is the most convenient endpoint to capture when reporting a problem, as it
  aggregates in one request what would otherwise require one request per node per resource.

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
            "config": { ... },
            "stats": { ... },
            "threaddump": { ... }
        },
        "ncs": [
            {
                "node_id": "asterix_nc1",
                "config": { ... },
                "stats": { ... },
                "threaddump": { ... }
            }
        ],
        "date": "Fri Aug 07 16:30:17 IST 2026"
    }

  Each of the three resources is gathered independently; if one of them fails, its value is replaced by a string
  describing the failure and the rest of the response is still returned.

__Response codes__

* `200 OK` - Diagnostics were collected
* `404 Not Found` - A sub-path was given; this endpoint accepts no sub-path
* `503 Service Unavailable` - The cluster is shutting down, or a node's detail could not be obtained

## <a id="runningrequests">GET /admin/requests/running</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Returns the requests that are currently running in the cluster, and cancels a running request when
  used with the `DELETE` method.

__Parameters (GET)__

* `redact` - If `true`, statements, plans and error messages are redacted so that user data is not disclosed.
  (Optional, default: `false`)

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
            "uuid": "d5abc843-b747-4fa5-89f0-b647f866f9ea",
            "requestTime": "2026-08-07T11:01:09.763",
            "elapsedTime": 5.326,
            "state": "running",
            "cancellable": true,
            "statement": "SELECT COUNT(*) FROM range(1,2000000000) r;",
            "clientContextID": "demo-ctx",
            "node": "127.0.0.1:19002",
            "remoteAddr": "127.0.0.1:19002",
            "userAgent": "curl/7.43.0",
            "jobId": "JID:0.8",
            "jobStatus": "RUNNING",
            "jobCreateTime": "2026-08-07T11:01:09.807",
            "jobStartTime": "2026-08-07T11:01:09.807",
            "jobQueueTime": 0,
            "jobRequiredCPUs": 1,
            "jobRequiredMemory": 240844
        }
    ]

  `elapsedTime` is reported in seconds. The `jobXXX` fields are only present once a Hyracks job has been created for
  the request.

__Command (cancel a running request)__

    $ curl -X DELETE "http://localhost:19002/admin/requests/running?request_id=d5abc843-b747-4fa5-89f0-b647f866f9ea"

    $ curl -X DELETE "http://localhost:19002/admin/requests/running?client_context_id=demo-ctx"

__Parameters (DELETE)__

  Exactly one of the following is required. `request_id` corresponds to the `requestID` returned by the
  <a href="#queryservice">/query/service</a> endpoint, and `client_context_id` to its `client_context_id` parameter.
  If both are given, `request_id` takes precedence.

* `request_id` - The UUID of the request to cancel
* `client_context_id` - The client context id of the request to cancel

__Response codes (DELETE)__

* `200 OK` - The request was cancelled
* `400 Bad Request` - Neither `request_id` nor `client_context_id` was given
* `403 Forbidden` - The request is not cancellable
* `404 Not Found` - No running request matches the given id

## <a id="activestats">GET /admin/active</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Returns the statistics of every active entity in the cluster, keyed by entity id in the form
  `dataverse.entity(extension)`. Active entities are long-running data ingestion processes, i.e. feeds. Entities that
  are not currently active are omitted, so the response is an empty object when nothing is running.

  Statistics are cached. If the cached statistics of an entity are older than the cache expiry, they are refreshed
  before being returned.

__Parameters__

* The path suffix is an optional cache expiry in milliseconds, e.g. `/admin/active/5000`. (Optional, default: `2000`)

__Command__

    $ curl -v http://localhost:19002/admin/active

__Sample response__

    > GET /admin/active HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    {
        "TinySocial.TwitterFeed(Feed)": {
            "Stats": [
                { "intake-partition": 0, "incoming-records-count": 50000, ... }
            ]
        }
    }

  The contents of each `Stats` array element are entity-specific, with one element per runtime partition. An entity
  whose statistics have not been collected yet reports `{ "Stats": "N/A" }`.

__Command (with an explicit cache expiry)__

    $ curl -v http://localhost:19002/admin/active/5000

__Sample response (no active entities)__

    { }

## <a id="version">GET /admin/version</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Returns the build properties of the running AsterixDB instance. The properties are those recorded at
  build time from the source repository, so all keys carry a `git.` prefix.

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
        "git.build.version": "0.9.10-SNAPSHOT",
        "git.branch": "master",
        "git.commit.id": "f15fd8cada3479ec219941edb35fc0019ba0dd6c",
        "git.commit.id.abbrev": "f15fd8c",
        "git.commit.time": "2026-06-30T14:22:01+0530",
        "git.build.time": "2026-07-15T13:21:21+0530",
        "git.dirty": "false",
        "git.closest.tag.name": "apache-hyracks-0.3.8.2",
        ...
    }

__Note:__ the response body is JSON, but is served with a `text/plain` content type.

## <a id="plancache">DELETE /admin/plancache</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Clears all entries in the query plan cache.
The response is a JSON object that describes the number of entries that were cleared from the query plan cache.

__Command__

    $ curl -X DELETE -v http://localhost:19002/admin/plancache

__Sample response__

    > DELETE /admin/plancache HTTP/1.1
    > Host: localhost:19002
    > User-Agent: curl/8.4.0
    > Accept: */*
    >
    < HTTP/1.1 200 OK
    < connection: keep-alive
    < content-length: 58
    <
    {"status":"2 query plan cache entries have been cleared."}

## <a id="rebalance">POST /admin/rebalance</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Redistributes the data of one or more datasets onto a given set of nodes. This is how a dataset is
  spread over nodes that were added to the cluster after it was created, or moved off nodes that are being retired.

  Rebalance requests are executed one at a time, in submission order, and the response is only sent once the request
  has completed. When a whole dataverse or the whole cluster is rebalanced, views and the datasets of the `Metadata`
  dataverse are skipped; naming a dataset of the `Metadata` dataverse explicitly is rejected. A dataset that is
  dropped while its rebalance is queued is silently skipped.

__Parameters__

* `targetNode` - A node to place the data on. Required, and may be repeated to specify several nodes, e.g.
  `targetNode=asterix_nc1&targetNode=asterix_nc2`.
* `dataverseName` - The dataverse to rebalance. (Optional) May be repeated to address a multi-part dataverse name.
* `datasetName` - The dataset to rebalance. (Optional) Requires `dataverseName` to be given as well.
* `force` - If `false`, a dataset that already resides exactly on the target nodes is left untouched.
  (Optional, default: `true`)

__Scope__

* If neither `dataverseName` nor `datasetName` is given, all non-metadata datasets are rebalanced.
* If only `dataverseName` is given, all datasets of that dataverse are rebalanced.
* If both are given, only that dataset is rebalanced.

__Command (rebalance one dataset)__

    $ curl -X POST "http://localhost:19002/admin/rebalance?dataverseName=TinySocial&datasetName=FacebookUsers\
                    &targetNode=asterix_nc1&targetNode=asterix_nc2"

__Command (rebalance a whole dataverse)__

    $ curl -X POST "http://localhost:19002/admin/rebalance?dataverseName=TinySocial&targetNode=asterix_nc1"

__Sample response__

    > POST /admin/rebalance HTTP/1.1
    > Host: localhost:19002
    >
    < HTTP/1.1 200 OK
    < content-type: application/json; charset=utf-8
    <
    { "results": "successful" }

__Verifying a rebalance__

  Use the [Storage API](#storage) on each node controller to see where a dataset now resides. The path reported by
  `GET /admin/storage/stats` ends in a rebalance counter, which is incremented every time the dataset is rebalanced:

    $ curl -s http://localhost:19004/admin/storage/stats   # before
    [ ..., { "dataset": "FacebookUsers", "partition": 0,
             "path": "storage/partition_0/TinySocial/FacebookUsers/0/FacebookUsers", ... } ]

    $ curl -s http://localhost:19004/admin/storage/stats   # after rebalancing onto this node
    [ ..., { "dataset": "FacebookUsers", "partition": 0,
             "path": "storage/partition_0/TinySocial/FacebookUsers/1/FacebookUsers", ... } ]

  A node that no longer holds the dataset stops reporting it here. Note that inspecting the io devices on disk is not a
  reliable check: the directory of the previous rebalance counter is left behind empty, so a node can still show a
  directory for a dataset whose data has moved away.

__Error responses__

  Failures are reported in the same `results` field, with a non-`200` status:

    < HTTP/1.1 400 Bad Request
    { "results": "at least one targetNode must be specified" }

    < HTTP/1.1 400 Bad Request
    { "results": "to rebalance a particular dataset, the parameter dataverseName must be given" }

    < HTTP/1.1 400 Bad Request
    { "results": "cannot rebalance a metadata dataset" }

__Cancelling a rebalance__

  A `DELETE` cancels all rebalance tasks that are pending or in progress:

    $ curl -X DELETE http://localhost:19002/admin/rebalance

    { "results": "rebalance tasks are cancelled" }

## <a id="shutdown">POST /admin/shutdown</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Shuts the cluster down gracefully. The Cluster Controller stops accepting new requests as soon as the
  response has been written, and the Node Controllers are then stopped in an orderly fashion.

__Parameters__

* `all` - If `true`, the NCService processes that supervise the Node Controllers are terminated as well, so that the
  nodes are not restarted. (Optional, default: `false`)

__Command (stop the cluster)__

    $ curl -X POST http://localhost:19002/admin/shutdown

__Command (stop the cluster and its NCService processes)__

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
        "date": "Fri Aug 07 16:32:43 IST 2026",
        "cluster": {
            "metadata_node": "asterix_nc1",
            "ncs": [
                {
                    "node_id": "asterix_nc1",
                    "state": "ACTIVE",
                    "partitions": [ { "partition_id": "partition_0", "active": true } ],
                    "pid": 74120
                }
            ],
            "state": "ACTIVE"
        }
    }

__Note:__ the response is sent with status `202 Accepted` before the shutdown completes, and reports the state of the
  cluster as it was when the request arrived. The `pid` of each node, and its `ncservice_pid` where available, are
  included so that a caller can verify afterwards that the processes have exited.

## <a id="storage">GET /admin/storage</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Reports the storage partitions hosted by a Node Controller, the replicas of each partition, and the
  on-disk size of each index.

__Note:__ This is an NC-level endpoint, served on the `nc.api.port` (default `19004`), not on the CC.

__Endpoints__

* `GET /admin/storage` - All partitions of this node and their replicas
* `GET /admin/storage/partition/{partition_id}` - A single partition and its replicas
* `GET /admin/storage/stats` - Size of every index resource stored on this node

  Any other sub-path returns `404 Not Found`.

__Command (list partitions)__

    $ curl -v http://localhost:19004/admin/storage

__Sample response__

    > GET /admin/storage HTTP/1.1
    > Host: localhost:19004
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
                    "nodeId": "asterix_nc1"
                }
            ]
        }
    ]

  The `replicas` array is empty when replication is not enabled. A replica `status` is one of `IN_SYNC`,
  `CATCHING_UP` and `DISCONNECTED`.

__Command (storage statistics)__

    $ curl -v http://localhost:19004/admin/storage/stats

__Sample response__

    [
        {
            "index": "FacebookUsers",
            "dataset": "FacebookUsers",
            "datasetId": 101,
            "partition": 0,
            "path": "storage/partition_0/TinySocial/FacebookUsers/0/FacebookUsers",
            "totalSize": 528313,
            "components": [ { "id": "0_22", "size": 524320 } ]
        }
    ]

__Replica management__

  The endpoint also accepts `POST` requests that manage replicas. These are used internally by the replication
  protocol and are not normally invoked by hand:

* `POST /admin/storage/addReplica` - Add a replica (parameters: `partition`, `host`, `port`, `nodeId`)
* `POST /admin/storage/removeReplica` - Remove a replica (parameters: `partition`, `host`, `port`, `nodeId`)
* `POST /admin/storage/promote` - Promote a partition on this node to master (parameter: `partition`)
* `POST /admin/storage/release` - Release a partition held by this node (parameter: `partition`)

  Each returns `200 OK` on success and `400 Bad Request` if a required parameter is missing.

## <a id="netdiagnostics">GET /admin/net</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Returns the state of a Node Controller's network multiplexer: the address it listens on, and the
  channels of each inbound and outbound connection to its peers. This is useful when diagnosing stalled jobs, which
  often show up here as connections with outstanding channel state.

__Note:__ This is an NC-level endpoint, served on the `nc.api.port` (default `19004`), not on the CC.

__Command__

    $ curl -v http://localhost:19004/admin/net

__Sample response__

    > GET /admin/net HTTP/1.1
    > Host: localhost:19004
    > User-Agent: curl/7.43.0
    > Accept: */*
    >
    < HTTP/1.1 200 OK
    < transfer-encoding: chunked
    < connection: keep-alive
    < content-type: application/json; charset=utf-8
    <
    {
        "localAddress": "/127.0.0.1:59521",
        "incomingConnections": [
            {
                "remoteAddress": "/127.0.0.1:59530",
                "channels": [ ... ]
            }
        ],
        "outgoingConnections": []
    }

  A connection that has not been established yet is omitted from both arrays; on an idle cluster both are empty.

## <a id="udf">POST /admin/udf</a><span style="font-size: medium"> <a href="#toc">[Back to TOC]</a></span>

__Description__ Installs, replaces and removes external UDF libraries. See the
  [User-defined Functions](udf.html) documentation for how to build a library and declare functions from it.

__Note:__ This is an NC-level endpoint, served on the `nc.api.port` (default `19004`), not on the CC. It only accepts
  requests originating from the loopback interface, and requires HTTP basic authentication against the credentials
  named by the `credential.file` setting. In a cloud deployment the endpoint is served over a Unix domain socket
  instead, at the path given by the `udf.api.ds.path` setting.

__Endpoints__

* `POST /admin/udf/{dataverse}/{library}` - Install or replace a library
* `DELETE /admin/udf/{dataverse}/{library}` - Remove a library

  Both path segments are URL-encoded; a multi-part dataverse name is given in its canonical dotted form. Any other
  number of path segments is rejected.

__Request body (POST)__

  The body is a `multipart/form-data` request with two parts:

* `type` - The implementation language of the library: `java` or `python`
* `data` - The library archive itself, uploaded as a file: for Java a `.zip` containing the library jar and any
  dependency jars under `lib/`, for Python a `.pyz`. Note that a Java library is uploaded as the zip archive around
  the jar, not as the jar itself.

__Command (install a Java library)__

    $ curl -v -u admin:admin -X POST         \
           -F 'data=@./lib.zip'              \
           -F 'type=java'                    \
           http://localhost:19004/admin/udf/udfs/testlib

__Command (install a Python library)__

    $ curl -v -u admin:admin -X POST         \
           -F 'data=@./lib.pyz'              \
           -F 'type=python'                  \
           http://localhost:19004/admin/udf/udfs/pylib

__Command (remove a library)__

    $ curl -v -u admin:admin -X DELETE http://localhost:19004/admin/udf/udfs/testlib

  A library can only be removed once every function declared from it has been dropped. Dropping the dataverse drops
  its libraries as well.

__Sample response__

    < HTTP/1.1 200 OK
    <
    {}

__Response codes__

* `200 OK` - The library was installed, replaced or removed
* `400 Bad Request` - The `type` or `data` part is missing, the language is not supported, or the request path does
  not name both a dataverse and a library
* `401 Unauthorized` - Credentials were missing or incorrect
* `403 Forbidden` - The request did not originate from the loopback interface
