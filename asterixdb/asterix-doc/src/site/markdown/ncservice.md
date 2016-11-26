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

## <a id="toc">Table of Contents</a> ##

* [Starting a small cluster using the NCService](#Small_cluster)
* [Parameter setting](#Parameters)

#  <a id="Small_cluster">Starting a small cluster using the NCService</a>

When running a cluster using the `NCService` there are 3 different kind of
processes involved:

1. `NCDriver` does the work of a NodeController
2. `NCService` configures and starts an `NCDriver`
3. `CCDriver` does the work of a ClusterController and sends the
    configuration to the `NCServices`

To start a small cluster consisting of 2 NodeControllers (`red` and `blue`)
and 1 ClusterController (`cc`) on a single machine only 2 configuration
files are required.
The first one is

`blue.conf`:

    [ncservice]
    port=9091

It is a configuration file for the second `NCService`.
This contains only the port that the `NCService` of the second
NodeControllers listens to as it is non-standard.
The first `NCService` does not need a configuration file, as it only uses
default parameters.
In a distributed environment with 1 NodeController per machine, no
`NCService` needs a configuration file.

The second configuration file is

`cc.conf`:

    [nc/red]
    txnlogdir=/tmp/asterix/red/txnlog
    coredumpdir=/tmp/asterix/red/coredump
    iodevices=/tmp/asterix/red

    [nc/blue]
    port=9091
    txnlogdir=/tmp/asterix/blue/txnlog
    coredumpdir=/tmp/asterix/blue/coredump
    iodevices=/tmp/asterix/blue

    [nc]
    app.class=org.apache.asterix.hyracks.bootstrap.NCApplicationEntryPoint
    storagedir=storage
    address=127.0.0.1
    command=asterixnc

    [cc]
    cluster.address = 127.0.0.1
    http.port = 12345

This is the configuration file for the cluster and it contains information
that each `NCService` will use when starting the corresponding `NCDriver` as
well as information for the `CCDriver`.

To start the cluster simply use the following steps

1. Set BASEDIR to location of an unzipped asterix-server binary assembly (in
the source tree that's at `asterixdb/asterix-server/target`).

        $ export BASEDIR=[..]/asterix-server-0.8.9-SNAPSHOT-binary-assembly

2. Start the 2 `NCServices` for `red` and `blue`.

        $ $BASEDIR/bin/asterixncservice -config-file blue.conf > blue-service.log 2>&1 &
        $ $BASEDIR/bin/asterixncservice >red-service.log 2>&1 &

3. Start the `CCDriver`.

        $ $BASEDIR/bin/asterixcc -config-file cc.conf > cc.log 2>&1 &

The `CCDriver` will connect to the `NCServices` and thus initiate the
configuration and the start of the `NCDrivers`.
After running these scripts, `jps` should show a result similar to this:

    $ jps
    13184 NCService
    13200 NCDriver
    13185 NCService
    13186 CCDriver
    13533 Jps
    13198 NCDriver

The logs for the `NCDrivers` will be in `$BASEDIR/logs`.

To stop the cluster again simply run

    $ kill `jps | egrep '(CDriver|NCService)' | awk '{print $1}'`

to kill all processes.

# <a id="Parameters">Parameter settings</a>

The following parameters are for the master process, under the "[cc]" section.

| Parameter | Meaning |  Default |
|----------|--------|-------|
| compiler.framesize |  The page size (in bytes) for computation  | 32768 |
| compiler.groupmemory |  The memory budget (in bytes) for a group by operator instance in a partition | 33554432 |
| compiler.joinmemory | The memory budget (in bytes) for a join operator instance in a partition  | 33554432 |
| compiler.sortmemory | The memory budget (in bytes) for a sort operator instance in a partition | 33554432 |
| instance.name  |  The name of the AsterixDB instance   | "DEFAULT_INSTANCE" |
| max.wait.active.cluster | The max pending time (in seconds) for cluster startup. After the threshold, if the cluster still is not up and running, it is considered unavailable.    | 60 |
| metadata.callback.port | The port for metadata communication | 0 |
| cluster.address | The binding IP address for the AsterixDB instance | N/A |

The following parameters for slave processes, under "[nc]" sections.

| Parameter | Meaning |  Default |
|----------|--------|-------|
| address | The binding IP address for the slave process |  N/A   |
| command | The command for the slave process | N/A (for AsterixDB, it should be "asterixnc") |
| coredumpdir | The path for core dump | N/A |
| iodevices | Comma separated directory paths for both storage files and temporary files | N/A |
| jvm.args | The JVM arguments | -Xmx1536m |
| metadata.port | The metadata communication port on the metadata node. This parameter should only be present in the section of the metadata NC | 0 |
| metadata.registration.timeout.secs | The time out threshold (in seconds) for metadata node registration | 60 |
| port | The port for the NCService that starts the slave process |  N/A |
| storagedir | The directory for storage files  |  N/A |
| storage.buffercache.maxopenfiles | The maximum number of open files for the buffer cache.  Note that this is the parameter for the AsterixDB <br/> and setting the operating system parameter is still required. | 2147483647 |
| storage.buffercache.pagesize |  The page size (in bytes) for the disk buffer cache (for reads) | 131072 |
| storage.buffercache.size | The overall budget (in bytes) of the disk buffer cache (for reads) | 536870912 |
| storage.lsm.bloomfilter.falsepositiverate | The false positive rate for the bloom filter for each memory/disk components | 0.01 |
| storage.memorycomponent.globalbudget | The global budget (in bytes) for all memory components of all datasets and indexes (for writes) |  536870912 |
| storage.memorycomponent.numcomponents | The number of memory components per data partition per index  | 2 |
| storage.memorycomponent.numpages | The number of pages for all memory components of a dataset, including those for secondary indexes | 256 |
| storage.memorycomponent.pagesize | The page size (in bytes) of memory components | 131072 |
| storage.metadata.memorycomponent.numpages | The number of pages for all memory components of a metadata dataset | 256 |
| txnlogdir  | The directory for transaction logs | N/A |
| txn.commitprofiler.reportinterval |  The interval for reporting commit statistics | 5 |
| txn.job.recovery.memorysize  | The memory budget (in bytes) used for recovery | 67108864 |
| txn.lock.timeout.sweepthreshold | Interval (in milliseconds) for checking lock timeout | 10000 |
| txn.lock.timeout.waitthreshold | Time out (in milliseconds) of waiting for a lock | 60000 |
| txn.log.buffer.numpages | The number of pages in the transaction log tail | 8 |
| txn.log.buffer.pagesize | The page size (in bytes) for transaction log buffer. | 131072 |
| txn.log.checkpoint.history |  The number of checkpoints to keep in the transaction log | 0 |
| txn.log.checkpoint.lsnthreshold | The checkpoint threshold (in terms of LSNs (log sequence numbers) that have been written to the transaction log, i.e., the length of the transaction log) for transection logs | 67108864 |


The following parameter is for both master and slave processes, under the "[app]" section.

| Parameter | Meaning |  Default |
|----------|--------|-------|
| log.level | The logging level for master and slave processes | "INFO" |
