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

* [Quick Start](#quickstart)
* [Starting a small single-machine cluster using the NCService](#Small_cluster)
* [Deploying AsterixDB via NCService in a multi-machine setup](#Multi_machine)
* [Available Configuration Parameters](#Parameters)


#  <a id="quickstart">Quick Start</a>

The fastest way to get set up with a single-machine sample instance of AsterixDB is
to use the included sample helper scripts. To do so, in the extracted `asterix-server`
directory, navigate to `opt/local/bin/`

    user@localhost:~/
    $cd asterix-server/
    user@localhost:~/asterix-server
    $cd opt/local/bin

This folder should contain 4 scripts, two pairs of `.sh` and `.bat` files
respectively. `start-sample-cluster.sh` will simply start a basic sample cluster
using the coniguration files located in `samples/local/conf/`.

    user@localhost:~/a/o/l/bin
    $./start-sample-cluster.sh
    CLUSTERDIR=/home/user/asterix-server/opt/local
    INSTALLDIR=/home/user/asterix-server
    LOGSDIR=/home/user/asterix-server/samples/opt/logs

    INFO: Starting sample cluster...
    INFO: Waiting up to 30 seconds for cluster 127.0.0.1:19002 to be available.
    INFO: Cluster started and is ACTIVE.
    user@localhost:~/a/o/l/bin
    $

Now, there should be a running AsterixDB cluster on the machine. To go to the
Web Interface, visit [http://localhost:19001](http://localhost:19001)

<div class="source">
<pre>
<img src="images/asterixdb_interface.png" alt="The AsterixDB Web Interface"/>
<em>Fig. 1</em>: The AsterixDB Web Interface
</pre>
</div>


#  <a id="Small_cluster">Starting a small single-machine cluster using NCService</a>

The above cluster was started using a script, but below is a description in detail
of how precisely this was achieved. The config files here are analagous to the
ones within `samples/local/conf`.

When running a cluster using the `NCService` there are 3 different kinds of
processes involved:

- `NCDriver`, also known as the Node Controller or NC for short. This is the
  process that does the actual work of queries and data management within the
  AsterixDB cluster
- `NCService` configures and starts the `NCDriver` process. It is a simple
  daemon whose only purpose is to wait for the `CCDriver` process to call
  upon it to initiate cluster bootup.
- `CCDriver`, which is the Cluster Controller process, also known as the CC.
  This process manages the distribution of tasks to all NCs, as well as providing
  the parameters of each NC to the NCService upon cluster startup. It also hosts
  the Web interface and query compiler and optimizer.

The cluster startup follows a particular sequence, which is as follows:

0. Each host on which an NC is desired and is mentioned in the configuration,
   the `NCService` daemon is started first. It will listen and wait for the CC
   to contact it.
1. The one host on which the CC is to be placed is started with an appropriate
   configuration file.
2. The CC contacts all `NCService` daemons and the `NCService` subsequently starts
   and `NCDriver` process with the configration supplied by the `CC`
3. Each `NCDriver` then contacts the CC to register itself as started

This process is briefly illustrated in the diagram below:

<div class="source">
<pre>
<img src="images/ncservice.png" alt="The AsterixDB Web Interface"/>
<em>Fig. 2</em>: NCService startup sequence
</pre>
</div>

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

1. Change directory into the asterix-server binary folder

    ```
    user@localhost:~/
    $cd asterix-server/
    user@localhost:~/asterix-server
    $cd samples/local/bin
    ```

2. Start the 2 `NCServices` for `red` and `blue`.

    ```
    user@localhost:~/asterix-server
    $bin/asterixncservice -config-file blue.conf > blue-service.log 2>&1 &
    user@localhost:~/asterix-server
    $bin/asterixncservice >red-service.log 2>&1 &
    ```

3. Start the `CCDriver`.

    ```
    user@localhost:~/asterix-server
    $bin/asterixcc -config-file cc.conf > cc.log 2>&1 &
    ```

The `CCDriver` will connect to the `NCServices` and thus initiate the
configuration and the start of the `NCDrivers`.
After running these scripts, `jps` should show a result similar to this:

    user@localhost:~/asterix-server
    $jps
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

#  <a id="Multi_machine">Deploying AsterixDB via NCService in a multi-machine setup</a>

Deploying on multiple machines only differs in the configuration file and where each process
is actually resident. Take for example a deployment on 3 machines, `cacofonix-1`,`cacofonix-2`,and `cacofonix-3`.
`cacofonix-1` will be the CC, and `cacofonix-2` and `cacofonix-3` will be the two NCs, respectively.
The configuration would be as follows:

`cc.conf`:

    [nc/red]
    txnlogdir=/lv_scratch/asterix/red/txnlog
    coredumpdir=/lv_scratch/asterix/red/coredump
    iodevices=/lv_scratch/asterix/red
    address=cacofonix-2

    [nc/blue]
    txnlogdir=/lv_scratch/asterix/blue/txnlog
    coredumpdir=/lv_scratch/asterix/blue/coredump
    iodevices=/lv_scratch/asterix/blue
    address=cacofonix-3

    [nc]
    app.class=org.apache.asterix.hyracks.bootstrap.NCApplicationEntryPoint
    storagedir=storage
    command=asterixnc

    [cc]
    cluster.address = cacofonix-1

To deploy, first the `asterix-server` binary must be present on each machine. Any method to transfer
the archive to each machine will work, but here `scp` will be used for simplicity's sake.

    user@localhost:~
    $for f in {1,2,3}; scp asterix-server.zip cacofonix-$f:~/; end

Then unzip the binary on each machine. First, start the `NCService` processes on each of the slave
machines. Any way of getting a shell on the machine is fine, be it physical or via `ssh`.

    user@cacofonix-2 12:41:42 ~/asterix-server/
    $ bin/asterixncservice > red-service.log 2>&1 &


    user@cacofonix-3 12:41:42 ~/asterix-server/
    $ bin/asterixncservice > blue-service.log 2>&1 &

Now that each `NCService` is waiting, the CC can be started.

    user@cacofonix-1 12:41:42 ~/asterix-server/
    $ bin/asterixcc -config-file cc.conf > cc.log 2>&1 &


The cluster should now be started and the Web UI available on the CC host at the default port.

# <a id="Parameters">Available Configuration Parameters</a>

The following parameters are for the master process, under the "[cc]" section.

| Parameter | Meaning |  Default |
|----------|--------|-------|
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
| storage.buffercache.maxopenfiles | The maximum number of open files for the buffer cache.  Note that this is the parameter for the AsterixDB and setting the operating system parameter is still required. | 2147483647 |
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
| compiler.framesize |  The page size (in bytes) for computation  | 32768 |
| compiler.groupmemory |  The memory budget (in bytes) for a group by operator instance in a partition | 33554432 |
| compiler.joinmemory | The memory budget (in bytes) for a join operator instance in a partition  | 33554432 |
| compiler.sortmemory | The memory budget (in bytes) for a sort operator instance in a partition | 33554432 |
| compiler.parallelism | The degree of parallelism for query execution. Zero means to use the storage parallelism as the query execution parallelism, while other integer values dictate the number of query execution parallel partitions. The system will fall back to use the number of all available CPU cores in the cluster as the degree of parallelism if the number set by a user is too large or too small.  | 0 |
