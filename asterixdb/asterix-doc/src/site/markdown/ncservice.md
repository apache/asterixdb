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

# Starting a small cluster using the NCService

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