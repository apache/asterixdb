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
<a href="http://asterixdb.apache.org"><img src="http://asterixdb.apache.org/img/asterixdb_tm.png" height=100></img></a>

## What is AsterixDB?

AsterixDB is a BDMS (Big Data Management System) with a rich feature set that sets it apart from other Big Data platforms.  Its feature set makes it well-suited to modern needs such as web data warehousing and social data storage and analysis. AsterixDB has:

- __Data model__<br/>
A semistructured NoSQL style data model ([ADM](https://ci.apache.org/projects/asterixdb/datamodel.html)) resulting from
extending JSON with object database ideas

- __Query languages__<br/>
Two expressive and declarative query languages ([SQL++](http://asterixdb.apache.org/docs/0.9.1/sqlpp/manual.html)
and [AQL](http://asterixdb.apache.org/docs/0.9.1/aql/manual.html)) that support a broad range of queries and analysis
over semistructured data

- __Scalability__<br/>
A parallel runtime query execution engine, Apache Hyracks, that has been scale-tested on up to 1000+ cores and 500+ disks

- __Native storage__<br/>
Partitioned LSM-based data storage and indexing to support efficient ingestion and management of semistructured data

- __External storage__<br/>
Support for query access to externally stored data (e.g., data in HDFS) as well as to data stored natively by AsterixDB

- __Data types__<br/>
A rich set of primitive data types, including spatial and temporal data in addition to integer, floating point, and textual data

- __Indexing__<br/>
Secondary indexing options that include B+ trees, R trees, and inverted keyword (exact and fuzzy) index types

- __Transactions__<br/>
Basic transactional (concurrency and recovery) capabilities akin to those of a NoSQL store

Learn more about AsterixDB at its [website](http://asterixdb.apache.org).


## Build from source

To build AsterixDB from source, you should have a platform with the following:

* A Unix-ish environment (Linux, OS X, will all do).
* git
* Maven 3.3.9 or newer.
* Oracle JDK 8 or newer.

Instructions for building the master:

* Checkout AsterixDB master:

        $git clone https://github.com/apache/asterixdb.git

* Build AsterixDB master:

        $cd asterixdb
        $mvn clean package -DskipTests


## Run the build on your machine
Here are steps to get AsterixDB running on your local machine:

* Start a single-machine AsterixDB instance:

        $cd asterixdb/asterix-server/target/asterix-server-*-binary-assembly/apache-asterixdb-*-SNAPSHOT
        $./opt/local/bin/start-sample-cluster.sh

* Good to go and run queries in your browser at:

        http://localhost:19001

* Read more [documentation](https://ci.apache.org/projects/asterixdb/index.html) to learn the data model, query language, and how to create a cluster instance.

## Documentation

* [master](https://ci.apache.org/projects/asterixdb/index.html) |
  [0.9.3](http://asterixdb.apache.org/docs/0.9.3/index.html) |
  [0.9.2](http://asterixdb.apache.org/docs/0.9.2/index.html) |
  [0.9.1](http://asterixdb.apache.org/docs/0.9.1/index.html) |
  [0.9.0](http://asterixdb.apache.org/docs/0.9.0/index.html)

## Community support

- __Users__</br>
maling list: [users@asterixdb.apache.org](mailto:users@asterixdb.apache.org)</br>
Join the list by sending an email to [users-subscribe@asterixdb.apache.org](mailto:users-subscribe@asterixdb.apache.org)</br>
- __Developers and contributors__</br>
mailing list:[dev@asterixdb.apache.org](mailto:dev@asterixdb.apache.org)</br>
Join the list by sending an email to [dev-subscribe@asterixdb.apache.org](mailto:dev-subscribe@asterixdb.apache.org)

