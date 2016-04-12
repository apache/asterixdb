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
#AsterixDB

AsterixDB is a BDMS (Big Data Management System) with a rich feature set that sets it apart from other Big Data platforms.  Its feature set makes it well-suited to modern needs such as web data warehousing and social data storage and analysis. AsterixDB has:

 * A semistructured NoSQL style data model (ADM) resulting from extending JSON with object database ideas
 * An expressive and declarative query language (AQL) that supports a broad range of queries and analysis over semistructured data
 * A parallel runtime query execution engine, Hyracks, that has been scale-tested on up to 1000+ cores and 500+ disks
 * Partitioned LSM-based data storage and indexing to support efficient ingestion and management of semistructured data
 * Support for query access to externally stored data (e.g., data in HDFS) as well as to data stored natively by AsterixDB
 * A rich set of primitive data types, including spatial and temporal data in addition to integer, floating point, and textual data
 * Secondary indexing options that include B+ trees, R trees, and inverted keyword (exact and fuzzy) index types
 * Support for fuzzy and spatial queries as well as for more traditional parametric queries
 * Basic transactional (concurrency and recovery) capabilities akin to those of a NoSQL store

Learn more about AsterixDB at [http://asterixdb.incubator.apache.org] (http://asterixdb.incubator.apache.org)


##Building AsterixDB

To build AsterixDB from source, you should have a platform with the following:

* A Unix-ish environment (Linux, OS X, will all do).
* git
* Maven 3.1.1 or newer.
* Java 8 or newer.

Additionally to run all the integration tests you should be running `sshd` locally, and have passwordless ssh logins enabled for the account which is running the tests.

Instructions for building the master:

* Checkout AsterixDB master:

        $git clone https://github.com/apache/incubator-asterixdb.git

* Build AsterixDB master:

        $cd incubator-asterixdb
        $mvn clean package -DskipTests


##Running AsterixDB (on your machine from your build)
Here are steps to get AsterixDB running on your machine:

* Create a directory as the home for AsterixDB installer program, i.e., managix:

        $mkdir ~/managix

* Copy AsterixDB binary artifact into the installer directory and unzip it:

        $cp asterixdb/asterix-installer/target/asterix-installer-*-binary-assembly.zip ~/managix/
        $cd ~/managix
        $unzip asterix-installer-*-binary-assembly.zip

* Configure the installer:

        $bin/managix configure

* Validate if the computer environment is suitable:

        $bin/managix validate

* Create and start your instance:

        $bin/managix create -n test -c clusters/local/local.xml

* Good to go and run queries in your browser at:

        http://localhost:19001

* Read more documentations to learn the data model, query language, and how to create a cluster instance:
  [https://ci.apache.org/projects/asterixdb/index.html] (https://ci.apache.org/projects/asterixdb/index.html)

##Documentation

AsterixDB's official documentation resides at [https://ci.apache.org/projects/asterixdb/index.html] (https://ci.apache.org/projects/asterixdb/index.html). This is built from the maven project under `asterix-doc/` as a maven site. The documentation on the official website refers to the most stable build version, so for pre-release versions one should refer to the compiled documentation.

##Support/Contact

If you have any questions, please feel free to ask on our mailing list, [users@asterixdb.incubator.apache.org](mailto:users@asterixdb.incubator.apache.org). Join the list by sending an email to [users-subscribe@asterixdb.incubator.apache.org](mailto:users-subscribe@asterixdb.incubator.apache.org). If you are interested in the internals or developement of AsterixDB, also please feel free to subscribe to our developer mailing list, [dev@asterixdb.incubator.apache.org](mailto:dev@asterixdb.incubator.apache.org), by sending an email to [dev-subscribe@asterixdb.incubator.apache.org](mailto:dev-subscribe@asterixdb.incubator.apache.org).

