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

# AsterixDB #

AsterixDB is a BDMS (Big Data Management System) with a rich feature set that
sets it apart from other Big Data platforms.
Its feature set makes it well-suited to modern needs such as web data
warehousing and social data storage and analysis. AsterixDB has:

 * A semistructured NoSQL style data model (ADM) resulting from extending JSON
   with object database ideas
 * Two expressive and declarative query languages (SQL++ and AQL) that support a broad
   range of queries and analysis over semistructured data
 * A parallel runtime query execution engine, Apache Hyracks, that has been
   scale-tested on up to 1000+ cores and 500+ disks
 * Partitioned LSM-based data storage and indexing to support efficient
   ingestion and management of semistructured data
 * Support for query access to externally stored data (e.g., data in HDFS) as
   well as to data stored natively by AsterixDB
 * A rich set of primitive data types, including spatial and temporal data in
   addition to integer, floating point, and textual data
 * Secondary indexing options that include B+ trees, R trees, and inverted
   keyword (exact and fuzzy) index types
 * Support for fuzzy and spatial queries as well as for more traditional
   parametric queries
 * Basic transactional (concurrency and recovery) capabilities akin to those of
   a NoSQL store
