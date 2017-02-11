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

# AsterixDB: A Big Data Management System #

## <a id="toc">Table of Contents</a> ##
* [What Is AsterixDB?](#WhatIsAsterixDB)
* [Getting and Using AsterixDB](#GettingAndUsingAsterixDB)

## <a id="WhatIsAsterixDB">What Is AsterixDB?</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

In a nutshell, AsterixDB is a full-function BDMS (Big Data Management System) with a rich feature set that distinguishes it from pretty much any other Big Data platform that's out and available today.  We believe that its feature set makes it well-suited to modern needs such as web data warehousing and social data storage and analysis.  AsterixDB has:

 * A semistructured NoSQL style data model (ADM) resulting from extending JSON with object database ideas
 * An expressive and declarative query language (AQL) that supports a broad range of queries and analysis over semistructured data
 * A parallel runtime query execution engine, Apache Hyracks, that has been scale-tested on up to 1000+ cores and 500+ disks
 * Partitioned LSM-based data storage and indexing to support efficient ingestion and management of semistructured data
 * Support for query access to externally stored data (e.g., data in HDFS) as well as to data stored natively by AsterixDB
 * A rich set of primitive data types, including spatial and temporal data in addition to integer, floating point, and textual data
 * Secondary indexing options that include B+ trees, R trees, and inverted keyword (exact and fuzzy) index types
 * Support for fuzzy and spatial queries as well as for more traditional parametric queries
 * Basic transactional (concurrency and recovery) capabilities akin to those of a NoSQL store

## <a id="GettingAndUsingAsterixDB">Getting and Using AsterixDB</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

You are most likely here because you are interested in getting your hands on AsterixDB---so you would like to know how to get it, how to set it up, and how to use it.
The following is a list of the supporting documents that we have available today:

1. [Installing AsterixDB using Managix](install.html) :
This is our installation guide, and it is where you should start.
This document will tell you how to obtain, install, and manage instances of [AsterixDB](https://asterixdb.apache.org/download.html), including both single-machine setup (for developers) as well as cluster installations (for deployment in its intended form).

2. [AsterixDB 101: An ADM and AQL Primer](aql/primer.html) :
This is a first-timers introduction to the user model of the AsterixDB BDMS, by which we mean the view of AsterixDB as seen from the perspective of an "average user" or Big Data application developer.
The AsterixDB user model consists of its data modeling features (ADM) and its query capabilities (AQL).
This document presents a tiny "social data warehousing" example and uses it as a backdrop for describing, by example, the key features of AsterixDB.
By working through this document, you will learn how to define the artifacts needed to manage data in AsterixDB, how to load data into the system, how to use most of the basic features of its query language, and how to insert and delete data dynamically.

3. [Asterix Data Model (ADM)](aql/datamodel.html), [Asterix Functions](aql/functions.html), [Asterix functions for Allen's Relations](aql/allens.html), and [Asterix Query Language (AQL)](aql/manual.html) :
These are reference documents that catalog the primitive data types and built-in functions available in AQL and the reference manual for AQL itself.

5. [REST API to AsterixDB](api.html) :
Access to data in an AsterixDB instance is provided via a REST-based API.
This is a short document that describes the REST API entry points and their URL syntax.

To all who have now come this far: Thanks for your interest in AsterixDB, and for kicking its tires in its Beta form.
In addition to getting the system and trying it out, please sign up as a member of the AsterixDB user mailing list (users (at) asterixdb.apache.org) so that you can contact us easily with your questions, issues, and other feedback.
We want AsterixDB to be a "big hit" some day, and we are anxious to see what users do with it and to learn from that feedback what we should be working on most urgently in the next phase of the project.
