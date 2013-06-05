# AsterixDB: A Big Data Management System #

## What Is AsterixDB? ##

Welcome to the new home of the AsterixDB Big Data Management System (BDMS).
The AsterixDB BDMS is the result of about 3.5 years of R&D involving researchers at UC Irvine, UC Riverside, and UC San Diego.
The AsterixDB code base now consists of roughly 250K lines of Java code that has been co-developed at UC Irvine and UC Riverside.

Initiated in 2009, the NSF-sponsored ASTERIX project has been developing new technologies for ingesting, storing, managing, indexing, querying, and analyzing vast quantities of semi-structured information.
The project has been combining ideas from three distinct areas---semi-structured data, parallel databases, and data-intensive computing (a.k.a. today's Big Data platforms)---in order to create a next-generation, open-source software platform that scales by running on large, shared-nothing commodity computing clusters.
The ASTERIX effort has been targeting a wide range of semi-structured information, ranging from "data" use cases---where information is well-typed and highly regular---to "content" use cases---where data tends to be irregular, much of each datum may be textual, and the ultimate schema for the various data types involved may be hard to anticipate up front.
The ASTERIX project has been addressing technical issues including highly scalable data storage and indexing,  semi-structured query processing on very large clusters, and  merging time-tested parallel database techniques with modern data-intensive computing techniques  to support performant yet declarative solutions to the problem of storing and analyzing semi-structured information effectively.
The first fruits of this labor have been captured in the AsterixDB system that is now being released in preliminary or "Beta" release form.
We are hoping that the arrival of AsterixDB will mark the beginning of the "BDMS era", and we hope that both the Big Data community and the database community will find the AsterixDB system to be interesting and useful for a much broader class of problems than can be addressed with any one of today's current Big Data platforms and related technologies (e.g., Hadoop, Pig, Hive, HBase, MongoDB, and so on).  One of our project mottos has been "one size fits a bunch"---at least that has been our aim.  For more information about the research effort that led to the birth of AsterixDB, please refer to our NSF project web site: [http://asterix.ics.uci.edu/](http://asterix.ics.uci.edu/).

In a nutshell, AsterixDB is a full-function BDMS with a rich feature set that distinguishes it from pretty much any other Big Data platform that's out and available today.  We believe that its feature set makes it well-suited to modern needs such as web data warehousing and social data storage and analysis.  AsterixDB has:

 * A semistructured NoSQL style data model (ADM) resulting from extending JSON with object database ideas
 * An expressive and declarative query language (AQL) that supports a broad range of queries and analysis over semistructured data
 * A parallel runtime query execution engine, Hyracks, that has been scale-tested on up to 1000+ cores and 500+ disks
 * Partitioned LSM-based data storage and indexing to support efficient ingestion and management of semistructured data
 * Support for query access to externally stored data (e.g., data in HDFS) as well as to data stored natively by AsterixDB
 * A rich set of primitive data types, including spatial and temporal data in addition to integer, floating point, and textual data
 * Secondary indexing options that include B+ trees, R trees, and inverted keyword (exact and fuzzy) index types
 * Support for fuzzy and spatial queries as well as for more traditional parametric queries
 * Basic transactional (concurrency and recovery) capabilities akin to those of a NoSQL store

## Getting and Using AsterixDB ##

You are most likely here because you are interested in getting your hands on AsterixDB---so you would like to know how to get it, how to set it up, and how to use it.
Someday our plan is to have comprehensive documentation for AsterixDB and its data model (ADM) and query language (AQL) here on this wiki.
For the Beta release, we've got a start; for the Beta release a month or so from now, we will hopefully have much more.
The following is a list of the wiki pages and supporting documents that we have available today:

1. [Installing AsterixDB using Managix](install.html) :
This is our installation guide, and it is where you should start.
This document will tell you how to obtain, install, and manage instances of [AsterixDB](https://asterixdb.googlecode.com/files/asterix-installer-0.0.4-binary-assembly.zip), including both single-machine setup (for developers) as well as cluster installations (for deployment in its intended form).

2. [AsterixDB 101: An ADM and AQL Primer](aql/primer.html) :
This is a first-timers introduction to the user model of the AsterixDB BDMS, by which we mean the view of AsterixDB as seen from the perspective of an "average user" or Big Data application developer.
The AsterixDB user model consists of its data modeling features (ADM) and its query capabilities (AQL).
This document presents a tiny "social data warehousing" example and uses it as a backdrop for describing, by example, the key features of AsterixDB.
By working through this document, you will learn how to define the artifacts needed to manage data in AsterixDB, how to load data into the system, how to use most of the basic features of its query language, and how to insert and delete data dynamically.

3. [Asterix Data Model (ADM)](aql/datamodel.html), [Asterix Functions](aql/functions.html), and [Asterix Query Language (AQL)](aql/manual.html) :
These are reference documents that catalog the primitive data types and built-in functions available in AQL and the reference manual for AQL itself.

5. [REST API to AsterixDB](api.html) :
Access to data in an AsterixDB instance is provided via a REST-based API.
This is a short document that describes the REST API entry points and their URL syntax.

To all who have now come this far: Thanks for your interest in AsterixDB, and for kicking its tires in its Beta form.
In addition to getting the system and trying it out, please sign up as a member of the AsterixDB user mailing list (asterixdb-users (at) googlegroups.com) so that you can contact us easily with your questions, issues, and other feedback.
We want AsterixDB to be a "big hit" some day, and we are anxious to see what users do with it and to learn from that feedback what we should be working on most urgently in the next phase of the project.
