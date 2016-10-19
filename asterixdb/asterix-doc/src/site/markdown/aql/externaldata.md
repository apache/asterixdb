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

# Accessing External Data in AsterixDB #

## <a id="toc">Table of Contents</a> ##

* [Introduction](#Introduction)
* [Adapter for an External Dataset](#IntroductionAdapterForAnExternalDataset)
* [Builtin Adapters](#BuiltinAdapters)
* [Creating an External Dataset](#IntroductionCreatingAnExternalDataset)
* [Writing Queries against an External Dataset](#WritingQueriesAgainstAnExternalDataset)
* [Building Indexes over External Datasets](#BuildingIndexesOverExternalDatasets)
* [External Data Snapshots](#ExternalDataSnapshot)
* [Frequently Asked Questions](#FAQ)

## <a id="Introduction">Introduction</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
Data that needs to be processed by AsterixDB could be residing outside AsterixDB storage. Examples include data files on a distributed file system such as HDFS or on the local file system of a machine that is part of an AsterixDB cluster. For AsterixDB to process such data, an end-user may create a regular dataset in AsterixDB (a.k.a. an internal dataset) and load the dataset with the data. AsterixDB also supports ‘‘external datasets’’ so that it is not necessary to “load” all data prior to using it. This also avoids creating multiple copies of data and the need to keep the copies in sync.

### <a id="IntroductionAdapterForAnExternalDataset">Adapter for an External Dataset</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ###
External data is accessed using wrappers (adapters in AsterixDB) that abstract away the mechanism of connecting with an external service, receiving its data and transforming the data into ADM objects that are understood by AsterixDB. AsterixDB comes with built-in adapters for common storage systems such as HDFS or the local file system.

### <a id="BuiltinAdapters">Builtin Adapters</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ###
AsterixDB offers a set of builtin adapters that can be used to query external data or for loading data into an internal dataset using a load statement or a data feed. Each adapter requires specifying the `format` of the data in order to be able to parse objects correctly. Using adapters with feeds, the parameter `output-type` must also be specified.

Following is a listing of existing built-in adapters and their configuration parameters:

1. ___localfs___: used for reading data stored in a local filesystem in one or more of the node controllers
     * `path`: A fully qualified path of the form `host://absolute_path`. Comma separated list if there are
     multiple directories or files
     * `expression`: A [regular expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html) to match and filter against file names
2. ___hdfs___: used for reading data stored in an HDFS instance
     * `path`: A fully qualified path of the form `host://absolute_path`. Comma separated list if there are
     multiple directories or files
     * `expression`: A [regular expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html) to match and filter against file names
     * `input-format`: A fully qualified name or an alias for a class of HDFS input format
     * `hdfs`: The HDFS name node URL
3. ___socket___: used for listening to connections that sends data streams through one or more sockets
     * `sockets`: comma separated list of sockets to listen to
     * `address-type`: either IP if the list uses IP addresses, or NC if the list uses NC names
4. ___socket\_client___: used for connecting to one or more sockets and reading data streams
     * `sockets`: comma separated list of sockets to connect to
5. ___twitter\_push___: used for establishing a connection and subscribing to a twitter feed
     * `consumer.key`: access parameter provided by twitter OAuth
     * `consumer.secret`: access parameter provided by twitter OAuth
     * `access.token`: access parameter provided by twitter OAuth
     * `access.token.secret`: access parameter provided by twitter OAuth
6. ___twitter\_pull___: used for polling a twitter feed for tweets based on a configurable frequency
     * `consumer.key`: access parameter provided by twitter OAuth
     * `consumer.secret`: access parameter provided by twitter OAuth
     * `access.token`: access parameter provided by twitter OAuth
     * `access.token.secret`: access parameter provided by twitter OAuth
     * `query`: twitter query string
     * `interval`: poll interval in seconds
7. ___rss___: used for reading RSS feed
     * `url`: a comma separated list of RSS urls


### <a id="IntroductionCreatingAnExternalDataset">Creating an External Dataset</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ###
As an example we consider the Lineitem dataset from the [TPCH schema](http://www.openlinksw.com/dataspace/doc/dav/wiki/Main/VOSTPCHLinkedData/tpch.sql).
We assume that you have successfully created an AsterixDB instance following the instructions at [Installing AsterixDB Using Managix](../install.html). _For constructing an example, we assume a single machine setup.._

Similar to a regular dataset, an external dataset has an associated datatype. We shall first create the datatype associated with each object in Lineitem data. Paste the following in the
query textbox on the webpage at http://127.0.0.1:19001 and hit ‘Execute’.

        create dataverse ExternalFileDemo;
        use dataverse ExternalFileDemo;

        create type LineitemType as closed {
          l_orderkey:int32,
          l_partkey: int32,
          l_suppkey: int32,
          l_linenumber: int32,
          l_quantity: double,
          l_extendedprice: double,
          l_discount: double,
          l_tax: double,
          l_returnflag: string,
          l_linestatus: string,
          l_shipdate: string,
          l_commitdate: string,
          l_receiptdate: string,
          l_shipinstruct: string,
          l_shipmode: string,
          l_comment: string}


Here, we describe two scenarios.

#### 1) Data file resides on the local file system of a host####
Prerequisite: The host is a  part of the ASTERIX cluster.

Earlier, we assumed a single machine ASTERIX setup. To satisfy the prerequisite, log-in to the machine running ASTERIX.

 * Download the [data file](../data/lineitem.tbl) to an appropriate location. We denote this location by SOURCE_PATH.

ASTERIX provides a built-in adapter for data residing on the local file system. The adapter is referred by its alias- 'localfs'. We create an external dataset named Lineitem and use the 'localfs' adapter.


        create external dataset Lineitem(LineitemType)
        using localfs

Above, the definition is not complete as we need to provide a set of parameters that are specific to the source file.

<table>
<tr>
  <td> Parameter </td>
  <td> Description </td>
</tr>
<tr>
  <td> path </td>
  <td> A fully qualified path of the form <tt>host://&lt;absolute path&gt;</tt>.
  Use a comma separated list if there are multiple files.
  E.g. <tt>host1://&lt;absolute path&gt;</tt>, <tt>host2://&lt;absolute path&gt;</tt> and so forth. </td>
</tr>
<tr>
  <td> format </td>
  <td> The format for the content. Use 'adm' for data in ADM (ASTERIX Data Model) or <a href="http://www.json.org/">JSON</a> format. Use 'delimited-text' if fields are separated by a delimiting character (eg., CSV). </td></tr>
<tr>
  <td>delimiter</td>
  <td>The delimiting character in the source file if format is 'delimited text'</td>
</tr>
</table>

As we are using a single single machine ASTERIX instance, we use 127.0.0.1 as host in the path parameter.
We *complete the create dataset statement* as follows.


        use dataverse ExternalFileDemo;

        create external dataset Lineitem(LineitemType)
        using localfs
        (("path"="127.0.0.1://SOURCE_PATH"),
        ("format"="delimited-text"),
        ("delimiter"="|"));


Please substitute SOURCE_PATH with the absolute path to the source file on the local file system.

#### Common source of error ####

An incorrect value for the path parameter will give the following exception message when the dataset is used in a query.

        org.apache.hyracks.algebricks.common.exceptions.AlgebricksException: org.apache.hyracks.api.exceptions.HyracksDataException: org.apache.hyracks.api.exceptions.HyracksDataException: Job failed.


Verify the correctness of the path parameter provided to the localfs adapter. Note that the path parameter must be an absolute path to the data file. For e.g. if you saved your file in your home directory (assume it to be /home/joe), then the path value should be

        127.0.0.1:///home/joe/lineitem.tbl.


In your web-browser, navigate to 127.0.0.1:19001 and paste the above to the query text box. Finally hit 'Execute'.

Next we move over to the the section [Writing Queries against an External Dataset](#Writing_Queries_against_an_External_Dataset) and try a sample query against the external dataset.

#### 2) Data file resides on an HDFS instance ####
rerequisite: It is required that the Namenode and HDFS Datanodes are reachable from the hosts that form the AsterixDB cluster. AsterixDB provides a built-in adapter for data residing on HDFS. The HDFS adapter can be referred (in AQL) by its alias - ‘hdfs’. We can create an external dataset named Lineitem and associate the HDFS adapter with it as follows;

        create external dataset Lineitem(LineitemType)
        using hdfs((“hdfs”:”hdfs://localhost:54310”),(“path”:”/asterix/Lineitem.tbl”),...,(“input- format”:”rc-format”));

The expected parameters are described below:

<table>
<tr>
  <td> Parameter </td>
  <td> Description </td>
</tr>
<tr>
  <td> hdfs </td>
  <td> The HDFS URL </td>
</tr>
<tr>
  <td> path </td>
  <td> The absolute path to the source HDFS file or directory. Use a comma separated list if there are multiple files or directories. </td></tr>
<tr>
  <td> input-format </td>
  <td> The associated input format. Use 'text-input-format' for text files , 'sequence-input-format' for hadoop sequence files, 'rc-input-format' for Hadoop Object Columnar files, or a fully qualified name of an implementation of org.apache.hadoop.mapred.InputFormat. </td>
</tr>
<tr>
  <td> format </td>
  <td> The format of the input content. Use 'adm' for text data in ADM (ASTERIX Data Model) or <a href="http://www.json.org/">JSON</a> format, 'delimited-text' for text delimited data that has fields separated by a delimiting character, 'binary' for other data.</td>
</tr>
<tr>
  <td> delimiter </td>
  <td> The delimiting character in the source file if format is 'delimited text' </td>
</tr>
<tr>
  <td> parser </td>
  <td> The parser used to parse HDFS objects if the format is 'binary'. Use 'hive- parser' for data deserialized by a Hive Serde (AsterixDB can understand deserialized Hive objects) or a fully qualified class name of user- implemented parser that implements the interface org.apache.asterix.external.input.InputParser. </td>
</tr>
<tr>
  <td> hive-serde </td>
  <td> The Hive serde is used to deserialize HDFS objects if format is binary and the parser is hive-parser. Use a fully qualified name of a class implementation of org.apache.hadoop.hive.serde2.SerDe. </td>
</tr>
<tr>
  <td> local-socket-path </td>
  <td> The UNIX domain socket path if local short-circuit reads are enabled in the HDFS instance</td>
</tr>
</table>

*Difference between 'input-format' and 'format'*

*input-format*: Files stored under HDFS have an associated storage format. For example,
TextInputFormat represents plain text files. SequenceFileInputFormat indicates binary compressed files. RCFileInputFormat corresponds to objects stored in a object columnar fashion. The parameter ‘input-format’ is used to distinguish between these and other HDFS input formats.

*format*: The parameter ‘format’ refers to the type of the data contained in the file. For example, data contained in a file could be in json or ADM format, could be in delimited-text with fields separated by a delimiting character or could be in binary format.

As an example. consider the [data file](../data/lineitem.tbl).  The file is a text file with each line representing a object. The fields in each object are separated by the '|' character.

We assume the HDFS URL to be hdfs://localhost:54310. We further assume that the example data file is copied to HDFS at a path denoted by “/asterix/Lineitem.tbl”.

The complete set of parameters for our example file are as follows. ((“hdfs”=“hdfs://localhost:54310”,(“path”=“/asterix/Lineitem.tbl”),(“input-format”=“text- input-format”),(“format”=“delimited-text”),(“delimiter”=“|”))


#### Using the Hive Parser ####

if a user wants to create an external dataset that uses hive-parser to parse HDFS objects, it is important that the datatype associated with the dataset matches the actual data in the Hive table for the correct initialization of the Hive SerDe. Here is the conversion from the supported Hive data types to AsterixDB data types:

<table>
<tr>
  <td> Hive </td>
  <td> AsterixDB </td>
</tr>
<tr>
  <td>BOOLEAN</td>
  <td>Boolean</td>
</tr>
<tr>
  <td>BYTE(TINY INT)</td>
  <td>Int8</td>
</tr>
<tr>
  <td>DOUBLE</td>
  <td>Double</td>
</tr>
<tr>
  <td>FLOAT</td>
  <td>Float</td>
</tr>
<tr>
  <td>INT</td>
  <td>Int32</td>
</tr>
<tr>
  <td>LONG(BIG INT)</td>
  <td>Int64</td>
</tr>
<tr>
  <td>SHORT(SMALL INT)</td>
  <td>Int16</td>
</tr>
<tr>
  <td>STRING</td>
  <td>String</td>
</tr>
<tr>
  <td>TIMESTAMP</td>
  <td>Datetime</td>
</tr>
<tr>
  <td>DATE</td>
  <td>Date</td>
</tr>
<tr>
  <td>STRUCT</td>
  <td>Nested Object</td>
</tr>
<tr>
  <td>LIST</td>
  <td>OrderedList or UnorderedList</td>
</tr>
</table>


#### Examples of dataset definitions for external datasets ####

*Example 1*: We can modify the create external dataset statement as follows:

        create external dataset Lineitem('LineitemType)
        using hdfs(("hdfs"="hdfs://localhost:54310"),("path"="/asterix/Lineitem.tbl"),("input-format"="text- input-format"),("format"="delimited-text"),("delimiter"="|"));

*Example 2*: Here, we create an external dataset of lineitem objects stored in sequence files that has content in ADM format:

        create external dataset Lineitem('LineitemType)
        using hdfs(("hdfs"="hdfs://localhost:54310"),("path"="/asterix/SequenceLineitem.tbl"),("input- format"="sequence-input-format"),("format"="adm"));

*Example 3*: Here, we create an external dataset of lineitem objects stored in object-columnar files that has content in binary format parsed using hive-parser with hive ColumnarSerde:

        create external dataset Lineitem('LineitemType)
        using hdfs(("hdfs"="hdfs://localhost:54310"),("path"="/asterix/RCLineitem.tbl"),("input-format"="rc-input-format"),("format"="binary"),("parser"="hive-parser"),("hive- serde"="org.apache.hadoop.hive.serde2.columnar.ColumnarSerde"));

## <a id="WritingQueriesAgainstAnExternalDataset">Writing Queries against an External Dataset</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
You may write AQL queries against an external dataset in exactly the same way that queries are written against internal datasets. The following is an example of an AQL query that applies a filter and returns an ordered result.


        use dataverse ExternalFileDemo;

        for $c in dataset('Lineitem')
        where $c.l_orderkey <= 3
        order by $c.l_orderkey, $c.l_linenumber
        return $c

## <a id="BuildingIndexesOverExternalDatasets">Building Indexes over External Datasets</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
AsterixDB supports building B-Tree and R-Tree indexes over static data stored in the Hadoop Distributed File System.
To create an index, first create an external dataset over the data as follows

        create external dataset Lineitem(LineitemType)
        using hdfs(("hdfs"="hdfs://localhost:54310"),("path"="/asterix/Lineitem.tbl"),("input-format"="text-input- format"),("format"="delimited-text"),("delimiter"="|"));

You can then create a B-Tree index on this dataset instance as if the dataset was internally stored as follows:

        create index PartkeyIdx on Lineitem(l_partkey);

You could also create an R-Tree index as follows:

        ￼create index IndexName on DatasetName(attribute-name) type rtree;

After building the indexes, the AsterixDB query compiler can use them to access the dataset and answer queries in a more cost effective manner.
AsterixDB can read all HDFS input formats, but indexes over external datasets can currently be built only for HDFS datasets with 'text-input-format', 'sequence-input-format' or 'rc-input-format'.

## <a id="ExternalDataSnapshots">External Data Snapshots</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
An external data snapshot represents the status of a dataset's files in HDFS at a point in time. Upon creating the first index over an external dataset, AsterixDB captures and stores a snapshot of the dataset in HDFS. Only objects present at the snapshot capture time are indexed, and any additional indexes created afterwards will only contain data that was present at the snapshot capture time thus preserving consistency across all indexes of a dataset.
To update all indexes of an external dataset and advance the snapshot time to be the present time, a user can use the refresh external dataset command as follows:

        refresh external dataset DatasetName;

After a refresh operation commits, all of the dataset's indexes will reflect the status of the data as of the new snapshot capture time.

## <a id="FAQ">Frequently Asked Questions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

Q. I added data to my dataset in HDFS, Will the dataset indexes in AsterixDB be updated automatically?

A. No, you must use the refresh external dataset statement to make the indexes aware of any changes in the dataset files in HDFS.

Q. Why doesn't AsterixDB update external indexes automatically?

A. Since external data is managed by other users/systems with mechanisms that are system dependent, AsterixDB has no way of knowing exactly when data is added or deleted in HDFS, so the responsibility of refreshing indexes are left to the user. A user can use internal datasets for which AsterixDB manages the data and its indexes.

Q. I created an index over an external dataset and then added some data to my HDFS dataset. Will a query that uses the index return different results from a query that doesn't use the index?

A. No, queries' results are access path independent and the stored snapshot is used to determines which data are going to be included when processing queries.

Q. I created an index over an external dataset and then deleted some of my dataset's files in HDFS, Will indexed data access still return the objects in deleted files?

A. No. When AsterixDB accesses external data, with or without the use of indexes, it only access files present in the file system at runtime.

Q. I submitted a refresh command on a an external dataset and a failure occurred, What has happened to my indexes?

A. External Indexes Refreshes are treated as a single transaction. In case of a failure, a rollback occurs and indexes are restored to their previous state. An error message with the cause of failure is returned to the user.

Q. I was trying to refresh an external dataset while some queries were accessing the data using index access method. Will the queries be affected by the refresh operation?

A. Queries have access to external dataset indexes state at the time where the queries are submitted. A query that was submitted before a refresh commits will only access data under the snapshot taken before the refresh; queries that are submitted after the refresh commits will access data under the snapshot taken after the refresh.

Q. What happens when I try to create an additional index while a refresh operation is in progress or vice versa?

A. The create index operation will wait until the refresh commits or aborts and then the index will be built according to the external data snapshot at the end of the refresh operation. Creating indexes and refreshing datasets are mutually exclusive operations and will not be run in parallel. Multiple indexes can be created in parallel, but not multiple refresh operations.
