# Accessing External Data in AsterixDB #

## Introduction ##
Data that needs to be processed by ASTERIX could be residing outside ASTERIX storage. Examples include data files on a distributed file system such as HDFS or on the local file system of a machine that is part of an ASTERIX cluster.  For ASTERIX to process such data, end-user may create a regular dataset in ASTERIX (a.k.a. internal dataset) and load the dataset with the data. ASTERIX supports ''external datasets'' so that it is not necessary to “load” all data prior to using it. This also avoids creating multiple copies of data and the need to keep the copies in sync.

### Adapter for an External Dataset ###
External data is accessed using wrappers (adapters in ASTERIX) that abstract away the mechanism of connecting with an external service, receiving data and transforming the data into ADM records that are understood by ASTERIX. ASTERIX comes with built-in adapters for common storage systems such as HDFS or the local file system.

### Creating an External Dataset ###

As an example we consider the Lineitem dataset from [TPCH schema](http://www.openlinksw.com/dataspace/doc/dav/wiki/Main/VOSTPCHLinkedData/tpch.sql).

We assume that you have successfully created an ASTERIX instance following the instructions at [Installing Asterix Using Managix](../install.html).
_For constructing an example, we assume a single machine setup._

Similar to a regular dataset, an external dataset has an associated datatype.  We shall first create the datatype associated with each record in Lineitem data.
Paste the following in the query textbox on the webpage at http://127.0.0.1:19001 and hit 'Execute'.


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


We describe here two scenarios.

#### 1) Data file resides on the local file system of a host####
Prerequisite: The host is a  part of the ASTERIX cluster.

Earlier, we assumed a single machine ASTERIX setup. To satisfy the prerequisite, log-in to the machine running ASTERIX.

 * Download the [data file](https://code.google.com/p/asterixdb/downloads/detail?name=lineitem.tbl&amp;can=2&amp;q=) to an appropriate location. We denote this location by SOURCE_PATH.

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
  <td> The format for the content. Use 'adm' for data in ADM (ASTERIX Data Model) or <a href="http://www.json.org/">JSON</a> format. Use 'delimited-text' if fields are separted by . </td></tr>
<tr><td>delimiter</td><td>The delimiting character in the source file if format is 'delimited text'</td></tr>
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

        edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException: edu.uci.ics.hyracks.api.exceptions.HyracksDataException: edu.uci.ics.hyracks.api.exceptions.HyracksDataException: Job failed.


Verify the correctness of the path parameter provided to the localfs adapter. Note that the path parameter must be an absolute path to the data file. For e.g. if you saved your file in your home directory (assume it to be /home/joe), then the path value should be

        127.0.0.1:///home/joe/lineitem.tbl.


In your web-browser, navigate to 127.0.0.1:19001 and paste the above to the query text box. Finally hit 'Execute'.

Next we move over to the the section [Writing Queries against an External Dataset](#Writing_Queries_against_an_External_Dataset) and try a sample query against the external dataset.

#### 2) Data file resides on an HDFS instance ####
Pre-requisite: It is required that the Namenode and atleast one of the HDFS Datanodes are reachable from the hosts that form the ASTERIX cluster.  ASTERIX provides a built-in adapter for data residing on HDFS. The HDFS adapter is referred (in AQL) by its alias - 'hdfs'. We create an external dataset named Lineitem and associate the HDFS adapter with it.


        create external dataset Lineitem(LineitemType)
        using hdfs


The above statement is *not complete* as we need to provide a set of parameters specific to the HDFS instance and the source file.
These parameters are described below.

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
  <td> The absolute path to the source HDFS file. Use a comma separated list if there are multiple files. </td></tr>
<tr>
  <td> input-format </td>
  <td> The associated input format. Use 'text-input-format' for textual data or 'sequence-input-format' for binary data (sequence files). </td>
</tr>
<tr>
  <td> format </td>
  <td> The format for the content. Use 'adm' for data in ADM (ASTERIX Data Model) or 
  <a href="http://www.json.org/">JSON</a> format and use 'delimited-text' for delimited data 
  that has fields separated by a delimiting character. </td>
</tr>
<tr>
  <td> delimiter </td>
  <td> The delimiting character in the source file if format is 'delimited text' </td>
</tr>
</table>

*Difference between 'input-format' and 'format'*

*input-format*: File stored under HDFS have an associated storage format  For example, TextInputFormat represents plain text files.  SequenceFileInputFormat indicates binary compressed file. The parameter 'input-format' is used to distinguish between these two kind of files.

*format*:
The parameter 'format' refers to the type of the data contained in the file. For example data contained in a file could be in json, ADM format or could be delimited-text with fields separated by a delimiting character.

As an example. consider the [data file](https://code.google.com/p/asterixdb/downloads/detail?name=lineitem.tbl&amp;can=2&amp;q=).  The file is a text file with each line representing a record. The fields in each record are separated by the '|' character.

We assume the HDFS URL to be hdfs://host:port. We further assume that the example data file is copied to the HDFS at a path denoted by HDFS_PATH.

The complete set of parameters for our example file are as follows. (("hdfs"="HDFS_URL",("path"="HDFS_PATH"),("input-format"="text-input-format"),("format"="delimited-text"),("delimiter"="|"))

We modify the create external dataset statement as follows.


        create external dataset Lineitem('LineitemType)
        using hdfs
        (("hdfs"="HDFS_URL"),("path"="HDFS_PATH"),("input-format"="text-input-format"),("format"="delimited-text"),("delimiter"="|"));


Once you have copied the source data file to your HDFS instance, substitute the values of HDFS_URL and HDFS_PATH in the above statement. In your web-browser, navigate to http://127.0.0.1:19001 and execute the above statement with substituted values.

You may now run the sample query in next section.

## Writing Queries against an External Dataset ##
You may  write AQL queries against an external dataset. Following is an example AQL query that applies a filter and returns an ordered result.


        use dataverse ExternalFileDemo;
        
        for $c in dataset('Lineitem')
        where $c.l_orderkey <= 3
        order by $c.l_orderkey, $c.l_linenumber
        return $c


The expected result is:


        { "l_orderkey": 1, "l_partkey": 156, "l_suppkey": 4, "l_linenumber": 1, "l_quantity": 17, "l_extendedprice": 17954.55d, "l_discount": 0.04d, "l_tax": 0.02d, "l_returnflag": "N", "l_linestatus": "O", "l_shipdate": "1996-03-13", "l_commitdate": "1996-02-12", "l_receiptdate": "1996-03-22", "l_shipinstruct": "DELIVER IN PERSON", "l_shipmode": "TRUCK", "l_comment": "egular courts above the" }
        { "l_orderkey": 1, "l_partkey": 68, "l_suppkey": 9, "l_linenumber": 2, "l_quantity": 36, "l_extendedprice": 34850.16d, "l_discount": 0.09d, "l_tax": 0.06d, "l_returnflag": "N", "l_linestatus": "O", "l_shipdate": "1996-04-12", "l_commitdate": "1996-02-28", "l_receiptdate": "1996-04-20", "l_shipinstruct": "TAKE BACK RETURN", "l_shipmode": "MAIL", "l_comment": "ly final dependencies: slyly bold " }
        { "l_orderkey": 1, "l_partkey": 64, "l_suppkey": 5, "l_linenumber": 3, "l_quantity": 8, "l_extendedprice": 7712.48d, "l_discount": 0.1d, "l_tax": 0.02d, "l_returnflag": "N", "l_linestatus": "O", "l_shipdate": "1996-01-29", "l_commitdate": "1996-03-05", "l_receiptdate": "1996-01-31", "l_shipinstruct": "TAKE BACK RETURN", "l_shipmode": "REG AIR", "l_comment": "riously. regular, express dep" }
        { "l_orderkey": 1, "l_partkey": 3, "l_suppkey": 6, "l_linenumber": 4, "l_quantity": 28, "l_extendedprice": 25284.0d, "l_discount": 0.09d, "l_tax": 0.06d, "l_returnflag": "N", "l_linestatus": "O", "l_shipdate": "1996-04-21", "l_commitdate": "1996-03-30", "l_receiptdate": "1996-05-16", "l_shipinstruct": "NONE", "l_shipmode": "AIR", "l_comment": "lites. fluffily even de" }
        { "l_orderkey": 1, "l_partkey": 25, "l_suppkey": 8, "l_linenumber": 5, "l_quantity": 24, "l_extendedprice": 22200.48d, "l_discount": 0.1d, "l_tax": 0.04d, "l_returnflag": "N", "l_linestatus": "O", "l_shipdate": "1996-03-30", "l_commitdate": "1996-03-14", "l_receiptdate": "1996-04-01", "l_shipinstruct": "NONE", "l_shipmode": "FOB", "l_comment": " pending foxes. slyly re" }
        { "l_orderkey": 1, "l_partkey": 16, "l_suppkey": 3, "l_linenumber": 6, "l_quantity": 32, "l_extendedprice": 29312.32d, "l_discount": 0.07d, "l_tax": 0.02d, "l_returnflag": "N", "l_linestatus": "O", "l_shipdate": "1996-01-30", "l_commitdate": "1996-02-07", "l_receiptdate": "1996-02-03", "l_shipinstruct": "DELIVER IN PERSON", "l_shipmode": "MAIL", "l_comment": "arefully slyly ex" }
        { "l_orderkey": 2, "l_partkey": 107, "l_suppkey": 2, "l_linenumber": 1, "l_quantity": 38, "l_extendedprice": 38269.8d, "l_discount": 0.0d, "l_tax": 0.05d, "l_returnflag": "N", "l_linestatus": "O", "l_shipdate": "1997-01-28", "l_commitdate": "1997-01-14", "l_receiptdate": "1997-02-02", "l_shipinstruct": "TAKE BACK RETURN", "l_shipmode": "RAIL", "l_comment": "ven requests. deposits breach a" }
        { "l_orderkey": 3, "l_partkey": 5, "l_suppkey": 2, "l_linenumber": 1, "l_quantity": 45, "l_extendedprice": 40725.0d, "l_discount": 0.06d, "l_tax": 0.0d, "l_returnflag": "R", "l_linestatus": "F", "l_shipdate": "1994-02-02", "l_commitdate": "1994-01-04", "l_receiptdate": "1994-02-23", "l_shipinstruct": "NONE", "l_shipmode": "AIR", "l_comment": "ongside of the furiously brave acco" }
        { "l_orderkey": 3, "l_partkey": 20, "l_suppkey": 10, "l_linenumber": 2, "l_quantity": 49, "l_extendedprice": 45080.98d, "l_discount": 0.1d, "l_tax": 0.0d, "l_returnflag": "R", "l_linestatus": "F", "l_shipdate": "1993-11-09", "l_commitdate": "1993-12-20", "l_receiptdate": "1993-11-24", "l_shipinstruct": "TAKE BACK RETURN", "l_shipmode": "RAIL", "l_comment": " unusual accounts. eve" }
        { "l_orderkey": 3, "l_partkey": 129, "l_suppkey": 8, "l_linenumber": 3, "l_quantity": 27, "l_extendedprice": 27786.24d, "l_discount": 0.06d, "l_tax": 0.07d, "l_returnflag": "A", "l_linestatus": "F", "l_shipdate": "1994-01-16", "l_commitdate": "1993-11-22", "l_receiptdate": "1994-01-23", "l_shipinstruct": "DELIVER IN PERSON", "l_shipmode": "SHIP", "l_comment": "nal foxes wake. " }
        { "l_orderkey": 3, "l_partkey": 30, "l_suppkey": 5, "l_linenumber": 4, "l_quantity": 2, "l_extendedprice": 1860.06d, "l_discount": 0.01d, "l_tax": 0.06d, "l_returnflag": "A", "l_linestatus": "F", "l_shipdate": "1993-12-04", "l_commitdate": "1994-01-07", "l_receiptdate": "1994-01-01", "l_shipinstruct": "NONE", "l_shipmode": "TRUCK", "l_comment": "y. fluffily pending d" }
        { "l_orderkey": 3, "l_partkey": 184, "l_suppkey": 5, "l_linenumber": 5, "l_quantity": 28, "l_extendedprice": 30357.04d, "l_discount": 0.04d, "l_tax": 0.0d, "l_returnflag": "R", "l_linestatus": "F", "l_shipdate": "1993-12-14", "l_commitdate": "1994-01-10", "l_receiptdate": "1994-01-01", "l_shipinstruct": "TAKE BACK RETURN", "l_shipmode": "FOB", "l_comment": "ages nag slyly pending" }
        { "l_orderkey": 3, "l_partkey": 63, "l_suppkey": 8, "l_linenumber": 6, "l_quantity": 26, "l_extendedprice": 25039.56d, "l_discount": 0.1d, "l_tax": 0.02d, "l_returnflag": "A", "l_linestatus": "F", "l_shipdate": "1993-10-29", "l_commitdate": "1993-12-18", "l_receiptdate": "1993-11-04", "l_shipinstruct": "TAKE BACK RETURN", "l_shipmode": "RAIL", "l_comment": "ges sleep after the caref" }

