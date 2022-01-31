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

# Querying Parquet Files #

## <a id="toc">Table of Contents</a> ##

* [Overview](#Overview)
* [DDL](#DDL)
* [Query Parquet Files](#QueryParquetFiles)
* [Type Compatibility](#TypeCompatibility)
* [Parquet Type Flags](#ParquetTypeFlags)

## <a id="Overview">Overview</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

[Apache Parquet](https://parquet.apache.org/) is a columnar file format for storing semi-structured data (like JSON).
Apache AsterixDB supports running queries against Parquet files that are stored in Amazon S3 and Microsoft Azure Blob
Storage as [External Datasets](../aql/externaldata.html).

## <a id="DDL">DDL</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

To start, an end-user needs to create a type as follows:

    -- The type should not contain any declared fields
    CREATE TYPE ParquetType AS {
    }

Note that the created type does not have any declared fields. The reason is that Parquet files embed the schema within
each file. Thus, no type is needed to be declared, and it is up to AsterixDB to read each file's schema. If the created
type contains any declared type, AsterixDB will throw an error:

    Type 'ParquetType' contains declared fields, which is not supported for 'parquet' format

Next, the user can create an external dataset - using the declared type - as follows:

### Amazon S3

    CREATE EXTERNAL DATASET ParquetDataset(ParquetType) USING S3
    (
        -- Replace <ACCESS-KEY> with your access key
        ("accessKeyId"="<ACCESS-KEY>"),

        -- Replace <SECRET-ACCESS-KEY> with your access key
        ("secretAccessKey" = "<SECRET-ACCESS-KEY>"),

        -- S3 bucket
        ("container"="parquetBucket"),

        -- Path to the parquet files within the bucket
        ("definition"="path/to/parquet/files"),

        -- Specifying the format as parquet
        ("format" = "parquet")
    );

### Microsoft Azure Blob Storage

    CREATE EXTERNAL DATASET ParquetDataset(ParquetType) USING AZUREBLOB
    (
        -- Replace <ACCOUNT-NAME> with your account name
        ("accountName"="<ACCOUNT-NAME>"),

        -- Replace <ACCOUNT-KEY> with your account key
        ("accountKey"="<ACCOUNT-KEY>"),

        -- Azure Blob container
        ("container"="parquetContainer"),

        -- Path to the parquet files within the bucket
        ("definition"="path/to/parquet/files"),

        -- Specifying the format as parquet
        ("format" = "parquet")
    );

<i><b>Additional setting/properties could be set as detailed later in [Parquet Type Flags](#ParquetTypeFlags)</b></i>

## <a id="QueryParquetFiles">Query Parquet Files</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

To query the data stored in Parquet files, one can simply write a query against the created External Dataset. For
example:

    SELECT COUNT(*)
    FROM ParquetDataset;

Another example:

    SELECT pd.age, COUNT(*) cnt
    FROM ParquetDataset pd
    GROUP BY pd.age;

## <a id="TypeCompatibility">Type Compatibility</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

AsterixDB supports Parquet's generic types such `STRING`, `INT` and `DOUBLE`. However, Parquet files could
contain [additional types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) such as `DATE` and
`DATETIME` like types. The following table show the type mapping between Apache Parquet and AsterixDB:

<table>
    <thead>
        <tr>
            <th>Parquet</th>
            <th>AsterixDB</th>
            <th>Value Examples</th>
            <th>Comment</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><code>BOOLEAN</code></td>
            <td><code>BOOLEAN</code></td>
            <td><code>true</code> / <code>false</code></td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>INT_8</code></td>
            <td rowspan="8"><code>BIGINT</code></td>
            <td rowspan="8">
                AsterixDB <code>BIGINT</code> Range:
                <ul>
                    <li><b>Min</b>:-9,223,372,036,854,775,808</li>
                    <li><b>Max</b>: 9,223,372,036,854,775,807</li>
                </ul>
            </td>
            <td rowspan="7">-</td>
        </tr>
        <tr>
            <td><code>INT_16</code></td>
        </tr>
        <tr>
            <td><code>INT_32</code></td>
        </tr>
        <tr>
            <td><code>INT_64</code></td>
        </tr>
        <tr>
            <td><code>UNIT_8</code></td>
        </tr>
        <tr>
            <td><code>UINT_16</code></td>
        </tr>
        <tr>
            <td><code>UINT_32</code></td>
        </tr>
        <tr>
            <td><code>UINT_64</code></td>
            <td>There is a possibility that a value overflows. A warning will be issued in case of an overflow and
                <code>MISSING</code> would be returned.
            </td>
        </tr>
        <tr>
            <td><code>FLOAT</code></td>
            <td rowspan="4"><code>DOUBLE</code></td>
            <td rowspan="4">
                AsterixDB <code>DOUBLE</code> Range:
                <ul>
                    <li><b>Min Positive Value</b>: 2^-1074</li>
                    <li><b>Max Positive Value</b>: 2^1023</li>
                </ul>
            </td>
            <td rowspan="2">-</td>
        </tr>
        <tr>
            <td><code>DOUBLE</code></td>
        </tr>
        <tr>
            <td><code>FIXED_LEN_BYTE_ARRAY (DECIMAL)</code></td>
            <td rowspan="2">
                Parquet <code>DECIMAL</code> values are converted to doubles, with the possibility of precision loss.
                The flag <code>decimal-to-double</code> must be set upon creating the dataset.
                <ul><li><i>See <a href ="#ParquetTypeFlags">Parquet Type Flags</a></i></li></ul>
            </td>
        </tr>
        <tr>
            <td><code>BINARY (DECIMAL)</code></td>
        </tr>
        <tr>
            <td><code>BINARY (ENUM)</code></td>
            <td><code>"Fruit"</code></td>
            <td>Parquet Enum values are parsed as Strings</td>
        </tr>
        <tr>
            <td><code>BINARY (UTF8)</code></td>
            <td><code>STRING</code></td>
            <td><code>"Hello World"</code></td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>FIXED_LEN_BYTE_ARRAY (UUID)</code></td>
            <td><code>UUID</code></td>
            <td><code>uuid("123e4567-e89b-12d3-a456-426614174000")</code></td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>INT_32 (DATE)</code></td>
            <td><code>DATE</code></td>
            <td><code>date("2021-11-01")</code></td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>INT_32 (TIME)</code></td>
            <td><code>TIME</code></td>
            <td rowspan="2"><code>time("00:00:00.000")</code></td>
            <td>Time in milliseconds.</td>
        </tr>
        <tr>
            <td><code>INT_64 (TIME)</code></td>
            <td><code>TIME</code></td>
            <td>Time in micro/nano seconds.</td>
        </tr>
        <tr>
            <td><code>INT_64 (TIMESTAMP)</code></td>
            <td rowspan="2"><code>DATETIME</code></td>
            <td rowspan="2"><code>datetime("2021-11-01T21:37:13.738")"</code></td>
            <td>Timestamp in milli/micro/nano seconds. Parquet also can store the timestamp values with the option
                <code>isAdjustedToUTC = true</code>. To get the local timestamp value, the user can set the time zone ID 
                by setting the value using the option <code>timezone</code> to get the local <code>DATETIME</code> value.
                <ul><li><i>See <a href ="#ParquetTypeFlags">Parquet Type Flags</a></i></li></ul>
            </td>
        </tr>
        <tr>
            <td><code>INT96</code></td>
            <td>A timestamp values that separate days and time to form a timestamp. INT96 is always in localtime.</td>
        </tr>
        <tr>
            <td><code>BINARY (JSON)</code></td>
            <td>any type</td>
            <td>
                <ul>
                    <li><code>{"name": "John"}</code></li>
                    <li><code>[1, 2, 3]</code></li>
                </ul> 
            </td>
            <td>
                Parse JSON string into internal AsterixDB value.
                The flag <code>parse-json-string</code> is set by default. To get the string value (i.e., not parsed as
                AsterixDB value), unset the flag <code>parse-json-string</code>.
                <ul><li><i>See <a href ="#ParquetTypeFlags">Parquet Type Flags</a></i></li></ul>
            </td>
        </tr>
        <tr>
            <td><code>BINARY</code></td>
            <td rowspan="2"><code>BINARY</code></td>
            <td><code>hex("0101FF")</code></td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>BSON</code></td>
            <td>N/A</td>
            <td>BSON values will be returned as <code>BINARY</code></td>
        </tr>
        <tr>
            <td><code>LIST</code></td>
            <td><code>ARRAY</code></td>
            <td><code>[1, 2, 3]</code></td>
            <td>Parquet's <code>LIST</code> type is converted into <code>ARRAY</code></td>
        </tr>
        <tr>
            <td><code>MAP</code></td>
            <td><code>ARRAY</code> of <code>OBJECT</code></td>
            <td><code>[{"key":1, "value":1}, {"key":2, "value":2}]</code></td>
            <td>Parquet's <code>MAP</code> types are converted into an <code>ARRAY</code> of <code>OBJECT</code>. Each 
                <code>OBJECT</code> value consists of two fields: <code>key</code> and <code>value</code>
            </td>
        </tr>
        <tr>
            <td><code>FIXED_LEN_BYTE_ARRAY (INTERVAL)</code></td>
            <td>-</td>
            <td>N/A</td>
            <td><code>INTERVAL</code> is not supported. A warning will be issued and <code>MISSING</code> value
                will be returned.
            </td>
        </tr>
    </tbody>
</table>

## <a id="ParquetTypeFlags">Parquet Type Flags</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

The table in [Type Compatibility](#TypeCompatibility) shows the type mapping between Parquet and AsterixDB. Some of the
Parquet types are not parsed by default as those type are not natively supported in AsterixDB. However, the user can set
a flag to convert some of those types into a supported AsterixDB type.

##### DECIMAL TYPE

The user can enable parsing `DECIMAL` Parquet values by enabling a certain flag as in the following example:

    CREATE EXTERNAL DATASET ParquetDataset(ParquetType) USING S3
    (
        -- Credintials and path to Parquet files
        ...

        -- Enable converting decimal values to double
        ("decimal-to-double" = "true")
    );

This flag will enable parsing/converting `DECIMAL` values/types into `DOUBLE`. For example, if the flag
`decimal-to-double` is not set and a Parquet file contains a `DECIMAL` value, the following error will be thrown when
running a query that request a `DECIMAL` value:

    Parquet type "optional fixed_len_byte_array(16) decimalType (DECIMAL(38,18))" is not supported by default. To enable type conversion, recreate the external dataset with the option "decimal-to-double" enabled

and the returned value will be `MISSING`. If the flag `decimal-to-double` is set, the converted `DOUBLE` value will be
returned.

##### TEMPORAL TYPES

For the temporal types (namely `DATETIME`), their values could be stored in Parquet with the option
`isAdjustedToUTC = true`. Hence, the user has to provide the timezone ID to adjust their values to the local value by
setting the flag `timezone`. To do so, a user can set the timezone ID to "<b>PST</b>" upon creating a dataset as in the
following example:

    CREATE EXTERNAL DATASET ParquetDataset(ParquetType) USING S3
    (
        -- Credintials and path to Parquet files
        ...

        -- Converting UTC time to PST time
        ("timezone" = "PST")
    );

If the flag `timezone` is not set, a warning will appear when running a query:

    Parquet file(s) contain "datetime" values that are adjusted to UTC. Recreate the external dataset and set "timezone" to get the local "datetime" value.

and the UTC `DATETIME` will be returned.

##### JSON TYPE

By default, we parse the JSON values into AsterixDB values, where a user can process those values using `SQL++` queries.
However, one could disable the parsing of JSON string values (which stored as `STRING`) by unsetting the flag
`parseJsonString` as in the following example:

    CREATE EXTERNAL DATASET ParquetDataset(ParquetType) USING S3
    (
        -- Credintials and path to Parquet files
        ...

        -- Stop parsing JSON string values
        ("parse-json-string" = "false")
    );

And the returned value will be of type `STRING`.

##### INTERVAL TYPE

Currently, AsterixDB do not support Parquet's `INTERVAL` type. When a query requests (or projects) an `INTERVAL` value,
a warning will be issued and `MISSING` value will be returned instead.
    