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

### <a id="Functions"> Functions</a>

The create function statement creates a **named** function that can then be used and reused in queries.
The body of a function can be any query expression involving the function's parameters.

    FunctionSpecification ::= "FUNCTION" FunctionOrTypeName IfNotExists ParameterList "{" Expression "}"

The following is an example of a CREATE FUNCTION statement which is similar to our earlier DECLARE FUNCTION example.
It differs from that example in that it results in a function that is persistently registered by name in the specified dataverse (the current dataverse being used, if not otherwise specified).

##### Example

    CREATE FUNCTION friendInfo(userId) {
        (SELECT u.id, u.name, len(u.friendIds) AS friendCount
         FROM GleambookUsers u
         WHERE u.id = userId)[0]
     };

### <a id="Removal"> Removal</a>

    DropStatement       ::= "DROP" ( "DATAVERSE" Identifier IfExists
                                   | "TYPE" FunctionOrTypeName IfExists
                                   | "DATASET" QualifiedName IfExists
                                   | "INDEX" DoubleQualifiedName IfExists
                                   | "FUNCTION" FunctionSignature IfExists )
    IfExists            ::= ( "IF" "EXISTS" )?

The DROP statement is the inverse of the CREATE statement. It can be used to drop dataverses, datatypes, datasets, indexes, and functions.

The following examples illustrate some uses of the DROP statement.

##### Example

    DROP DATASET GleambookUsers IF EXISTS;

    DROP INDEX GleambookMessages.gbSenderLocIndex;

    DROP TYPE TinySocial2.GleambookUserType;

    DROP FUNCTION friendInfo@1;

    DROP DATAVERSE TinySocial;

When an artifact is dropped, it will be droppped from the current dataverse if none is specified
(see the DROP DATASET example above) or from the specified dataverse (see the DROP TYPE example above)
if one is specified by fully qualifying the artifact name in the DROP statement.
When specifying an index to drop, the index name must be qualified by the dataset that it indexes.
When specifying a function to drop, since the query language allows functions to be overloaded by their number of arguments,
the identifying name of the function to be dropped must explicitly include that information.
(`friendInfo@1` above denotes the 1-argument function named friendInfo in the current dataverse.)

### <a id="Load_statement">Load Statement</a>

    LoadStatement  ::= <LOAD> <DATASET> QualifiedName <USING> AdapterName Configuration ( <PRE-SORTED> )?

The LOAD statement is used to initially populate a dataset via bulk loading of data from an external file.
An appropriate adapter must be selected to handle the nature of the desired external data.
The LOAD statement accepts the same adapters and the same parameters as discussed earlier for External datasets.
(See the [guide to external data](externaldata.html) for more information on the available adapters.)
If a dataset has an auto-generated primary key field, the file to be imported should not include that field in it.

The following example shows how to bulk load the GleambookUsers dataset from an external file containing data that has been prepared in ADM (Asterix Data Model) format.

##### Example

     LOAD DATASET GleambookUsers USING localfs
        (("path"="127.0.0.1:///Users/bignosqlfan/tinysocialnew/gbu.adm"),("format"="adm"));

