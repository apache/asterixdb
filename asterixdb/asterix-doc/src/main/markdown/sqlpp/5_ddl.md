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

# <a id="DDL_and_DML_statements">4. DDL and DML statements</a>

    Statement ::= ( SingleStatement ( ";" )? )* <EOF>
    SingleStatement ::= DatabaseDeclaration
                      | FunctionDeclaration
                      | CreateStatement
                      | DropStatement
                      | LoadStatement
                      | SetStatement
                      | InsertStatement
                      | DeleteStatement
                      | Query ";"

In addition to queries, an implementation of SQL++ needs to support statements for data definition
and manipulation purposes as well as controlling the context to be used in evaluating SQL++ expressions.
This section details the DDL and DML statements supported in the SQL++ language as realized today in
Apache AsterixDB.

## <a id="Declarations">Declarations</a>

    DatabaseDeclaration ::= "USE" Identifier

At the uppermost level, the world of data is organized into data namespaces called **dataverses**.
To set the default dataverse for a series of statements, the USE statement is provided in SQL++.

As an example, the following statement sets the default dataverse to be "TinySocial".

##### Example

    USE TinySocial;

When writing a complex SQL++ query, it can sometimes be helpful to define one or more auxilliary functions
that each address a sub-piece of the overall query.
The declare function statement supports the creation of such helper functions.
In general, the function body (expression) can be any legal SQL++ query expression.

    FunctionDeclaration  ::= "DECLARE" "FUNCTION" Identifier ParameterList "{" Expression "}"
    ParameterList        ::= "(" ( <VARIABLE> ( "," <VARIABLE> )* )? ")"

The following is a simple example of a temporary SQL++ function definition and its use.

##### Example

    DECLARE FUNCTION friendInfo(userId) {
        (SELECT u.id, u.name, len(u.friendIds) AS friendCount
         FROM GleambookUsers u
         WHERE u.id = userId)[0]
     };

    SELECT VALUE friendInfo(2);

For our sample data set, this returns:

    [
      { "id": 2, "name": "IsbelDull", "friendCount": 2 }
    ]

## <a id="Lifecycle_management_statements">Lifecycle management statements</a>

    CreateStatement ::= "CREATE" ( DatabaseSpecification
                                 | TypeSpecification
                                 | DatasetSpecification
                                 | IndexSpecification
                                 | FunctionSpecification )

    QualifiedName       ::= Identifier ( "." Identifier )?
    DoubleQualifiedName ::= Identifier "." Identifier ( "." Identifier )?

The CREATE statement in SQL++ is used for creating dataverses as well as other persistent artifacts in a dataverse.
It can be used to create new dataverses, datatypes, datasets, indexes, and user-defined SQL++ functions.

### <a id="Dataverses"> Dataverses</a>

    DatabaseSpecification ::= "DATAVERSE" Identifier IfNotExists

The CREATE DATAVERSE statement is used to create new dataverses.
To ease the authoring of reusable SQL++ scripts, an optional IF NOT EXISTS clause is included to allow
creation to be requested either unconditionally or only if the dataverse does not already exist.
If this clause is absent, an error is returned if a dataverse with the indicated name already exists.

The following example creates a new dataverse named TinySocial if one does not already exist.

##### Example

    CREATE DATAVERSE TinySocial IF NOT EXISTS;

### <a id="Types"> Types</a>

    TypeSpecification    ::= "TYPE" FunctionOrTypeName IfNotExists "AS" ObjectTypeDef
    FunctionOrTypeName   ::= QualifiedName
    IfNotExists          ::= ( <IF> <NOT> <EXISTS> )?
    TypeExpr             ::= ObjectTypeDef | TypeReference | ArrayTypeDef | MultisetTypeDef
    ObjectTypeDef        ::= ( <CLOSED> | <OPEN> )? "{" ( ObjectField ( "," ObjectField )* )? "}"
    ObjectField          ::= Identifier ":" ( TypeExpr ) ( "?" )?
    NestedField          ::= Identifier ( "." Identifier )*
    IndexField           ::= NestedField ( ":" TypeReference )?
    TypeReference        ::= Identifier
    ArrayTypeDef         ::= "[" ( TypeExpr ) "]"
    MultisetTypeDef      ::= "{{" ( TypeExpr ) "}}"

The CREATE TYPE statement is used to create a new named datatype.
This type can then be used to create stored collections or utilized when defining one or more other datatypes.
Much more information about the data model is available in the [data model reference guide](datamodel.html).
A new type can be a object type, a renaming of another type, an array type, or a multiset type.
A object type can be defined as being either open or closed.
Instances of a closed object type are not permitted to contain fields other than those specified in the create type statement.
Instances of an open object type may carry additional fields, and open is the default for new types if neither option is specified.

The following example creates a new object type called GleambookUser type.
Since it is defined as (defaulting to) being an open type,
instances will be permitted to contain more than what is specified in the type definition.
The first four fields are essentially traditional typed name/value pairs (much like SQL fields).
The friendIds field is a multiset of integers.
The employment field is an array of instances of another named object type, EmploymentType.

##### Example

    CREATE TYPE GleambookUserType AS {
      id:         int,
      alias:      string,
      name:       string,
      userSince: datetime,
      friendIds: {{ int }},
      employment: [ EmploymentType ]
    };

The next example creates a new object type, closed this time, called MyUserTupleType.
Instances of this closed type will not be permitted to have extra fields,
although the alias field is marked as optional and may thus be NULL or MISSING in legal instances of the type.
Note that the type of the id field in the example is UUID.
This field type can be used if you want to have this field be an autogenerated-PK field.
(Refer to the Datasets section later for more details on such fields.)

##### Example

    CREATE TYPE MyUserTupleType AS CLOSED {
      id:         uuid,
      alias:      string?,
      name:       string
    };

### <a id="Datasets"> Datasets</a>

    DatasetSpecification ::= ( <INTERNAL> )? <DATASET> QualifiedName "(" QualifiedName ")" IfNotExists
                               PrimaryKey ( <ON> Identifier )? ( <HINTS> Properties )?
                               ( "USING" "COMPACTION" "POLICY" CompactionPolicy ( Configuration )? )?
                               ( <WITH> <FILTER> <ON> Identifier )?
                              |
                               <EXTERNAL> <DATASET> QualifiedName "(" QualifiedName ")" IfNotExists <USING> AdapterName
                               Configuration ( <HINTS> Properties )?
                               ( <USING> <COMPACTION> <POLICY> CompactionPolicy ( Configuration )? )?
    AdapterName          ::= Identifier
    Configuration        ::= "(" ( KeyValuePair ( "," KeyValuePair )* )? ")"
    KeyValuePair         ::= "(" StringLiteral "=" StringLiteral ")"
    Properties           ::= ( "(" Property ( "," Property )* ")" )?
    Property             ::= Identifier "=" ( StringLiteral | IntegerLiteral )
    FunctionSignature    ::= FunctionOrTypeName "@" IntegerLiteral
    PrimaryKey           ::= <PRIMARY> <KEY> NestedField ( "," NestedField )* ( <AUTOGENERATED> )?
    CompactionPolicy     ::= Identifier

The CREATE DATASET statement is used to create a new dataset.
Datasets are named, multisets of object type instances;
they are where data lives persistently and are the usual targets for SQL++ queries.
Datasets are typed, and the system ensures that their contents conform to their type definitions.
An Internal dataset (the default kind) is a dataset whose content lives within and is managed by the system.
It is required to have a specified unique primary key field which uniquely identifies the contained objects.
(The primary key is also used in secondary indexes to identify the indexed primary data objects.)

Internal datasets contain several advanced options that can be specified when appropriate.
One such option is that random primary key (UUID) values can be auto-generated by declaring the field to be UUID and putting "AUTOGENERATED" after the "PRIMARY KEY" identifier.
In this case, unlike other non-optional fields, a value for the auto-generated PK field should not be provided at insertion time by the user since each object's primary key field value will be auto-generated by the system.

Another advanced option, when creating an Internal dataset, is to specify the merge policy to control which of the
underlying LSM storage components to be merged.
(The system supports Log-Structured Merge tree based physical storage for Internal datasets.)
Currently the system supports four different component merging policies that can be chosen per dataset:
no-merge, constant, prefix, and correlated-prefix.
The no-merge policy simply never merges disk components.
The constant policy merges disk components when the number of components reaches a constant number k that can be configured by the user.
The prefix policy relies on both component sizes and the number of components to decide which components to merge.
It works by first trying to identify the smallest ordered (oldest to newest) sequence of components such that the sequence does not contain a single component that exceeds some threshold size M and that either the sum of the component's sizes exceeds M or the number of components in the sequence exceeds another threshold C.
If such a sequence exists, the components in the sequence are merged together to form a single component.
Finally, the correlated-prefix policy is similar to the prefix policy, but it delegates the decision of merging the disk components of all the indexes in a dataset to the primary index.
When the correlated-prefix policy decides that the primary index needs to be merged (using the same decision criteria as for the prefix policy), then it will issue successive merge requests on behalf of all other indexes associated with the same dataset.
The system's default policy is the prefix policy except when there is a filter on a dataset, where the preferred policy for filters is the correlated-prefix.

Another advanced option shown in the syntax above, related to performance and mentioned above, is that a **filter** can optionally be created on a field to further optimize range queries with predicates on the filter's field.
Filters allow some range queries to avoid searching all LSM components when the query conditions match the filter.
(Refer to [Filter-Based LSM Index Acceleration](filters.html) for more information about filters.)

An External dataset, in contrast to an Internal dataset, has data stored outside of the system's control.
Files living in HDFS or in the local filesystem(s) of a cluster's nodes are currently supported.
External dataset support allows SQL++ queries to treat foreign data as though it were stored in the system,
making it possible to query "legacy" file data (e.g., Hive data) without having to physically import it.
When defining an External dataset, an appropriate adapter type must be selected for the desired external data.
(See the [Guide to External Data](externaldata.html) for more information on the available adapters.)

The following example creates an Internal dataset for storing FacefookUserType objects.
It specifies that their id field is their primary key.

#### Example

    CREATE INTERNAL DATASET GleambookUsers(GleambookUserType) PRIMARY KEY id;

The next example creates another Internal dataset (the default kind when no dataset kind is specified) for storing MyUserTupleType objects.
It specifies that the id field should be used as the primary key for the dataset.
It also specifies that the id field is an auto-generated field,
meaning that a randomly generated UUID value should be assigned to each incoming object by the system.
(A user should therefore not attempt to provide a value for this field.)
Note that the id field's declared type must be UUID in this case.

#### Example

    CREATE DATASET MyUsers(MyUserTupleType) PRIMARY KEY id AUTOGENERATED;

The next example creates an External dataset for querying LineItemType objects.
The choice of the `hdfs` adapter means that this dataset's data actually resides in HDFS.
The example CREATE statement also provides parameters used by the hdfs adapter:
the URL and path needed to locate the data in HDFS and a description of the data format.

#### Example

    CREATE EXTERNAL DATASET LineItem(LineItemType) USING hdfs (
      ("hdfs"="hdfs://HOST:PORT"),
      ("path"="HDFS_PATH"),
      ("input-format"="text-input-format"),
      ("format"="delimited-text"),
      ("delimiter"="|"));



#### Indices

    IndexSpecification ::= <INDEX> Identifier IfNotExists <ON> QualifiedName
                           "(" ( IndexField ) ( "," IndexField )* ")" ( "type" IndexType "?")?
                           ( <ENFORCED> )?
    IndexType          ::= <BTREE> | <RTREE> | <KEYWORD> | <NGRAM> "(" IntegerLiteral ")"

The CREATE INDEX statement creates a secondary index on one or more fields of a specified dataset.
Supported index types include `BTREE` for totally ordered datatypes, `RTREE` for spatial data,
and `KEYWORD` and `NGRAM` for textual (string) data.
An index can be created on a nested field (or fields) by providing a valid path expression as an index field identifier.

An indexed field is not required to be part of the datatype associated with a dataset if the dataset's datatype
is declared as open **and** if the field's type is provided along with its name and if the `ENFORCED` keyword is
specified at the end of the index definition.
`ENFORCING` an open field introduces a check that makes sure that the actual type of the indexed field
(if the optional field exists in the object) always matches this specified (open) field type.

The following example creates a btree index called gbAuthorIdx on the authorId field of the GleambookMessages dataset.
This index can be useful for accelerating exact-match queries, range search queries, and joins involving the author-id
field.

#### Example

    CREATE INDEX gbAuthorIdx ON GleambookMessages(authorId) TYPE BTREE;

The following example creates an open btree index called gbSendTimeIdx on the (non-predeclared) sendTime field of the GleambookMessages dataset having datetime type.
This index can be useful for accelerating exact-match queries, range search queries, and joins involving the sendTime field.

#### Example

    CREATE INDEX gbSendTimeIdx ON GleambookMessages(sendTime: datetime?) TYPE BTREE ENFORCED;

The following example creates a btree index called crpUserScrNameIdx on screenName,
a nested field residing within a object-valued user field in the ChirpMessages dataset.
This index can be useful for accelerating exact-match queries, range search queries,
and joins involving the nested screenName field.
Such nested fields must be singular, i.e., one cannot index through (or on) an array-valued field.

#### Example

    CREATE INDEX crpUserScrNameIdx ON ChirpMessages(user.screenName) TYPE BTREE;

The following example creates an rtree index called gbSenderLocIdx on the sender-location field of the GleambookMessages dataset. This index can be useful for accelerating queries that use the [`spatial-intersect` function](functions.html#spatial-intersect) in a predicate involving the sender-location field.

#### Example

    CREATE INDEX gbSenderLocIndex ON GleambookMessages("sender-location") TYPE RTREE;

The following example creates a 3-gram index called fbUserIdx on the name field of the GleambookUsers dataset. This index can be used to accelerate some similarity or substring maching queries on the name field. For details refer to the document on [similarity queries](similarity.html#NGram_Index).

#### Example

    CREATE INDEX fbUserIdx ON GleambookUsers(name) TYPE NGRAM(3);

The following example creates a keyword index called fbMessageIdx on the message field of the GleambookMessages dataset. This keyword index can be used to optimize queries with token-based similarity predicates on the message field. For details refer to the document on [similarity queries](similarity.html#Keyword_Index).

#### Example

    CREATE INDEX fbMessageIdx ON GleambookMessages(message) TYPE KEYWORD;

### <a id="Functions"> Functions</a>

The create function statement creates a **named** function that can then be used and reused in SQL++ queries.
The body of a function can be any SQL++ expression involving the function's parameters.

    FunctionSpecification ::= "FUNCTION" FunctionOrTypeName IfNotExists ParameterList "{" Expression "}"

The following is an example of a CREATE FUNCTION statement which is similar to our earlier DECLARE FUNCTION example.
It differs from that example in that it results in a function that is persistently registered by name in the specified dataverse (the current dataverse being used, if not otherwise specified).

##### Example

    CREATE FUNCTION friendInfo(userId) {
        (SELECT u.id, u.name, len(u.friendIds) AS friendCount
         FROM GleambookUsers u
         WHERE u.id = userId)[0]
     };

#### Removal

    DropStatement       ::= "DROP" ( "DATAVERSE" Identifier IfExists
                                   | "TYPE" FunctionOrTypeName IfExists
                                   | "DATASET" QualifiedName IfExists
                                   | "INDEX" DoubleQualifiedName IfExists
                                   | "FUNCTION" FunctionSignature IfExists )
    IfExists            ::= ( "IF" "EXISTS" )?

The DROP statement in SQL++ is the inverse of the CREATE statement. It can be used to drop dataverses, datatypes, datasets, indexes, and functions.

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
When specifying a function to drop, since SQL++ allows functions to be overloaded by their number of arguments,
the identifying name of the function to be dropped must explicitly include that information.
(`friendInfo@1` above denotes the 1-argument function named friendInfo in the current dataverse.)

### Import/Export Statements

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

## <a id="Modification_statements">Modification statements</a>

### <a id="Inserts">INSERTs</a>

    InsertStatement ::= <INSERT> <INTO> QualifiedName Query

The SQL++ INSERT statement is used to insert new data into a dataset.
The data to be inserted comes from a SQL++ query expression.
This expression can be as simple as a constant expression, or in general it can be any legal SQL++ query.
If the target dataset has an auto-generated primary key field, the insert statement should not include a
value for that field in it.
(The system will automatically extend the provided object with this additional field and a corresponding value.)
Insertion will fail if the dataset already has data with the primary key value(s) being inserted.

Inserts are processed transactionally by the system.
The transactional scope of each insert transaction is the insertion of a single object plus its affiliated secondary index entries (if any).
If the query part of an insert returns a single object, then the INSERT statement will be a single, atomic transaction.
If the query part returns multiple objects, each object being inserted will be treated as a separate tranaction.
The following example illustrates a query-based insertion.

##### Example

    INSERT INTO UsersCopy (SELECT VALUE user FROM GleambookUsers user)

### <a id="Upserts">UPSERTs</a>

    UpsertStatement ::= <UPSERT> <INTO> QualifiedName Query

The SQL++ UPSERT statement syntactically mirrors the INSERT statement discussed above.
The difference lies in its semantics, which for UPSERT are "add or replace" instead of the INSERT "add if not present, else error" semantics.
Whereas an INSERT can fail if another object already exists with the specified key, the analogous UPSERT will replace the previous object's value with that of the new object in such cases.

The following example illustrates a query-based upsert operation.

##### Example

    UPSERT INTO UsersCopy (SELECT VALUE user FROM GleambookUsers user)

*Editor's note: Upserts currently work in AQL but are not yet enabled (at the moment) in SQL++.

### <a id="Deletes">DELETEs</a>

    DeleteStatement ::= <DELETE> <FROM> QualifiedName ( ( <AS> )? Variable )? ( <WHERE> Expression )?

The SQL++ DELETE statement is used to delete data from a target dataset.
The data to be deleted is identified by a boolean expression involving the variable bound to the target dataset in the DELETE statement.

Deletes are processed transactionally by the system.
The transactional scope of each delete transaction is the deletion of a single object plus its affiliated secondary index entries (if any).
If the boolean expression for a delete identifies a single object, then the DELETE statement itself will be a single, atomic transaction.
If the expression identifies multiple objects, then each object deleted will be handled as a separate transaction.

The following examples illustrate single-object deletions.

##### Example

    DELETE FROM GleambookUsers user WHERE user.id = 8;

##### Example

    DELETE FROM GleambookUsers WHERE id = 5;

