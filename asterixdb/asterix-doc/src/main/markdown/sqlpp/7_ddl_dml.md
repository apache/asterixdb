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

## <a id="Lifecycle_management_statements">Lifecycle Management Statements</a>

### <a id="Use">Use Statement</a>
---

### UseStmnt
**![](../images/diagrams/UseStmnt.png)**


---

At the uppermost level, the world of data is organized into data namespaces called **dataverses**.
To set the default dataverse for statements, the `USE` statement is provided.

As an example, the following statement sets the default dataverse to be `Commerce`.

	USE Commerce;

### <a id="Sets"> Set Statement</a>
The `SET` statement can be used to override certain configuration parameters. More information about `SET` can be found in [Appendix 2](#Performance_tuning).

### <a id="Functions"> Function Declaration</a>

When writing a complex query, it can sometimes be helpful to define one or more auxiliary functions that each address a sub-piece of the overall query. 

The `DECLARE FUNCTION` statement supports the creation of such helper functions.
In general, the function body (expression) can be any legal query expression.

The function named in the `DECLARE FUNCTION` statement is accessible only in the current query. To create a persistent function for use in multiple queries, use the `CREATE FUNCTION` statement.

---
### FunctionDeclaration
**![](../images/diagrams/FunctionDeclaration.png)**

### ParameterList
**![](../images/diagrams/ParameterList.png)**

---
The following is a simple example of a temporary function definition and its use.

##### Example

    DECLARE FUNCTION nameSearch(customerId){
		(SELECT c.custid, c.name
		FROM customers AS c
		WHERE c.custid = customerId)[0]
     };


	SELECT VALUE nameSearch("C25");

For our sample data set, this returns:

    [
      { "custid": "C25", "name": "M. Sinclair" }
    ]

### <a id="Create"> Create Statement</a>
---
### CreateStmnt
**![](../images/diagrams/CreateStmnt.png)**

### QualifiedName
**![](../images/diagrams/QualifiedName.png)**

### DoubleQualifiedName
**![](../images/diagrams/DoubleQualifiedName.png)**

---

The `CREATE` statement is used for creating dataverses as well as other persistent artifacts in a dataverse.
It can be used to create new dataverses, datatypes, datasets, indexes, and user-defined query functions.

#### <a id="Dataverses"> Create Dataverse</a>

---
### CreateDataverse
**![](../images/diagrams/CreateDataverse.png)**

---
The `CREATE DATAVERSE` statement is used to create new dataverses.
To ease the authoring of reusable query scripts, an optional `IF NOT EXISTS` clause is included to allow
creation to be requested either unconditionally or only if the dataverse does not already exist.
If this clause is absent, an error is returned if a dataverse with the indicated name already exists.

The following example creates a new dataverse named `Commerce` if one does not already exist.

##### Example

    CREATE DATAVERSE Commerce IF NOT EXISTS;

#### <a id="Types"> Create Type </a>
---
### CreateType
**![](../images/diagrams/CreateType.png)**

### ObjectTypeDef
**![](../images/diagrams/ObjectTypeDef.png)**

### ObjectField
**![](../images/diagrams/ObjectField.png)**

### TypeExpr
**![](../images/diagrams/TypeExpr.png)**

### ArrayTypeDef
**![](../images/diagrams/ArrayTypeDef.png)**

### MultisetTypeDef
**![](../images/diagrams/MultisetTypeDef.png)**

### TypeRef
**![](../images/diagrams/TypeRef.png)**

---

The `CREATE TYPE` statement is used to create a new named datatype.
This type can then be used to create stored collections or utilized when defining one or more other datatypes.
Much more information about the data model is available in the [data model reference guide](../datamodel.html).
A new type can be a object type, a renaming of another type, an array type, or a multiset type.
A object type can be defined as being either open or closed.
Instances of a closed object type are not permitted to contain fields other than those specified in the create type statement.
Instances of an open object type may carry additional fields, and open is the default for new types if neither option is specified.

The following example creates three new object type called `addressType` ,  `customerType` and `itemType`.
Their fields are essentially traditional typed name/value pairs (much like SQL fields).
Since it is defined as (defaulting to) being an open type, instances will be permitted to contain more than what is specified in the type definition. Indeed many of the customer objects contain a rating as well, however this is not necessary for the customer object to be created. As can be seen in the sample data, customers can exist without ratings or with part (or all) of the address missing. 

##### Example

	CREATE TYPE addressType AS {
	    street:			string,
	    city:			string,
	    zipcode:			string?
	};

    CREATE TYPE customerType AS {
        custid:			string,
        name:			string,
        address:			addressType?
    };

	CREATE TYPE itemType AS {
	    itemno:			int,
	    qty:			int,
	    price:			int
	};

Optionally, you may wish to create a type that has an automatically generated primary key field. The example below shows an alternate form of `itemType` which achieves this by setting its primary key, `itemno`, to UUID. (Refer to the Datasets section later for more details on such fields.)

##### Example
	CREATE TYPE itemType AS {
	    itemno:			uuid,
	    qty:			int,
	    price:			int
	};

Note that the type of the `itemno` in this example is UUID. This field type can be used if you want to have an autogenerated-PK field. (Refer to the Datasets section later for more details on such fields.)

The next example creates a new object type, closed this time, called `orderType`.
Instances of this closed type will not be permitted to have extra fields,
although the `ship_date` field is marked as optional and may thus be `NULL` or `MISSING` in legal instances of the type. The items field is an array of instances of another object type, `itemType`. 

##### Example

	CREATE TYPE orderType AS CLOSED {
	    orderno:			int,
	    custid:			string,
	    order_date:			string,
	    ship_date:			string?,
	    items:			[ itemType ]
	};

#### <a id="Datasets"> Create Dataset</a>

---
### CreateDataset
**![](../images/diagrams/CreateDataset.png)**

### CreateInternalDataset
**![](../images/diagrams/CreateInternalDataset.png)**

### CreateExternalDataset
**![](../images/diagrams/CreateExternalDataset.png)**

### AdapterName
**![](../images/diagrams/AdapterName.png)**

### Configuration
**![](../images/diagrams/Configuration.png)**

### KeyValuePair
**![](../images/diagrams/KeyValuePair.png)**

### Properties
**![](../images/diagrams/Properties.png)**

### PrimaryKey
**![](../images/diagrams/PrimaryKey.png)**

### NestedField
**![](../images/diagrams/NestedField.png)**

### CompactionPolicy
**![](../images/diagrams/CompactionPolicy.png)**

---

The `CREATE DATASET` statement is used to create a new dataset.
Datasets are named, multisets of object type instances;
they are where data lives persistently and are the usual targets for queries.
Datasets are typed, and the system ensures that their contents conform to their type definitions.
An Internal dataset (the default kind) is a dataset whose content lives within and is managed by the system.
It is required to have a specified unique primary key field which uniquely identifies the contained objects.
(The primary key is also used in secondary indexes to identify the indexed primary data objects.)

Internal datasets contain several advanced options that can be specified when appropriate.
One such option is that random primary key (UUID) values can be auto-generated by declaring the field to be UUID and putting `AUTOGENERATED` after the `PRIMARY KEY` identifier.
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
(Refer to [Filter-Based LSM Index Acceleration](../sqlpp/filters.html) for more information about filters.)

An External dataset, in contrast to an Internal dataset, has data stored outside of the system's control.
Files living in HDFS or in the local filesystem(s) of a cluster's nodes are currently supported.
External dataset support allows queries to treat foreign data as though it were stored in the system,
making it possible to query "legacy" file data (for example, Hive data) without having to physically import it.
When defining an External dataset, an appropriate adapter type must be selected for the desired external data.
(See the [Guide to External Data](../aql/externaldata.html) for more information on the available adapters.)

The following example creates an Internal dataset for storing FacefookUserType objects.
It specifies that their id field is their primary key.

#### Example

    CREATE INTERNAL DATASET customers(customerType) PRIMARY KEY custid;

The next example creates an Internal dataset (the default kind when no dataset kind is specified) for storing `itemType` objects might look like. It specifies that the `itemno` field should be used as the primary key for the dataset.
It also specifies that the `itemno` field is an auto-generated field, meaning that a randomly generated UUID value should be assigned to each incoming object by the system. (A user should therefore not attempt to provide a value for this field.)

Note that the `itemno` field's declared type must be UUID in this case.

#### Example

    CREATE DATASET MyItems(itemType) PRIMARY KEY itemno AUTOGENERATED;

The next example creates an External dataset for querying LineItemType objects.
The choice of the `hdfs` adapter means that this dataset's data actually resides in HDFS.
The example `CREATE` statement also provides parameters used by the hdfs adapter:
the URL and path needed to locate the data in HDFS and a description of the data format.

#### Example

    CREATE EXTERNAL DATASET LineItem(LineItemType) USING hdfs (
      ("hdfs"="hdfs://HOST:PORT"),
      ("path"="HDFS_PATH"),
      ("input-format"="text-input-format"),
      ("format"="delimited-text"),
      ("delimiter"="|"));

#### <a id="Indices">Create Index</a>

---
### CreateIndex
**![](../images/diagrams/CreateIndex.png)**

### CreateSecondaryIndex
**![](../images/diagrams/CreateSecondaryIndex.png)**

### CreatePrimaryKeyIndex
**![](../images/diagrams/CreatePrimaryKeyIndex.png)**

### IndexField
**![](../images/diagrams/IndexField.png)**

### NestedField
**![](../images/diagrams/NestedField.png)**

### IndexType
**![](../images/diagrams/IndexType.png)**


---

The `CREATE INDEX` statement creates a secondary index on one or more fields of a specified dataset.
Supported index types include `BTREE` for totally ordered datatypes, `RTREE` for spatial data,
and `KEYWORD` and `NGRAM` for textual (string) data.
An index can be created on a nested field (or fields) by providing a valid path expression as an index field identifier.

An indexed field is not required to be part of the datatype associated with a dataset if the dataset's datatype
is declared as open **and** if the field's type is provided along with its name and if the `ENFORCED` keyword is
specified at the end of the index definition.
`ENFORCING` an open field introduces a check that makes sure that the actual type of the indexed field
(if the optional field exists in the object) always matches this specified (open) field type.

The following example creates a btree index called `cCustIdx` on the `custid` field of the orders dataset.
This index can be useful for accelerating exact-match queries, range search queries, and joins involving the `custid`field.

#### Example

    CREATE INDEX cCustIdx ON orders(custid) TYPE BTREE;

The following example creates an open btree index called `oCreatedTimeIdx` on the (non-declared) `createdTime` field of the `orders` dataset having `datetime` type.
This index can be useful for accelerating exact-match queries, range search queries, and joins involving the `createdTime` field.
The index is enforced so that records that do not have the `createdTime` field or have a mismatched type on the field
cannot be inserted into the dataset.

#### Example

    CREATE INDEX oCreatedTimeIdx ON orders(createdTime: datetime?) TYPE BTREE ENFORCED;

The following example creates an open btree index called `cAddedTimeIdx` on the (non-declared) `addedTime`
field of the `customers` dataset having datetime type.
This index can be useful for accelerating exact-match queries, range search queries,
and joins involving the `addedTime` field.
The index is not enforced so that records that do not have the `addedTime` field or have a mismatched type on the field
can still be inserted into the dataset.

#### Example

    CREATE INDEX cAddedTimeIdx ON customers(addedTime: datetime?);

The following example creates a btree index called `oOrderUserNameIdx` on `orderUserName`,
a nested field residing within a object-valued user field in the `orders` dataset.
This index can be useful for accelerating exact-match queries, range search queries,
and joins involving the nested `orderUserName` field.
Such nested fields must be singular, i.e., one cannot index through (or on) an array-valued field.

#### Example

    CREATE INDEX oOrderUserNameIdx ON orders(order.orderUserName) TYPE BTREE;

The following example creates an open rtree index called `oOrderLocIdx` on the order-location field of the `orders` dataset. This index can be useful for accelerating queries that use the [`spatial-intersect` function](builtins.html#spatial_intersect) in a predicate involving the sender-location field.

#### Example

    CREATE INDEX oOrderLocIDx ON orders(`order-location` : point?) TYPE RTREE ENFORCED;

The following example creates a 3-gram index called `cUserIdx` on the name field of the `customers` dataset. This index can be used to accelerate some similarity or substring maching queries on the name field. For details refer to the document on [similarity queries](similarity.html#NGram_Index).

#### Example

    CREATE INDEX cUserIdx ON customers(name) TYPE NGRAM(3);

The following example creates a keyword index called `oCityIdx` on the `city` within the `address` field of the `customers` dataset. This keyword index can be used to optimize queries with token-based similarity predicates on the `address` field. For details refer to the document on [similarity queries](similarity.html#Keyword_Index).

#### Example

    CREATE INDEX oCityIdx ON customers(address.city) TYPE KEYWORD;

The following example creates a special secondary index which holds only the primary keys.
This index is useful for speeding up aggregation queries which involve only primary keys.
The name of the index is optional. If the name is not specified, the system will generate
one. When the user would like to drop this index, the metadata can be queried to find the system-generated name.

#### Example

    CREATE PRIMARY INDEX cus_pk_idx ON customers;

An example query that can be accelerated using the primary-key index:

    SELECT COUNT(*) FROM customers;

To look up the the above primary-key index, issue the following query:

    SELECT VALUE i
    FROM Metadata.`Index` i
    WHERE i.DataverseName = "Commerce" AND i.DatasetName = "customers";

The query returns:

	[
	    {
	        "DataverseName": "Commerce",
	        "DatasetName": "customers",
	        "IndexName": "cus_pk_idx",
	        "IndexStructure": "BTREE",
	        "SearchKey": [],
	        "IsPrimary": false,
	        "Timestamp": "Fri Sep 18 14:15:51 PDT 2020",
	        "PendingOp": 0
	    },
	    {
	        "DataverseName": "Commerce",
	        "DatasetName": "customers",
	        "IndexName": "customers",
	        "IndexStructure": "BTREE",
	        "SearchKey": [
	            [
	                "custid"
	            ]
	        ],
	        "IsPrimary": true,
	        "Timestamp": "Thu Jul 16 13:07:37 PDT 2020",
	        "PendingOp": 0
	    }
	]

Remember that `CREATE PRIMARY INDEX` creates a secondary index.
That is the reason the `IsPrimary` field is false.
The primary-key index can be identified by the fact that the `SearchKey` field is empty since it only contains primary key fields.

#### <a id="Synonyms"> Create Synonym</a>

---
### CreateSynonym
**![](../images/diagrams/CreateSynonym.png)**


---

The `CREATE SYNONYM` statement creates a synonym for a given dataset.
This synonym may be used used instead of the dataset name in `SELECT`, `INSERT`, `UPSERT`, `DELETE`, and `LOAD` statements.
The target dataset does not need to exist when the synonym is created.

##### Example

    CREATE DATASET customers(customersType) PRIMARY KEY custid;

    CREATE SYNONYM customersSynonym FOR customers;

    SELECT * FROM customersSynonym;

More information on how synonyms are resolved can be found in the appendix section on Variable Resolution.

#### <a id="Create_function">Create Function</a>

The `CREATE FUNCTION` statement creates a **named** function that can then be used and reused in queries.
The body of a function can be any query expression involving the function's parameters.

---
### CreateFunction
**![](../images/diagrams/CreateFunction.png)**

### FunctionParameters
**![](../images/diagrams/FunctionParameters.png)**


---
The following is an example of a `CREATE FUNCTION` statement which is similar to our earlier `DECLARE FUNCTION` example.

It differs from that example in that it results in a function that is persistently registered by name in the specified dataverse (the current dataverse being used, if not otherwise specified).

##### Example

    CREATE FUNCTION nameSearch(customerId) {
        (SELECT c.custid, c.name
         FROM customers AS c
         WHERE u.custid = customerId)[0]
     };

The following is an example of CREATE FUNCTION statement that replaces an existing function.

##### Example

    CREATE OR REPLACE FUNCTION friendInfo(userId) {
        (SELECT u.id, u.name
         FROM GleambookUsers u
         WHERE u.id = userId)[0]
     };

External functions can also be loaded into Libraries via the [UDF API](../udf.html). Given
an already loaded library `pylib`, a function `sentiment` mapping to a Python method `sent_model.sentiment` in `sentiment_mod`
would be as follows

##### Example

    CREATE FUNCTION sentiment(a)
      AS "sentiment_mod", "sent_model.sentiment" AT pylib;

### <a id="Removal">Drop Statement</a>

---
### DropStmnt
**![](../images/diagrams/DropStmnt.png)**

### FunctionSignature
**![](../images/diagrams/FunctionSignature.png)**

---

The `DROP` statement is the inverse of the `CREATE` statement. It can be used to drop dataverses, datatypes, datasets, indexes, functions, and synonyms.

The following examples illustrate some uses of the `DROP` statement.

##### Example

    DROP DATASET customers IF EXISTS;

    DROP INDEX orders.orderidIndex;

    DROP TYPE customers2.customersType;

    DROP FUNCTION nameSearch@1;

    DROP SYNONYM customersSynonym;

    DROP DATAVERSE CommerceData;

When an artifact is dropped, it will be droppped from the current dataverse if none is specified
(see the `DROP DATASET` example above) or from the specified dataverse (see the `DROP TYPE` example above)
if one is specified by fully qualifying the artifact name in the `DROP` statement.
When specifying an index to drop, the index name must be qualified by the dataset that it indexes.
When specifying a function to drop, since the query language allows functions to be overloaded by their number of arguments,
the identifying name of the function to be dropped must explicitly include that information.
(`nameSearch@1` above denotes the 1-argument function named nameSearch in the current dataverse.)

### <a id="Load_statement">Load Statement</a>

---
### LoadStmnt
**![](../images/diagrams/LoadStmnt.png)**

### Configuration
**![](../images/diagrams/Configuration.png)**

### KeyValuePair
**![](../images/diagrams/KeyValuePair.png)**

---

The `LOAD` statement is used to initially populate a dataset via bulk loading of data from an external file.
An appropriate adapter must be selected to handle the nature of the desired external data.
The `LOAD` statement accepts the same adapters and the same parameters as discussed earlier for External datasets.
(See the [guide to external data](../aql/externaldata.html) for more information on the available adapters.)
If a dataset has an auto-generated primary key field, the file to be imported should not include that field in it.

The target dataset name may be a synonym introduced by `CREATE SYNONYM` statement.

The following example shows how to bulk load the `customers` dataset from an external file containing data that has been prepared in ADM (Asterix Data Model) format.

##### Example

     LOAD DATASET customers USING localfs
        (("path"="127.0.0.1:///Users/bignosqlfan/commercenew/gbu.adm"),("format"="adm"));

## <a id="Modification_statements">Modification statements</a>

### <a id="Inserts">Insert Statement</a>

---
### InsertStmnt
**![](../images/diagrams/InsertStmnt.png)**


---

The `INSERT` statement is used to insert new data into a dataset.
The data to be inserted comes from a query expression.
This expression can be as simple as a constant expression, or in general it can be any legal query.
In case the dataset has an auto-generated primary key, when performing an `INSERT` operation, the system allows the user to manually add the
auto-generated key field in the `INSERT` statement, or skip that field and the system will automatically generate it and add it. However,
it is important to note that if the a record already exists in the dataset with the auto-generated key provided by the user, then
that operation is going to fail. As a general rule, insertion will fail if the dataset already has data with the primary key value(s)
being inserted.

Inserts are processed transactionally by the system.
The transactional scope of each insert transaction is the insertion of a single object plus its affiliated secondary index entries (if any).
If the query part of an insert returns a single object, then the `INSERT` statement will be a single, atomic transaction.
If the query part returns multiple objects, each object being inserted will be treated as a separate tranaction.

The target dataset name may be a synonym introduced by `CREATE SYNONYM` statement.

The following example illustrates a query-based insertion.

##### Example

    INSERT INTO custCopy (SELECT VALUE c FROM customers c)

### <a id="Upserts">Upsert Statement</a>

---
### UpsertStmnt
**![](../images/diagrams/UpsertStmnt.png)**

---

The `UPSERT` statement syntactically mirrors the `INSERT `statement discussed above.
The difference lies in its semantics, which for `UPSERT` are "add or replace" instead of the `INSERT` "add if not present, else error" semantics.
Whereas an `INSERT` can fail if another object already exists with the specified key, the analogous `UPSERT` will replace the previous object's value
with that of the new object in such cases. Like the `INSERT` statement, the system allows the user to manually provide the auto-generated key
for datasets with an auto-generated key as its primary key. This operation will insert the record if no record with that key already exists, but
if a record with the key already exists, then the operation will be converted to a replace/update operation.

The target dataset name may be a synonym introduced by `CREATE SYNONYM` statement.

The following example illustrates a query-based upsert operation.

##### Example

    UPSERT INTO custCopy (SELECT VALUE c FROM customers c)

### <a id="Deletes">Delete Statement</a>
---
### DeleteStmnt
**![](../images/diagrams/DeleteStmnt.png)**


---
The `DELETE` statement is used to delete data from a target dataset.
The data to be deleted is identified by a boolean expression involving the variable bound to the target dataset in the `DELETE` statement.

Deletes are processed transactionally by the system.
The transactional scope of each delete transaction is the deletion of a single object plus its affiliated secondary index entries (if any).
If the boolean expression for a delete identifies a single object, then the `DELETE` statement itself will be a single, atomic transaction.
If the expression identifies multiple objects, then each object deleted will be handled as a separate transaction.

The target dataset name may be a synonym introduced by `CREATE SYNONYM` statement.

The following examples illustrate single-object deletions.

##### Example

    DELETE FROM customers c WHERE c.custid = "C41";

##### Example

    DELETE FROM customers WHERE custid = "C47";


