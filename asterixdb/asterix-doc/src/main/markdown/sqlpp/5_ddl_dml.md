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

## <a id="Modification_statements">Modification statements</a>

### <a id="Inserts">INSERTs</a>

    InsertStatement ::= <INSERT> <INTO> QualifiedName Query

The INSERT statement is used to insert new data into a dataset.
The data to be inserted comes from a query expression.
This expression can be as simple as a constant expression, or in general it can be any legal query.
In case the dataset has an auto-generated primary key, when performing an INSERT operation, the system allows the user to manually add the
auto-generated key field in the INSERT statement, or skip that field and the system will automatically generate it and add it. However,
it is important to note that if the a record already exists in the dataset with the auto-generated key provided by the user, then
that operation is going to fail. As a general rule, insertion will fail if the dataset already has data with the primary key value(s)
being inserted.

Inserts are processed transactionally by the system.
The transactional scope of each insert transaction is the insertion of a single object plus its affiliated secondary index entries (if any).
If the query part of an insert returns a single object, then the INSERT statement will be a single, atomic transaction.
If the query part returns multiple objects, each object being inserted will be treated as a separate tranaction.
The following example illustrates a query-based insertion.

##### Example

    INSERT INTO UsersCopy (SELECT VALUE user FROM GleambookUsers user)

### <a id="Upserts">UPSERTs</a>

    UpsertStatement ::= <UPSERT> <INTO> QualifiedName Query

The UPSERT statement syntactically mirrors the INSERT statement discussed above.
The difference lies in its semantics, which for UPSERT are "add or replace" instead of the INSERT "add if not present, else error" semantics.
Whereas an INSERT can fail if another object already exists with the specified key, the analogous UPSERT will replace the previous object's value
with that of the new object in such cases. Like the INSERT statement, the system allows the user to manually provide the auto-generated key
for datasets with an auto-generated key as its primary key. This operation will insert the record if no record with that key already exists, but
if a record with the key already exists, then the operation will be converted to a replace/update operation.

The following example illustrates a query-based upsert operation.

##### Example

    UPSERT INTO UsersCopy (SELECT VALUE user FROM GleambookUsers user)

*Editor's note: Upserts currently work in AQL but are not yet enabled (at the moment) in the current query language.

### <a id="Deletes">DELETEs</a>

    DeleteStatement ::= <DELETE> <FROM> QualifiedName ( ( <AS> )? Variable )? ( <WHERE> Expression )?

The DELETE statement is used to delete data from a target dataset.
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

