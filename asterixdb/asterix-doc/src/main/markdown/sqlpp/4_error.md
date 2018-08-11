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

A query can potentially result in one of the following errors:

 * syntax error,
 * identifier resolution error,
 * type error,
 * resource error.

If the query processor runs into any error, it will
terminate the ongoing processing of the query and
immediately return an error message to the client.

## <a id="Syntax_errors">Syntax Errors</a>
A valid query must satisfy the grammar rules of the query language.
Otherwise, a syntax error will be raised.

##### Example

    SELECT *
    GleambookUsers user

Since the query misses a `FROM` keyword before the dataset `GleambookUsers`,
we will get a syntax error as follows:

    Syntax error: In line 2 >>GleambookUsers user;<< Encountered <IDENTIFIER> \"GleambookUsers\" at column 1.

##### Example

    SELECT *
    FROM GleambookUsers user
    WHERE type="advertiser";

Since "type" is a reserved keyword in the query parser,
we will get a syntax error as follows:

    Error: Syntax error: In line 3 >>WHERE type="advertiser";<< Encountered 'type' "type" at column 7.
    ==> WHERE type="advertiser";


## <a id="Identifier_resolution_errors">Identifier Resolution Errors</a>
Referring to an undefined identifier can cause an error if the identifier
cannot be successfully resolved as a valid field access.

##### Example

    SELECT *
    FROM GleambookUser user;

If we have a typo as above in "GleambookUsers" that misses the dataset name's ending "s",
we will get an identifier resolution error as follows:

    Error: Cannot find dataset GleambookUser in dataverse Default nor an alias with name GleambookUser!

##### Example

    SELECT name, message
    FROM GleambookUsers u JOIN GleambookMessages m ON m.authorId = u.id;

If the compiler cannot figure out how to resolve an unqualified field name, which will occur if there is more than one variable in scope (e.g., `GleambookUsers u` and `GleambookMessages m` as above),
we will get an identifier resolution error as follows:

    Error: Cannot resolve ambiguous alias reference for undefined identifier name


## <a id="Type_errors">Type Errors</a>

The query compiler does type checks based on its available type information.
In addition, the query runtime also reports type errors if a data model instance
it processes does not satisfy the type requirement.

##### Example

    abs("123");

Since function `abs` can only process numeric input values,
we will get a type error as follows:

    Error: Type mismatch: function abs expects its 1st input parameter to be of type tinyint, smallint, integer, bigint, float or double, but the actual input type is string


## <a id="Resource_errors">Resource Errors</a>
A query can potentially exhaust system resources, such
as the number of open files and disk spaces.
For instance, the following two resource errors could be potentially
be seen when running the system:

    Error: no space left on device
    Error: too many open files

The "no space left on device" issue usually can be fixed by
cleaning up disk spaces and reserving more disk spaces for the system.
The "too many open files" issue usually can be fixed by a system
administrator, following the instructions
[here](https://easyengine.io/tutorials/linux/increase-open-files-limit/).

