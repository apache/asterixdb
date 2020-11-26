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

(Q4.1)

    customers AS c
	SELECT *

Since the queryhas no `FROM` keyword before the dataset `customers`,
we will get a syntax error as follows:

    ERROR: Code: 1 "ASX1001: Syntax error: In line 2 >>customers AS c<< Encountered \"AS\" at column 11. "

##### Example

(Q4.2)

     FROM customers AS c
	 WHERE type="advertiser"
	 SELECT *;

Since "type" is a reserved keyword in the query parser,
we will get a syntax error as follows:

    ERROR: Code: 1 "ASX1001: Syntax error: In line 3 >> WHERE type=\"advertiser\"<< Encountered \"type\" at column 8. ";


## <a id="Identifier_resolution_errors">Identifier Resolution Errors</a>
Referring to an undefined identifier can cause an error if the identifier
cannot be successfully resolved as a valid field access.

##### Example
(Q4.3)

     FROM customer AS c
	 SELECT *

If we have a typo as above in "customers" that misses the dataset name's ending "s",
we will get an identifier resolution error as follows:

    ERROR: Code: 1 "ASX1077: Cannot find dataset customer in dataverse Commerce nor an alias with name customer! (in line 2, at column 7)"

##### Example
(Q4.4)

     FROM customers AS c JOIN orders AS o ON c.custid = o.custid
	 SELECT name, orderno;

If the compiler cannot figure out how to resolve an unqualified field name, which will occur if there is more than one variable in scope (e.g., `customers AS c` and `orders AS o` as above),
we will get an identifier resolution error as follows:

    ERROR: Code: 1 "ASX1074: Cannot resolve ambiguous alias reference for identifier name (in line 3, at column 9)"

The same can happen when failing to properly identify the `GROUP BY` expression. 

(Q4.5)

	SELECT o.custid, COUNT(o.orderno) AS `order count`
	FROM orders AS o
	GROUP BY custid;

Result:

	ERROR: Code: 1 "ASX1073: Cannot resolve alias reference for undefined identifier o (in line 2, at column 8)"

## <a id="Type_errors">Type Errors</a>

The query compiler does type checks based on its available type information.
In addition, the query runtime also reports type errors if a data model instance
it processes does not satisfy the type requirement.

##### Example
(Q4.6)

    get_day(10/11/2020);

Since function `get_day` can only process duration, daytimeduration, date, or datetime input values,
we will get a type error as follows: 

    ERROR: Code: 1 "ASX0002: Type mismatch: function get-day expects its 1st input parameter to be of type duration, daytimeduration, date or datetime, but the actual input type is double (in line 2, at column 1)"


## <a id="Resource_errors">Resource Errors</a>
A query can potentially exhaust system resources, such
as the number of open files and disk spaces.
For instance, the following two resource errors could be potentially
be seen when running the system:

    Error: no space left on device
    Error: too many open files

The "no space left on device" issue usually can be fixed by
cleaning up disk space and reserving more disk space for the system.
The "too many open files" issue usually can be fixed by a system
administrator, following the instructions
[here](https://easyengine.io/tutorials/linux/increase-open-files-limit/).

