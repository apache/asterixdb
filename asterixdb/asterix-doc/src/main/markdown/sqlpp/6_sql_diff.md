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
 
SQL++ offers the following additional features beyond SQL-92:

  * Fully composable and functional: A subquery can iterate over any intermediate collection and can appear anywhere in a query.
  * Schema-free: The query language does not assume the existence of a static schema for any data that it processes.
  * Correlated `FROM` terms: A right-side `FROM` term expression can refer to variables defined by `FROM` terms on its left.
  * Powerful `GROUP BY`: In addition to a set of aggregate functions as in standard SQL, the groups created by the `GROUP BY` clause are directly usable in nested queries and/or to obtain nested results.
  * Generalized `SELECT` clause: A `SELECT` clause can return any type of collection, while in SQL-92, a `SELECT` clause has to return a (homogeneous) collection of objects.


The following matrix is a quick "SQL-92 compatibility cheat sheet" for SQL++.

| Feature |  SQL++ | SQL-92 |  Why different?  |
|----------|--------|-------|------------------|
| SELECT * | Returns nested objects | Returns flattened concatenated objects | Nested collections are 1st class citizens |
| SELECT list | order not preserved | order preserved | Fields in a JSON object are not ordered |
| Subquery | Returns a collection  | The returned collection is cast into a scalar value if the subquery appears in a SELECT list or on one side of a comparison or as input to a function | Nested collections are 1st class citizens |
| LEFT OUTER JOIN |  Fills in `MISSING`(s) for non-matches  |   Fills in `NULL`(s) for non-matches    | "Absence" is more appropriate than "unknown" here  |
| UNION ALL       | Allows heterogeneous inputs and output | Input streams must be UNION-compatible and output field names are drawn from the first input stream | Heterogenity and nested collections are common |
| IN constant_expr | The constant expression has to be an array or multiset, i.e., [..,..,...] | The constant collection can be represented as comma-separated items in a paren pair | Nested collections are 1st class citizens |
| String literal | Double quotes or single quotes | Single quotes only | Double quoted strings are pervasive in JSON|
| Delimited identifiers | Backticks | Double quotes | Double quoted strings are pervasive in JSON |

The following SQL-92 features are not implemented yet. However, SQL++ does not conflict with these features:

  * CROSS JOIN, NATURAL JOIN, UNION JOIN
  * RIGHT and FULL OUTER JOIN
  * INTERSECT, EXCEPT, UNION with set semantics
  * CAST expression
  * COALESCE expression
  * ALL and SOME predicates for linking to subqueries
  * UNIQUE predicate (tests a collection for duplicates)
  * MATCH predicate (tests for referential integrity)
  * Row and Table constructors
  * Preserved order for expressions in a SELECT list


