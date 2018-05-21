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

## <a id="Index_Only">Controlling Index-Only-Plan Parameter</a>
By default, the system tries to build an index-only plan whenever utilizing a secondary index is possible.
For example, if a SELECT or JOIN query can utilize an enforced B+Tree or R-Tree index on a field, the optimizer
checks whether a secondary-index search alone can generate the result that the query asks for. It
mainly checks two conditions: (1) predicates used in WHERE only uses the primary key field and/or secondary key field
and (2) the result does not return any other fields. If these two conditions hold, it builds an index-only plan.
Since an index-only plan only searches a secondary-index to answer a query, it is faster than
a non-index-only plan that needs to search the primary index.
However, this index-only plan can be turned off per query by setting the following parameter.

*  **noindexonly**: if this is set to true, the index-only-plan will not be applied; the default value is false.

##### Example

    SET noindexonly 'true';

    SELECT m.message AS message
    FROM GleambookMessages m where m.message = " love product-b its shortcut-menu is awesome:)";
