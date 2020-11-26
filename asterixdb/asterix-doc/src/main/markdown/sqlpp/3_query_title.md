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

# <a id="Queries">3. Queries</a>

A *query* can be an expression, or it can be constructed from blocks of code called *query blocks*. A query block may contain several clauses, including `SELECT`, `FROM`, `LET`, `WHERE`, `GROUP BY`, and `HAVING`. 

---
### Query
**![](../images/diagrams/Query.png)**

### Selection
**![](../images/diagrams/Selection.png)**

### QueryBlock
**![](../images/diagrams/QueryBlock.png)**

### StreamGenerator
**![](../images/diagrams/StreamGenerator.png)**


---

Note that, unlike SQL, SQL++ allows the `SELECT` clause to appear either at the beginning or at the end of a query block. For some queries, placing the `SELECT` clause at the end may make a query block easier to understand, because the `SELECT` clause refers to variables defined in the other clauses.
