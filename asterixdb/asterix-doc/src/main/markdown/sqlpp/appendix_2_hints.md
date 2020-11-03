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

## <a id="Query_hints">Query Hints</a>

#### <a id="hash_groupby">"hash" GROUP BY hint</a>

The system supports two algorithms for GROUP BY clause evaluation: pre-sorted and hash-based.
By default it uses the pre-sorted approach: The input data is first sorted on the grouping fields
and then aggregation is performed on that sorted data. The alternative is a hash-based strategy
which can be enabled via a `/*+ hash */` GROUP BY hint: The data is aggregated using an in-memory hash-table
(that can spill to disk if necessary). This approach is recommended for low-cardinality grouping fields.

##### Example:

    SELECT c.address.state, count(*)
    FROM Customers AS c
    /*+ hash */ GROUP BY c.address.state

#### <a id="hash_bcast_join">"hash-bcast" JOIN hint</a>

By default the system uses a partitioned-parallel hash join strategy to parallelize the execution of an
equi-join. In this approach both sides of the join are repartitioned (if necessary) on a hash of the join key;
potentially matching data items thus arrive at the same partition to be joined locally.
This strategy is robust, but not always the fastest when one of the join sides is low cardinality and
the other is high cardinality (since it scans and potentially moves the data from both sides).
This special case can be better handled by broadcasting (replicating) the smaller side to all data partitions
of the larger side and not moving the data from the other (larger) side. The system provides a join hint to enable
this strategy: `/*+ hash-bcast */`. This hint forces the right side of the join to be replicated while the left side
retains its original partitioning.

##### Example:

    SELECT *
    FROM Orders AS o JOIN Customers AS c
    ON o.customer_id /*+ hash-bcast */ = c.customer_id
