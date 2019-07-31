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

## <a id="Parallel_sort_parameter">Parallel Sort Parameter</a>
The following parameter enables you to activate or deactivate full parallel sort for order-by operations.

When full parallel sort is inactive (`false`), each existing data partition is sorted (in parallel),
and then all data partitions are merged into a single node.

When full parallel sort is active (`true`), the data is first sampled, and then repartitioned
so that each partition contains data that is greater than the previous partition.
The data in each partition is then sorted (in parallel),
but the sorted partitions are not merged into a single node.

* **compiler.sort.parallel**: A boolean specifying whether full parallel sort is active (`true`) or inactive (`false`).
  The default value is `true`.

##### Example

    SET `compiler.sort.parallel` "true";

    SELECT VALUE user
    FROM GleambookUsers AS user
    ORDER BY ARRAY_LENGTH(user.friendIds) DESC;

