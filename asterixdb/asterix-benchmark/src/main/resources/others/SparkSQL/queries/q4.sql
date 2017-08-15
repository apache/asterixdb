-- ------------------------------------------------------------
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
-- ------------------------------------------------------------

WITH tmp AS
  (SELECT l.L_ORDERKEY AS O_ORDERKEY
   FROM LINEITEM AS l
   WHERE l.L_COMMITDATE < l.L_RECEIPTDATE)
SELECT o.O_ORDERPRIORITY,
       count(*) AS COUNT
FROM ORDERS AS o
JOIN tmp AS t ON o.O_ORDERKEY = t.O_ORDERKEY
WHERE o.O_ORDERDATE >= "1993-07-01"
  AND o.O_ORDERDATE < "1993-10-01"
GROUP BY o.O_ORDERPRIORITY
ORDER BY O_ORDERPRIORITY