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

SELECT l.L_ORDERKEY,
       sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
        o.O_ORDERDATE,
        o.O_SHIPPRIORITY
FROM  CUSTOMER AS c,
      ORDERS AS o,
      LINEITEM AS l
where c.C_MKTSEGMENT = 'BUILDING'
      AND c.C_CUSTKEY = o.O_CUSTKEY
      AND l.L_ORDERKEY = o.O_ORDERKEY
      AND o.O_ORDERDATE < '1995-03-15'
      AND l.L_SHIPDATE > '1995-03-15'
GROUP BY l.L_ORDERKEY, o.O_ORDERDATE, o.O_SHIPPRIORITY
ORDER BY REVENUE DESC,O_ORDERDATE
LIMIT 10