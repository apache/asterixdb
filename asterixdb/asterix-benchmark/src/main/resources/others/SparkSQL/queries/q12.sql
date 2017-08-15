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

SELECT L.L_SHIPMODE,
       SUM(CASE WHEN O.O_ORDERPRIORITY = "1-URGENT"
           OR O.O_ORDERPRIORITY = "2-HIGH" THEN 1 ELSE 0 END) HIGH_LINE_COUNT,
       SUM(CASE WHEN O.O_ORDERPRIORITY = "1-URGENT"
           OR O.O_ORDERPRIORITY = "2-HIGH" THEN 0 ELSE 1 END) LOW_LINE_COUNT
FROM LINEITEM L,
     ORDERS O
WHERE O.O_ORDERKEY = L.L_ORDERKEY
  AND L.L_COMMITDATE < L.L_RECEIPTDATE
  AND L.L_SHIPDATE < L.L_COMMITDATE
  AND L.L_RECEIPTDATE >= "1994-01-01"
  AND L.L_RECEIPTDATE < "1995-01-01"
  AND (L.L_SHIPMODE = "MAIL"
       OR L.L_SHIPMODE = "SHIP")
GROUP BY L.L_SHIPMODE
ORDER BY L.L_SHIPMODE