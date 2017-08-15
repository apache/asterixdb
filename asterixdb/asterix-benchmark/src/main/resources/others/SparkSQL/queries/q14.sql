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

SELECT 100.0 * SUM(CASE WHEN P.P_TYPE LIKE "PROMO%" THEN L.L_EXTENDEDPRICE * (1 - L.L_DISCOUNT) ELSE 0.0 END) / SUM(L.L_EXTENDEDPRICE * (1 - L.L_DISCOUNT))
FROM LINEITEM L,
     PART P
WHERE L.L_PARTKEY = P.P_PARTKEY
  AND L.L_SHIPDATE >= "1995-09-01"
  AND L.L_SHIPDATE < "1995-10-01"