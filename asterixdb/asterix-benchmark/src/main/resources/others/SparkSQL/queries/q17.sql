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
  (SELECT L_PARTKEY T_PARTKEY,
                    0.2 * AVG(L_QUANTITY) T_AVG_QUANTITY
   FROM LINEITEM
   GROUP BY L_PARTKEY)
SELECT SUM(L.L_EXTENDEDPRICE) / 7.0
FROM tmp T,
     LINEITEM L,
     PART P
WHERE P.P_PARTKEY = L.L_PARTKEY
  AND P.P_CONTAINER = "MED BOX"
  AND P.P_BRAND = "Brand#23"
  AND L.L_PARTKEY = T.T_PARTKEY
  AND L.L_QUANTITY < T.T_AVG_QUANTITY