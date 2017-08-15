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

WITH q2_minimum_cost_supplier_tmp1 AS
  (SELECT s.S_ACCTBAL,
          s.S_NAME,
          n.N_NAME,
          p.P_PARTKEY,
          ps.PS_SUPPLYCOST,
          p.P_MFGR,
          s.S_ADDRESS,
          s.S_PHONE,
          s.S_COMMENT
   FROM NATION n
   JOIN REGION r ON n.N_REGIONKEY = r.R_REGIONKEY
   AND r.R_NAME = "EUROPE"
   JOIN SUPPLIER s ON s.S_NATIONKEY = n.N_NATIONKEY
   JOIN PARTSUPP ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
   JOIN PART p ON p.P_PARTKEY = ps.S_PARTKEY
   AND p.P_TYPE LIKE "%BRASS"
   AND p.P_SIZE = 15),
     q2_minimum_cost_supplier_tmp2 AS
  (SELECT p.P_PARTKEY,
          min(p.PS_SUPPLYCOST) AS PS_MIN_SUPPLYCOST
   FROM q2_minimum_cost_supplier_tmp1 p
   GROUP BY p.P_PARTKEY)
SELECT t1.S_ACCTBAL,
       t1.S_NAME,
       t1.N_NAME,
       t1.P_PARTKEY,
       t1.P_MFGR AS P_MFGR,
       t1.S_ADDRESS,
       t1.S_PHONE,
       t1.S_COMMENT
FROM q2_minimum_cost_supplier_tmp1 t1
JOIN q2_minimum_cost_supplier_tmp2 t2 ON t1.P_PARTKEY = t2.P_PARTKEY
AND t1.PS_SUPPLYCOST=t2.PS_MIN_SUPPLYCOST
ORDER BY t1.S_ACCTBAL DESC,
         t1.N_NAME,
         t1.S_NAME,
         t1.P_PARTKEY LIMIT 100