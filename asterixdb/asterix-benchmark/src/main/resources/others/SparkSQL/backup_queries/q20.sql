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

WITH Q20_TMP1 AS
  (SELECT DISTINCT P_PARTKEY
   FROM PART
   WHERE P_NAME LIKE "forest%"),
     Q20_TMP2 AS
  (SELECT L_PARTKEY,
          L_SUPPKEY,
          0.5 * SUM(L_QUANTITY) AS SUM_QUANTITY
   FROM LINEITEM
   WHERE L_SHIPDATE >= "1994-01-01"
     AND L_SHIPDATE < "1995-01-01"
   GROUP BY L_PARTKEY,
            L_SUPPKEY),
     Q20_TMP3 AS
  (SELECT PS_SUPPKEY,
          PS_AVAILQTY,
          T2.SUM_QUANTITY
   FROM PARTSUPP
   JOIN Q20_TMP1 T1 ON S_PARTKEY = T1.P_PARTKEY
   JOIN Q20_TMP2 T2 ON S_PARTKEY = T2.L_PARTKEY
   AND PS_SUPPKEY = T2.L_SUPPKEY),
     Q20_TMP4 AS
  (SELECT PS_SUPPKEY
   FROM Q20_TMP3
   WHERE PS_AVAILQTY > SUM_QUANTITY
   GROUP BY PS_SUPPKEY)
SELECT S.S_NAME,
       S.S_ADDRESS
FROM SUPPLIER S
JOIN NATION N ON S.S_NATIONKEY = N.N_NATIONKEY
JOIN Q20_TMP4 T4 ON S.S_SUPPKEY = T4.PS_SUPPKEY
WHERE N.N_NAME = "CANADA"
ORDER BY S.S_NAME