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

WITH TMP1 AS
  (SELECT L_ORDERKEY,
          COUNT(L_SUPPKEY) AS COUNT_SUPPKEY,
          MAX(L_SUPPKEY) AS MAX_SUPPKEY
   FROM
     (SELECT L_ORDERKEY,
             L_SUPPKEY
      FROM LINEITEM L
      GROUP BY L_ORDERKEY,
               L_SUPPKEY) AS L2
   GROUP BY L_ORDERKEY),
     TMP2 AS
  (SELECT L2.L_ORDERKEY,
          COUNT(L_SUPPKEY) AS COUNT_SUPPKEY,
          MAX(L_SUPPKEY) AS MAX_SUPPKEY
   FROM
     (SELECT L_ORDERKEY,
             L_SUPPKEY
      FROM LINEITEM L
      WHERE L_RECEIPTDATE > L_COMMITDATE
      GROUP BY L_ORDERKEY,
               L_SUPPKEY) AS L2
   GROUP BY L_ORDERKEY)
SELECT T4.S_NAME,
       COUNT(*) AS NUMWAIT
FROM
  (SELECT T3.S_NAME,
          T3.L_SUPPKEY,
          T2.L_ORDERKEY,
          COUNT_SUPPKEY,
          MAX_SUPPKEY
   FROM
     (SELECT NS.S_NAME,
             T1.L_ORDERKEY,
             L.L_SUPPKEY
      FROM LINEITEM L,

        (SELECT S.S_NAME,
                S.S_SUPPKEY
         FROM NATION N,
                     SUPPLIER S
         WHERE S.S_NATIONKEY = N.N_NATIONKEY
           AND N.N_NAME="SAUDI ARABIA") AS NS,
                    ORDERS O,
                           TMP1 AS T1
      WHERE NS.S_SUPPKEY = L.L_SUPPKEY
        AND L.L_RECEIPTDATE > L.L_COMMITDATE
        AND O.O_ORDERKEY = T1.L_ORDERKEY
        AND L.L_ORDERKEY = T1.L_ORDERKEY
        AND O.O_ORDERSTATUS = "F") AS T3
   JOIN TMP2 AS T2 ON COUNT_SUPPKEY >= 0
   AND T3.L_ORDERKEY = T2.L_ORDERKEY) AS T4
GROUP BY T4.S_NAME
ORDER BY NUMWAIT DESC,
         T4.S_NAME