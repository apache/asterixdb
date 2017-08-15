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

SELECT O1.N_NAME,
       SUM(O1.L_EXTENDEDPRICE * (1 - O1.L_DISCOUNT)) AS REVENUE
FROM CUSTOMER C
JOIN
  (SELECT L1.N_NAME,
          L1.L_EXTENDEDPRICE,
          L1.L_DISCOUNT,
          L1.S_NATIONKEY,
          O.O_CUSTKEY
   FROM ORDERS O
   JOIN
     (SELECT S1.N_NAME,
             L.L_EXTENDEDPRICE,
             L.L_DISCOUNT,
             L.L_ORDERKEY,
             S1.S_NATIONKEY
      FROM LINEITEM L
      JOIN
        (SELECT N1.N_NAME,
                S.S_SUPPKEY,
                S.S_NATIONKEY
         FROM SUPPLIER S
         JOIN
           (SELECT N.N_NAME,
                   N.N_NATIONKEY
            FROM NATION N
            JOIN REGION R ON N.N_REGIONKEY = R.R_REGIONKEY
            AND R.R_NAME = "ASIA") N1 ON S.S_NATIONKEY = N1.N_NATIONKEY) S1 ON L.L_SUPPKEY = S1.S_SUPPKEY) L1 ON L1.L_ORDERKEY = O.O_ORDERKEY
   AND O.O_ORDERDATE >= "1994-01-01"
   AND O.O_ORDERDATE < "1995-01-01") O1 ON C.C_NATIONKEY = O1.S_NATIONKEY
AND C.C_CUSTKEY = O1.O_CUSTKEY
GROUP BY O1.N_NAME
ORDER BY REVENUE DESC