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

SELECT NATION,
       O_YEAR,
       SUM(AMOUNT) AS SUM_PROFIT
FROM
  (SELECT L3.N_NAME AS NATION,
          YEAR(O.O_ORDERDATE) AS O_YEAR,
          L3.L_EXTENDEDPRICE * (1 - L3.L_DISCOUNT) - L3.PS_SUPPLYCOST * L3.L_QUANTITY AS AMOUNT
   FROM ORDERS O
   JOIN
     (SELECT L2.L_EXTENDEDPRICE,
             L2.L_DISCOUNT,
             L2.L_QUANTITY,
             L2.L_ORDERKEY,
             L2.N_NAME,
             L2. PS_SUPPLYCOST
      FROM PART P
      JOIN
        (SELECT L1.L_EXTENDEDPRICE,
                L1.L_DISCOUNT,
                L1.L_QUANTITY,
                L1.L_PARTKEY,
                L1.L_ORDERKEY,
                L1.N_NAME,
                PS.PS_SUPPLYCOST
         FROM PARTSUPP PS
         JOIN
           (SELECT L.L_SUPPKEY,
                   L.L_EXTENDEDPRICE,
                   L.L_DISCOUNT,
                   L.L_QUANTITY,
                   L.L_PARTKEY,
                   L.L_ORDERKEY,
                   S1.N_NAME
            FROM
              (SELECT S.S_SUPPKEY,
                      N.N_NAME
               FROM NATION N
               JOIN SUPPLIER S ON N.N_NATIONKEY = S.S_NATIONKEY) S1
            JOIN LINEITEM L ON S1.S_SUPPKEY = L.L_SUPPKEY) L1 ON PS.PS_SUPPKEY = L1.L_SUPPKEY
         AND PS.S_PARTKEY = L1.L_PARTKEY) L2 ON P.P_NAME LIKE "%green%"
      AND P.P_PARTKEY = L2.L_PARTKEY) L3 ON O.O_ORDERKEY = L3.L_ORDERKEY) PROFIT
GROUP BY NATION,
         O_YEAR
ORDER BY NATION,
         O_YEAR DESC