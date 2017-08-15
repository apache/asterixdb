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

WITH q7_volume_shipping_tmp AS
  (SELECT N1.N_NAME AS SUPP_NATION,
          N2.N_NAME AS CUST_NATION,
          N1.N_NATIONKEY AS S_NATIONKEY,
          N2.N_NATIONKEY AS C_NATIONKEY
   FROM NATION AS N1,
        NATION AS N2
   WHERE (N1.N_NAME="FRANCE"
          AND N2.N_NAME="GERMANY")
     OR (N1.N_NAME="GERMANY"
         AND N2.N_NAME="FRANCE"))
SELECT SUPP_NATION,
       CUST_NATION,
       L_YEAR,
       SUM(VOLUME) AS REVENUE
FROM
  (SELECT T.SUPP_NATION,
          T.CUST_NATION,
          YEAR(L3.L_SHIPDATE) AS L_YEAR,
          L3.L_EXTENDEDPRICE * (1 - L3.L_DISCOUNT) AS VOLUME
   FROM q7_volume_shipping_tmp T
   JOIN
     (SELECT L2.L_SHIPDATE,
             L2.L_EXTENDEDPRICE,
             L2.L_DISCOUNT,
             L2.C_NATIONKEY,
             S.S_NATIONKEY
      FROM SUPPLIER S
      JOIN
        (SELECT L1.L_SHIPDATE,
                L1.L_EXTENDEDPRICE,
                L1.L_DISCOUNT,
                L1.L_SUPPKEY,
                C.C_NATIONKEY
         FROM CUSTOMER C
         JOIN
           (SELECT L.L_SHIPDATE,
                   L.L_EXTENDEDPRICE,
                   L.L_DISCOUNT,
                   L.L_SUPPKEY,
                   O.O_CUSTKEY
            FROM ORDERS O
            JOIN LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY
            AND L.L_SHIPDATE >= "1995-01-01"
            AND L.L_SHIPDATE <= "1996-12-31") L1 ON C.C_CUSTKEY = L1.O_CUSTKEY) L2 ON S.S_SUPPKEY = L2.L_SUPPKEY) L3 ON T.C_NATIONKEY = L3.C_NATIONKEY
   AND T.S_NATIONKEY = L3.S_NATIONKEY) SHIPPING
GROUP BY SUPP_NATION,
         CUST_NATION,
         L_YEAR
ORDER BY SUPP_NATION,
         CUST_NATION,
         L_YEAR