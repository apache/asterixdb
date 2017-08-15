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

SELECT O_YEAR,
       SUM(CASE WHEN T.S_NAME = "BRAZIL" THEN T.REVENUE ELSE 0.0 END) / SUM(T.REVENUE) AS MKT_SHARE FROM
  (SELECT YEAR(SLNRCOP.O_ORDERDATE) AS O_YEAR, SLNRCOP.L_EXTENDEDPRICE * (1 - SLNRCOP.L_DISCOUNT) AS REVENUE, N2.N_NAME AS S_NAME
   FROM
     (SELECT LNRCOP.O_ORDERDATE, LNRCOP.L_DISCOUNT, LNRCOP.L_EXTENDEDPRICE, LNRCOP.L_SUPPKEY, S.S_NATIONKEY
      FROM SUPPLIER S,
        (SELECT LNRCO.O_ORDERDATE, LNRCO.L_DISCOUNT, LNRCO.L_EXTENDEDPRICE, LNRCO.L_SUPPKEY
         FROM
           (SELECT NRCO.O_ORDERDATE, L.L_PARTKEY, L.L_DISCOUNT, L.L_EXTENDEDPRICE, L.L_SUPPKEY
            FROM LINEITEM L,
              (SELECT O.O_ORDERDATE, O.O_ORDERKEY
               FROM ORDERS O,
                 (SELECT C.C_CUSTKEY
                  FROM CUSTOMER C,
                    (SELECT N.N_NATIONKEY
                     FROM NATION N, REGION R
                     WHERE N.N_REGIONKEY = R.R_REGIONKEY
                       AND R.R_NAME = "AMERICA") AS NR
                  WHERE C.C_NATIONKEY = NR.N_NATIONKEY) AS NRC
               WHERE NRC.C_CUSTKEY = O.O_CUSTKEY) AS NRCO
            WHERE L.L_ORDERKEY = NRCO.O_ORDERKEY
              AND NRCO.O_ORDERDATE >= "1995-01-01"
              AND NRCO.O_ORDERDATE < "1996-12-31") AS LNRCO, PART P
         WHERE P.P_PARTKEY = LNRCO.L_PARTKEY
           AND P.P_TYPE = "ECONOMY ANODIZED STEEL") AS LNRCOP
      WHERE S.S_SUPPKEY = LNRCOP.L_SUPPKEY) AS SLNRCOP, NATION N2
   WHERE SLNRCOP.S_NATIONKEY = N2.N_NATIONKEY) AS T
GROUP BY O_YEAR
ORDER BY O_YEAR