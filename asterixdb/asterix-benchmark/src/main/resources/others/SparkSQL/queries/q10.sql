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

SELECT C_CUSTKEY,
       C_NAME,
       SUM(LOCN.L_EXTENDEDPRICE * (1 - LOCN.L_DISCOUNT)) AS REVENUE,
       C_ACCTBAL,
       N_NAME,
       C_ADDRESS,
       C_PHONE,
       C_COMMENT
FROM
  (SELECT OCN.C_CUSTKEY,
          OCN.C_NAME,
          OCN.C_ACCTBAL,
          OCN.N_NAME,
          OCN.C_ADDRESS,
          OCN.C_PHONE,
          OCN.C_COMMENT,
          L.L_EXTENDEDPRICE,
          L.L_DISCOUNT
   FROM LINEITEM AS L,

     (SELECT C.C_CUSTKEY,
             C.C_NAME,
             C.C_ACCTBAL,
             N.N_NAME,
             C.C_ADDRESS,
             C.C_PHONE,
             C.C_COMMENT,
             O.O_ORDERKEY
      FROM ORDERS AS O,
           CUSTOMER AS C,
           NATION AS N
      WHERE C.C_CUSTKEY = O.O_CUSTKEY
        AND O.O_ORDERDATE >= "1993-10-01"
        AND O.O_ORDERDATE < "1994-01-01"
        AND C.C_NATIONKEY = N.N_NATIONKEY) AS OCN
   WHERE L.L_ORDERKEY = OCN.O_ORDERKEY
     AND L.L_RETURNFLAG = "R") AS LOCN
GROUP BY C_CUSTKEY,
         C_NAME,
         C_ACCTBAL,
         C_PHONE,
         N_NAME,
         C_ADDRESS,
         C_COMMENT
ORDER BY REVENUE DESC LIMIT 20