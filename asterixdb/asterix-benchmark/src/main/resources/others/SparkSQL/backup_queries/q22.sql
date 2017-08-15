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

WITH q22_customer_tmp AS
  (SELECT C_ACCTBAL,
          C_CUSTKEY,
          SUBSTRING(C_PHONE,1,2) AS CNTRYCODE
   FROM CUSTOMER
   WHERE SUBSTRING(C_PHONE,1,2) = "13"
     OR SUBSTRING(C_PHONE,1,2) = "31"
     OR SUBSTRING(C_PHONE,1,2) = "23"
     OR SUBSTRING(C_PHONE,1,2) = "29"
     OR SUBSTRING(C_PHONE,1,2) = "30"
     OR SUBSTRING(C_PHONE,1,2) = "18"
     OR SUBSTRING(C_PHONE,1,2) = "17"),
     AVG AS
  (SELECT AVG(C_ACCTBAL)
   FROM CUSTOMER
   WHERE C_ACCTBAL > 0.0
     AND (SUBSTRING(C_PHONE,1,2) = "13"
          OR SUBSTRING(C_PHONE,1,2) = "31"
          OR SUBSTRING(C_PHONE,1,2) = "23"
          OR SUBSTRING(C_PHONE,1,2) = "29"
          OR SUBSTRING(C_PHONE,1,2) = "30"
          OR SUBSTRING(C_PHONE,1,2) = "18"
          OR SUBSTRING(C_PHONE,1,2) = "17"))
SELECT CNTRYCODE,
       COUNT(*) AS NUMCUST,
       SUM(C_ACCTBAL) AS TOTACCTBAL
FROM Q22_CUSTOMER_TMP AS CT
WHERE CT.C_ACCTBAL >
    (SELECT *
     FROM AVG)
  AND EXISTS
    (SELECT *
     FROM ORDERS AS O
     WHERE CT.C_CUSTKEY = O.O_CUSTKEY)
GROUP BY CNTRYCODE
ORDER BY CNTRYCODE