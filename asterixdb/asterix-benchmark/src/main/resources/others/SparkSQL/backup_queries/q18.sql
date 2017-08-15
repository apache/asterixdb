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
  (SELECT L_ORDERKEY,
          SUM(L_QUANTITY) T_SUM_QUANTITY
   FROM LINEITEM
   GROUP BY L_ORDERKEY)
SELECT C.C_NAME,
       C.C_CUSTKEY,
       O.O_ORDERKEY,
       O.O_ORDERDATE,
       O.O_TOTALPRICE,
       SUM(L.L_QUANTITY) SUM_QUANTITY
FROM CUSTOMER C
JOIN ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
JOIN TMP T ON O.O_ORDERKEY = T.L_ORDERKEY
JOIN LINEITEM L ON T.L_ORDERKEY = L.L_ORDERKEY
WHERE T.T_SUM_QUANTITY > 30
GROUP BY C.C_NAME,
         C.C_CUSTKEY,
         O.O_ORDERKEY,
         O.O_ORDERDATE,
         O.O_TOTALPRICE
ORDER BY O.O_TOTALPRICE DESC,
         O.O_ORDERDATE LIMIT 100