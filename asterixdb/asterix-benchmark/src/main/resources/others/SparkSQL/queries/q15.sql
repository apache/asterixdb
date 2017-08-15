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

WITH REVENUE AS
  (SELECT L.L_SUPPKEY AS SUPPLIER_NO,
          SUM(L.L_EXTENDEDPRICE * (1 - L.L_DISCOUNT)) AS TOTAL_REVENUE
   FROM LINEITEM L
   WHERE L.L_SHIPDATE >= "1996-01-01"
     AND L.L_SHIPDATE < "1996-04-01"
   GROUP BY L.L_SUPPKEY),
     m AS
  (SELECT MAX(R2.TOTAL_REVENUE)
   FROM REVENUE R2)
SELECT S.S_SUPPKEY,
       S.S_NAME,
       S.S_ADDRESS,
       S.S_PHONE,
       R.TOTAL_REVENUE
FROM SUPPLIER S,
     REVENUE R
WHERE S.S_SUPPKEY = R.SUPPLIER_NO
  AND R.TOTAL_REVENUE <
    (SELECT *
     FROM m) + 0.000000001
  AND R.TOTAL_REVENUE >
    (SELECT *
     FROM m) - 0.000000001