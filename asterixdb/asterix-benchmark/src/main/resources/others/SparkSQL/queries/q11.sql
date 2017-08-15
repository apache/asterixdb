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

WITH s1 AS
  (SELECT SUM(PS.PS_SUPPLYCOST * PS.PS_AVAILQTY)
   FROM PARTSUPP AS PS,

     (SELECT S.S_SUPPKEY
      FROM SUPPLIER AS S,
           NATION AS N
      WHERE S.S_NATIONKEY = N.N_NATIONKEY
        AND N.N_NAME = "GERMANY") AS SN
   WHERE PS.PS_SUPPKEY = SN.S_SUPPKEY)
SELECT S_PARTKEY,
       SUM(PS.PS_SUPPLYCOST * PS.PS_AVAILQTY) AS PART_VALUE
FROM PARTSUPP PS,
  (SELECT S.S_SUPPKEY
   FROM SUPPLIER AS S,
        NATION AS N
   WHERE S.S_NATIONKEY = N.N_NATIONKEY
     AND N.N_NAME = "GERMANY") SN
WHERE PS.PS_SUPPKEY = SN.S_SUPPKEY
GROUP BY PS.S_PARTKEY HAVING PART_VALUE >
  (SELECT *
   FROM s1) * 0.0001000
ORDER BY PART_VALUE DESC