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

SELECT C_COUNT, COUNT(*) AS CUSTDIST
FROM  (
        SELECT C_CUSTKEY, SUM(O_ORDERKEY_COUNT) AS C_COUNT
        FROM  (
                SELECT C.C_CUSTKEY, COUNT(O.O_ORDERKEY) AS O_ORDERKEY_COUNT
                FROM (CUSTOMER C LEFT OUTER JOIN ORDERS O)
                WHERE C.C_CUSTKEY = O.O_CUSTKEY AND O.O_COMMENT NOT LIKE "%special%requests%"
                GROUP BY C.C_CUSTKEY
        ) CO
        GROUP BY C_CUSTKEY
) GCO
GROUP BY C_COUNT
ORDER BY CUSTDIST DESC,C_COUNT DESC