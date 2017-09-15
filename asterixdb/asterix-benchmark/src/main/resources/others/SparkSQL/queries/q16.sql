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
  (SELECT PSP.P_BRAND,
          PSP.P_TYPE,
          PSP.P_SIZE,
          PSP.PS_SUPPKEY
   FROM
     (SELECT P.P_BRAND,
             P.P_TYPE,
             P.P_SIZE,
             PS.PS_SUPPKEY
      FROM PARTSUPP PS,
                    PART P
      WHERE P.P_PARTKEY = PS.S_PARTKEY
        AND P.P_BRAND != "Brand#45"
        AND P.P_TYPE NOT LIKE "MEDIUM POLISHED%") AS PSP,
        SUPPLIER S
   WHERE PSP.PS_SUPPKEY = S.S_SUPPKEY
     AND S.S_COMMENT NOT LIKE "%Customer%Complaints%")
SELECT P_BRAND,
       P_TYPE,
       P_SIZE,
       COUNT(PS_SUPPKEY) SUPPLIER_CNT
FROM
  (SELECT P_BRAND,
          P_TYPE,
          P_SIZE,
          PS_SUPPKEY
   FROM tmp
   WHERE P_SIZE = 49
     OR P_SIZE = 14
     OR P_SIZE = 23
     OR P_SIZE = 45
     OR P_SIZE = 19
     OR P_SIZE = 3
     OR P_SIZE = 36
     OR P_SIZE = 9
   GROUP BY P_BRAND,
            P_TYPE,
            P_SIZE,
            PS_SUPPKEY) AS T2
GROUP BY P_BRAND,
         P_TYPE,
         P_SIZE
ORDER BY SUPPLIER_CNT DESC,
         P_BRAND,
         P_TYPE,
         P_SIZE