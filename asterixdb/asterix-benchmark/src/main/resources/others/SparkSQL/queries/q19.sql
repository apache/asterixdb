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
  (SELECT L_PARTKEY AS LPKEY,
          L_QUANTITY AS QUANTITY,
          L_EXTENDEDPRICE AS EXTNDPRICE,
          L_DISCOUNT AS DISCOUNT
   FROM LINEITEM
   WHERE (L_SHIPMODE = "AIR"
          OR L_SHIPMODE = "AIR REG")
     AND L_SHIPINSTRUCT = "DELIVER IN PERSON")
SELECT SUM(L.EXTNDPRICE * (1 - L.DISCOUNT))
FROM tmp L
JOIN PART P ON P.P_PARTKEY = L.LPKEY
WHERE (P.P_BRAND = "Brand#12"
       AND P.P_CONTAINER REGEXP "SM CASE|SM BOX|SM PACK|SM PKG"
       AND L.QUANTITY >= 1
       AND L.QUANTITY <= 11
       AND P.P_SIZE >= 1
       AND P.P_SIZE <= 5)
  OR (P.P_BRAND = "Brand#23"
      AND P.P_CONTAINER REGEXP "MED BAG|MED BOX|MED PKG|MED PACK"
      AND L.QUANTITY >= 10
      AND L.QUANTITY <= 20
      AND P.P_SIZE >= 1
      AND P.P_SIZE <= 10)
  OR (P.P_BRAND = "Brand#34"
      AND P.P_CONTAINER REGEXP "LG CASE|LG BOX|LG PACK|LG PKG"
      AND L.QUANTITY >= 20
      AND L.QUANTITY <= 30
      AND P.P_SIZE >= 1
      AND P.P_SIZE <= 15)