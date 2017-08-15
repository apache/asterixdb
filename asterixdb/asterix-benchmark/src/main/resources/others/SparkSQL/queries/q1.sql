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

SELECT l.L_RETURNFLAG,
       l.L_LINESTATUS,
       sum(l.L_QUANTITY) AS sum_qty,
       sum(l.L_EXTENDEDPRICE) AS sum_base_price,
       sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS sum_disc_price,
       sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) * (1 + l.L_TAX)) AS sum_charge,
       avg(l.l_quantity) AS ave_qty,
       avg(l.L_EXTENDEDPRICE) AS ave_price,
       avg(l.L_DISCOUNT) AS ave_disc,
       count(*) AS count_order
FROM LINEITEM AS l
WHERE l.L_SHIPDATE <= "1998-09-02"
GROUP BY l.L_RETURNFLAG,
         l.L_LINESTATUS
ORDER BY l.L_RETURNFLAG,
         l.L_LINESTATUS