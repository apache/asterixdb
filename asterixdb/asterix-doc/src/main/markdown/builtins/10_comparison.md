<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->

## <a id="ComparisonFunctions">Comparison Functions</a> ##

### greatest ###
 * Syntax:

        greatest(numeric_value1, numeric_value2, ...)

 * Computes the greatest value among arguments.
 * Arguments:
    * `numeric_value1`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value,
    * `numeric_value2`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value,
    * ....
 * Return Value:
    * the greatest values among arguments.
      The returning type is decided by the item type with the highest
      order in the numeric type promotion order (`tinyint`-> `smallint`->`integer`->`bigint`->`float`->`double`)
      among items.
    * `null` if any argument is a `missing` value or `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": greatest(1, 2, 3), "v2": greatest(float("0.5"), double("-0.5"), 5000) };


 * The expected result is:

        { "v1": 3, "v2": 5000.0 }


### least ###
 * Syntax:

        least(numeric_value1, numeric_value2, ...)

 * Computes the least value among arguments.
 * Arguments:
    * `numeric_value1`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value,
    * `numeric_value2`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value,
    * ....
 * Return Value:
    * the least values among arguments.
      The returning type is decided by the item type with the highest
      order in the numeric type promotion order (`tinyint`-> `smallint`->`integer`->`bigint`->`float`->`double`)
      among items.
    * `null` if any argument is a `missing` value or `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": least(1, 2, 3), "v2": least(float("0.5"), double("-0.5"), 5000) };


 * The expected result is:

        { "v1": 1, "v2": -0.5 }

