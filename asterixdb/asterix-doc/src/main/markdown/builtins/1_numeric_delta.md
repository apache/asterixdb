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

### round_half_to_even ###
 * Syntax:

        round_half_to_even(numeric_value, [precision])

 * Computes the closest numeric value to `numeric_value` that is a multiple of ten to the power of minus `precision`.
   `precision` is optional and by default value `0` is used.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
    * `precision`: an optional `tinyint`/`smallint`/`integer`/`bigint` field representing the
       number of digits in the fraction of the the result
 * Return Value:
    * The rounded value for the given number in the same type as the input argument,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-numeric value,
        * or, the second argument is any other non-tinyint, non-smallint, non-integer, or non-bigint value.

 * Example:

        {
          "v1": round_half_to_even(2013),
          "v2": round_half_to_even(-4036),
          "v3": round_half_to_even(0.8),
          "v4": round_half_to_even(float("-2013.256")),
          "v5": round_half_to_even(double("-2013.893823748327284")),
          "v6": round_half_to_even(double("-2013.893823748327284"), 2),
          "v7": round_half_to_even(2013, 4),
          "v8": round_half_to_even(float("-2013.256"), 5)
        };


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 1.0, "v4": -2013.0, "v5": -2014.0, "v6": -2013.89, "v7": 2013, "v8": -2013.256 }


