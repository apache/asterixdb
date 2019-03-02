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

## <a id="MiscFunctions">Miscellaneous Functions</a> ##

### uuid ###
 * Syntax:

        uuid()

* Generates a `uuid`.
* Arguments:
    * none
* Return Value:
    * a generated, random `uuid`.


### len ###
 * Syntax:

    len(array)

 * Returns the length of the array array.
 * Arguments:
    * `array` : an `array`, `multiset`, `null`, or `missing`, represents the collection that needs to be checked.
 * Return Value:
    * an `integer` that represents the length of input array or the size of the input multiset,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value.

 * Example:

        len(["Hello", "World"])


 * The expected result is:

        2


### not ###
 * Syntax:

        not(expr)

 * Inverts a `boolean` value
 * Arguments:
    * `expr` : an expression
 * Return Value:
    * a `boolean`, the inverse of `expr`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * other non-boolean argument value will cause a type error.
 * Example:

        { "v1": `not`(true), "v2": `not`(false), "v3": `not`(null), "v4": `not`(missing) };

 * The expected result is:

        { "v1": false, "v2": true, "v3": null }


### random ###
 * Syntax:

        random( [seed_value] )

 * Returns a random number, accepting an optional seed value
 * Arguments:
    * `seed_value`: an optional `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value representing the seed number.
 * Return Value:
    * A random number of type `double` between 0 and 1,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value or a non-numeric value.

 * Example:

        {
          "v1": random(),
          "v2": random(unix_time_from_datetime_in_ms(current_datetime()))
        };


### range ###
 * Syntax:

        range(start_numeric_value, end_numeric_value)

* Generates a series of `bigint` values based start the `start_numeric_value` until the `end_numeric_value`.
* Arguments:
   * `start_numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint` value representing the start value.
   * `end_numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint` value representing the max final value.
* Return Value:
    * an array that starts with the integer value of `start_numeric_value` and ends with
      the integer value of `end_numeric_value`, where the value of each entry in the array is
      the integer successor of the value in the preceding entry.
* Example:

        range(0, 3);

 * The expected result is:

        [ 0, 1, 2, 3 ]


### switch_case ###
 * Syntax:

        switch_case(
            condition,
            case1, case1_result,
            case2, case2_result,
            ...,
            default, default_result
        )

 * Switches amongst a sequence of cases and returns the result of the first matching case. If no match is found, the result of the default case is returned.
 * Arguments:
    * `condition`: a variable (any type is allowed).
    * `caseI/default`: a variable (any type is allowed).
    * `caseI/default_result`: a variable (any type is allowed).
 * Return Value:
    * `caseI_result` if `condition` matches `caseI`, otherwise `default_result`.
 * Example 1:

        switch_case(
            "a",
            "a", 0,
            "x", 1,
            "y", 2,
            "z", 3
        );


 * The expected result is:

        0

 * Example 2:

        switch_case(
            "a",
            "x", 1,
            "y", 2,
            "z", 3
        );

 * The expected result is:

        3


### deep_equal ###
* Syntax:

        deep_equal(expr1, expr2)


 * Assess the equality between two expressions of any type (e.g., object, arrays, or multiset).
 Two objects are deeply equal iff both their types and values are equal.
 * Arguments:
    * `expr1` : an expression,
    * `expr2` : an expression.
 * Return Value:
    * `true` or `false` depending on the data equality,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value.


 * Example:

        deep_equal(
                   {
                     "id":1,
                     "project":"AsterixDB",
                     "address":{"city":"Irvine", "state":"CA"},
                     "related":["Hivestrix", "Preglix", "Apache VXQuery"]
                   },
                   {
                     "id":1,
                     "project":"AsterixDB",
                     "address":{"city":"San Diego", "state":"CA"},
                     "related":["Hivestrix", "Preglix", "Apache VXQuery"]
                   }
        );

 * The expected result is:

        false

