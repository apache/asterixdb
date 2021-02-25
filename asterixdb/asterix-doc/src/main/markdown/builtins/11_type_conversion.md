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

### to_array ###
  * Syntax:

        to_array(expr)

  * Converts input value to an `array` value
  * Arguments:
     * `expr` : an expression
  * Return Value:
     * if the argument is `missing` then `missing` is returned
     * if the argument is `null` then `null` is returned
     * if the argument is of `array` type then it is returned as is
     * if the argument is of `multiset` type then it is returned as an `array` with elements in an undefined order
     * otherwise an `array` containing the input expression as its single item is returned

 * Example:

        {
          "v1": to_array("asterix"),
          "v2": to_array(["asterix"]),
        };

 * The expected result is:

        { "v1": ["asterix"], "v2": ["asterix"] }

 The function has an alias `toarray`.

### to_atomic (to_atom) ###
  * Syntax:

        to_atomic(expr)

  * Converts input value to a [primitive](../datamodel.html#PrimitiveTypes) value
  * Arguments:
     * `expr` : an expression
  * Return Value:
     * if the argument is `missing` then `missing` is returned
     * if the argument is `null` then `null` is returned
     * if the argument is of primitive type then it is returned as is
     * if the argument is of `array` or `multiset` type and has only one element then the result of invoking
       to_atomic() on that element is returned
     * if the argument is of `object` type and has only one field then the result of invoking to_atomic() on the
       value of that field is returned
     * otherwise `null` is returned

 * Example:

        {
          "v1": to_atomic("asterix"),
          "v2": to_atomic(["asterix"]),
          "v3": to_atomic([0, 1]),
          "v4": to_atomic({"value": "asterix"}),
          "v5": to_number({"x": 1, "y": 2})
        };

 * The expected result is:

        { "v1": "asterix", "v2": "asterix", "v3": null, "v4": "asterix", "v5": null }

 The function has three aliases: `toatomic`, `to_atom`, and `toatom`.

### to_boolean (to_bool) ###
  * Syntax:

        to_boolean(expr)

  * Converts input value to a `boolean` value
  * Arguments:
     * `expr` : an expression
  * Return Value:
     * if the argument is `missing` then `missing` is returned
     * if the argument is `null` then `null` is returned
     * if the argument is of `boolean` type then it is returned as is
     * if the argument is of numeric type then `false` is returned if it is `0` or `NaN`, otherwise `true`
     * if the argument is of `string` type then `false` is returned if it's empty, otherwise `true`
     * if the argument is of `array` or `multiset` type then `false` is returned if it's size is `0`, otherwise `true`
     * if the argument is of `object` type then `false` is returned if it has no fields, otherwise `true`
     * type error is raised for all other input types

 * Example:

        {
          "v1": to_boolean(0),
          "v2": to_boolean(1),
          "v3": to_boolean(""),
          "v4": to_boolean("asterix")
        };

 * The expected result is:

        { "v1": false, "v2": true, "v3": false, "v4": true }

 The function has three aliases: `toboolean`, `to_bool`, and `tobool`.

### to_bigint ###
  * Syntax:

        to_bigint(expr)

  * Converts input value to an integer value
  * Arguments:
     * `expr` : an expression
  * Return Value:
     * if the argument is `missing` then `missing` is returned
     * if the argument is `null` then `null` is returned
     * if the argument is of `boolean` type then `1` is returned if it is `true`, `0` if it is `false`
     * if the argument is of numeric integer type then it is returned as the same value of `bigint` type
     * if the argument is of numeric `float`/`double` type then it is converted to `bigint` type
     * if the argument is of `string` type and can be parsed as integer then that integer value is returned,
       otherwise `null` is returned
     * if the argument is of `array`/`multiset`/`object` type then `null` is returned
     * type error is raised for all other input types

 * Example:

        {
          "v1": to_bigint(false),
          "v2": to_bigint(true),
          "v3": to_bigint(10),
          "v4": to_bigint(float("1e100")),
          "v5": to_bigint(double("1e1000")),
          "v6": to_bigint("20")
        };

 * The expected result is:

        { "v1": 0, "v2": 1, "v3": 10, "v4": 9223372036854775807, "v5": 9223372036854775807, "v6": 20 }

 The function has an alias `tobigint`.

### to_double ###
  * Syntax:

        to_double(expr)

  * Converts input value to a `double` value
  * Arguments:
     * `expr` : an expression
  * Return Value:
     * if the argument is `missing` then `missing` is returned
     * if the argument is `null` then `null` is returned
     * if the argument is of `boolean` type then `1.0` is returned if it is `true`, `0.0` if it is `false`
     * if the argument is of numeric type then it is returned as the value of `double` type
     * if the argument is of `string` type and can be parsed as `double` then that `double` value is returned,
       otherwise `null` is returned
     * if the argument is of `array`/`multiset`/`object` type then `null` is returned
     * type error is raised for all other input types

 * Example:

        {
          "v1": to_double(false),
          "v2": to_double(true),
          "v3": to_double(10),
          "v4": to_double(11.5),
          "v5": to_double("12.5")
        };

 * The expected result is:

        { "v1": 0.0, "v2": 1.0, "v3": 10.0, "v4": 11.5, "v5": 12.5 }

 The function has an alias `todouble`.

### to_number (to_num) ###
  * Syntax:

        to_number(expr)

  * Converts input value to a numeric value
  * Arguments:
     * `expr` : an expression
  * Return Value:
     * if the argument is `missing` then `missing` is returned
     * if the argument is `null` then `null` is returned
     * if the argument is of numeric type then it is returned as is
     * if the argument is of `boolean` type then `1` is returned if it is `true`, `0` if it is `false`
     * if the argument is of `string` type and can be parsed as `bigint` then that `bigint` value is returned,
       otherwise if it can be parsed as `double` then that `double` value is returned,
       otherwise `null` is returned
     * if the argument is of `array`/`multiset`/`object` type then `null` is returned
     * type error is raised for all other input types

 * Example:

        {
          "v1": to_number(false),
          "v2": to_number(true),
          "v3": to_number(10),
          "v4": to_number(11.5),
          "v5": to_number("12.5")
        };

 * The expected result is:

        { "v1": 0, "v2": 1, "v3": 10, "v4": 11.5, "v5": 12.5 }

 The function has three aliases: `tonumber`, `to_num`, and `tonum`.

### to_object (to_obj) ###
  * Syntax:

        to_object(expr)

  * Converts input value to an `object` value
  * Arguments:
     * `expr` : an expression
  * Return Value:
     * if the argument is `missing` then `missing` is returned
     * if the argument is `null` then `null` is returned
     * if the argument is of `object` type then it is returned as is
     * otherwise an empty `object` is returned

 * Example:

        {
          "v1": to_object({"value": "asterix"}),
          "v2": to_object("asterix")
        };

 * The expected result is:

        { "v1": {"value": "asterix"}, "v2": {} }

 The function has three aliases: `toobject`, `to_obj`, and `toobj`.

### to_string (to_str) ###
  * Syntax:

        to_string(expr)

  * Converts input value to a string value
  * Arguments:
     * `expr` : an expression
  * Return Value:
     * if the argument is `missing` then `missing` is returned
     * if the argument is `null` then `null` is returned
     * if the argument is of `boolean` type then `"true"` is returned if it is `true`, `"false"` if it is `false`
     * if the argument is of numeric type then its string representation is returned
     * if the argument is of `string` type then it is returned as is
     * if the argument is of `array`/`multiset`/`object` type then `null` is returned
     * type error is raised for all other input types

 * Example:

        {
          "v1": to_string(false),
          "v2": to_string(true),
          "v3": to_string(10),
          "v4": to_string(11.5),
          "v5": to_string("asterix")
        };

 * The expected result is:

        { "v1": "false", "v2": "true", "v3": "10", "v4": "11.5", "v5": "asterix" }

 The function has three aliases: `tostring`, `to_str`, and `tostr`.

