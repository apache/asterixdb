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

## <a id="TypeFunctions">Type Functions</a> ##

### is_array (isarray) ###
 * Syntax:

        is_array(expr)

 * Checks whether the given expression is evaluated to be an `array` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is an `array` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_array(true),
          "b": is_array(false),
          "c": isarray(null),
          "d": isarray(missing),
          "e": isarray("d"),
          "f": isarray(4.0),
          "g": isarray(5),
          "h": isarray(["1", 2]),
          "i": isarray({"a":1})
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": false, "f": false, "g": false, "h": true, "i": false }

 The function has an alias `isarray`.

### is_atomic (isatomic, isatom) ###
 * Syntax:

        is_atomic(expr)

 * Checks whether the given expression is evaluated to be a value of a [primitive](../datamodel.html#PrimitiveTypes) type.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a primitive type or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_atomic(true),
          "b": is_atomic(false),
          "c": isatomic(null),
          "d": isatomic(missing),
          "e": isatomic("d"),
          "f": isatom(4.0),
          "g": isatom(5),
          "h": isatom(["1", 2]),
          "i": isatom({"a":1})
        };

* The expected result is:

        { "a": true, "b": true, "c": null, "e": true, "f": true, "g": true, "h": false, "i": false }

 The function has two aliases, `isatomic` or `isatom`.

### is_boolean (isboolean, isbool) ###
 * Syntax:

        is_boolean(expr)

 * Checks whether the given expression is evaluated to be a `boolean` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `boolean` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": isboolean(true),
          "b": isboolean(false),
          "c": is_boolean(null),
          "d": is_boolean(missing),
          "e": isbool("d"),
          "f": isbool(4.0),
          "g": isbool(5),
          "h": isbool(["1", 2]),
          "i": isbool({"a":1})
        };


 * The expected result is:

        { "a": true, "b": true, "c": null, "e": false, "f": false, "g": false, "h": false, "i": false }

 The function has two aliases, `isboolean` or `isbool`.


### is_number (isnumber, isnum) ###
 * Syntax:

        is_number(expr)

 * Checks whether the given expression is evaluated to be a numeric value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `smallint`/`tinyint`/`integer`/`bigint`/`float`/`double`
      value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_number(true),
          "b": is_number(false),
          "c": isnumber(null),
          "d": isnumber(missing),
          "e": isnumber("d"),
          "f": isnum(4.0),
          "g": isnum(5),
          "h": isnum(["1", 2]),
          "i": isnum({"a":1})
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": false, "f": true, "g": true, "h": false, "i": false }

 The function has two aliases, `isnumber` or `isnum`.

### is_object (isobject, isobj) ###
 * Syntax:

        is_object(expr)

 * Checks whether the given expression is evaluated to be a `object` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `object` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_object(true),
          "b": is_object(false),
          "c": isobject(null),
          "d": isobject(missing),
          "e": isobj("d"),
          "f": isobj(4.0),
          "g": isobj(5),
          "h": isobj(["1", 2]),
          "i": isobj({"a":1})
        };


 * The expected result is:

       { "a": false, "b": false, "c": null, "e": false, "f": false, "g": false, "h": false, "i": true }

 The function has two aliases, `isobject` or `isobj`.


### is_string (isstring, isstr) ###
 * Syntax:

        is_string(expr)

 * Checks whether the given expression is evaluated to be a `string` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `string` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_string(true),
          "b": isstring(false),
          "c": isstring(null),
          "d": isstr(missing),
          "e": isstr("d"),
          "f": isstr(4.0),
          "g": isstr(5),
          "h": isstr(["1", 2]),
          "i": isstr({"a":1})
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": true, "f": false, "g": false, "h": false, "i": false }

 The function has two aliases, `isstring` or `isstr`.


### is_null ###
 * Syntax:

        is_null(expr)

 * Checks whether the given expression is evaluated to be a `null` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the variable is a `null` or not,
    * a `missing` if the input is `missing`.

 * Example:

        { "v1": is_null(null), "v2": is_null(1), "v3": is_null(missing) };


 * The expected result is:

        { "v1": true, "v2": false }

 The function has an alias `isnull`.

### is_missing ###
 * Syntax:

        is_missing(expr)

 * Checks whether the given expression is evaluated to be a `missing` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the variable is a `missing` or not.

 * Example:

        { "v1": is_missing(null), "v2": is_missing(1), "v3": is_missing(missing) };


 * The expected result is:

        { "v1": false, "v2": false, "v3": true }

 The function has an alias `ismissing`.

### is_unknown ###
 * Syntax:

        is_unknown(expr)

 * Checks whether the given variable is a `null` value or a `missing` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the variable is a `null`/``missing` value (`true`) or not (`false`).

 * Example:

        { "v1": is_unknown(null), "v2": is_unknown(1), "v3": is_unknown(missing) };


 * The expected result is:

        { "v1": true, "v2": false, "v3": true }

 The function has an alias `isunknown`.


### to_boolean ###
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

 The function has an alias `toboolean`.


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


### to_string ###
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

 The function has an alias `tostring`.
