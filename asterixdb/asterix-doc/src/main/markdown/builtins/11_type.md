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

## <a id="TypeFunctions">Type Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

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

