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

## <a id="ConditionalFunctions">Conditional Functions</a> ##

### if_null (ifnull) ###

 * Syntax:

        if_null(expression1, expression2, ... expressionN)

 * Finds first argument which value is not `null` and returns that value
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * a `null` if all arguments evaluate to `null` or no arguments specified
    * a value of the first non-`null` argument otherwise

 * Example:

        {
            "a": if_null(),
            "b": if_null(null),
            "c": if_null(null, "asterixdb"),
            "d": is_missing(if_null(missing))
        };

 * The expected result is:

        { "a": null, "b": null, "c": "asterixdb", "d": true }

 The function has an alias `ifnull`.

### if_missing (ifmissing) ###

 * Syntax:

        if_missing(expression1, expression2, ... expressionN)

 * Finds first argument which value is not `missing` and returns that value
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * a `null` if all arguments evaluate to `missing` or no arguments specified
    * a value of the first non-`missing` argument otherwise

 * Example:

        {
            "a": if_missing(),
            "b": if_missing(missing),
            "c": if_missing(missing, "asterixdb"),
            "d": if_missing(null, "asterixdb")
        };

 * The expected result is:

        { "a": null, "b": null, "c": "asterixdb", "d": null }

 The function has an alias `ifmissing`.

### if_missing_or_null (ifmissingornull, coalesce) ###

 * Syntax:

        if_missing_or_null(expression1, expression2, ... expressionN)

 * Finds first argument which value is not `null` or `missing` and returns that value
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * a `null` if all arguments evaluate to either `null` or `missing`, or no arguments specified
    * a value of the first non-`null`, non-`missing` argument otherwise

* Example:

        {
            "a": if_missing_or_null(),
            "b": if_missing_or_null(null, missing),
            "c": if_missing_or_null(null, missing, "asterixdb")
        };

 * The expected result is:

        { "a": null, "b": null, "c": "asterixdb" }

 The function has two aliases: `ifmissingornull` and `coalesce`.

### if_inf (ifinf) ###

 * Syntax:

        if_inf(expression1, expression2, ... expressionN)

 * Finds first argument which is a non-infinite (`INF` or`-INF`) number
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * a `missing` if `missing` argument was encountered before the first non-infinite number argument
    * a `null` if `null` argument or any other non-number argument was encountered before the first non-infinite number argument
    * the first non-infinite number argument otherwise

 * Example:

        {
            "a": is_null(if_inf(null)),
            "b": is_missing(if_inf(missing)),
            "c": is_null(if_inf(double("INF"))),
            "d": if_inf(1, null, missing) ],
            "e": is_null(if_inf(null, missing, 1)) ],
            "f": is_missing(if_inf(missing, null, 1)) ],
            "g": if_inf(float("INF"), 1) ],
            "h": to_string(if_inf(float("INF"), double("NaN"), 1)) ]
        };

 * The expected result is:

        { "a": true, "b": true, "c": true, "d": 1, "e": true, "f": true, "g": 1, "h": "NaN" }

 The function has an alias `ifinf`.

### if_nan (ifnan) ###

 * Syntax:

        if_nan(expression1, expression2, ... expressionN)

 * Finds first argument which is a non-`NaN` number
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * a `missing` if `missing` argument was encountered before the first non-`NaN` number argument
    * a `null` if `null` argument or any other non-number argument was encountered before the first non-`NaN` number argument
    * the first non-`NaN` number argument otherwise

 * Example:

        {
            "a": is_null(if_nan(null)),
            "b": is_missing(if_nan(missing)),
            "c": is_null(if_nan(double("NaN"))),
            "d": if_nan(1, null, missing) ],
            "e": is_null(if_nan(null, missing, 1)) ],
            "f": is_missing(if_nan(missing, null, 1)) ],
            "g": if_nan(float("NaN"), 1) ],
            "h": to_string(if_nan(float("NaN"), double("INF"), 1)) ]
        };

 * The expected result is:

        { "a": true, "b": true, "c": true, "d": 1, "e": true, "f": true, "g": 1, "h": "INF" }

 The function has an alias `ifnan`.

### if_nan_or_inf (ifnanorinf) ###

 * Syntax:

        if_nan_or_inf(expression1, expression2, ... expressionN)

 * Finds first argument which is a non-infinite (`INF` or`-INF`) and non-`NaN` number
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * a `missing` if `missing` argument was encountered before the first non-infinite and non-`NaN` number argument
    * a `null` if `null` argument or any other non-number argument was encountered before the first non-infinite and non-`NaN` number argument
    * the first non-infinite and non-`NaN` number argument otherwise

 * Example:

        {
            "a": is_null(if_nan_or_inf(null)),
            "b": is_missing(if_nan_or_inf(missing)),
            "c": is_null(if_nan_or_inf(double("NaN"), double("INF"))),
            "d": if_nan_or_inf(1, null, missing) ],
            "e": is_null(if_nan_or_inf(null, missing, 1)) ],
            "f": is_missing(if_nan_or_inf(missing, null, 1)) ],
            "g": if_nan_or_inf(float("NaN"), float("INF"), 1) ],
        };

 * The expected result is:

        { "a": true, "b": true, "c": true, "d": 1, "e": true, "f": true, "g": 1 }

 The function has an alias `ifnanorinf`.


### null_if (nullif) ###

 * Syntax:

        null_if(expression1, expression2)

 * Compares two arguments and returns `null` if they are equal, otherwise returns the first argument.
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * `missing` if any argument is a `missing` value,
    * `null` if
        * any argument is a `null` value but no argument is a `missing` value, or
        * `argument1` = `argument2`
    * a value of the first argument otherwise

 * Example:

        {
            "a": null_if("asterixdb", "asterixdb"),
            "b": null_if(1, 2)
        };

 * The expected result is:

        { "a": null, "b": 1 }

 The function has an alias `nullif`.


### missing_if (missingif) ###

 * Syntax:

        missing_if(expression1, expression2)

 * Compares two arguments and returns `missing` if they are equal, otherwise returns the first argument.
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * `missing` if
        * any argument is a `missing` value, or
        * no argument is a `null` value and `argument1` = `argument2`
    * `null` if any argument is a `null` value but no argument is a `missing` value
    * a value of the first argument otherwise

 * Example:

        {
            "a": missing_if("asterixdb", "asterixdb")
            "b": missing_if(1, 2),
        };

 * The expected result is:

        { "b": 1 }

 The function has an alias `missingif`.


### nan_if (nanif) ###

 * Syntax:

        nan_if(expression1, expression2)

 * Compares two arguments and returns `NaN` value if they are equal, otherwise returns the first argument.
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value
    * `NaN` value of type `double` if `argument1` = `argument2`
    * a value of the first argument otherwise

 * Example:

        {
            "a": to_string(nan_if("asterixdb", "asterixdb")),
            "b": nan_if(1, 2)
        };

 * The expected result is:

        { "a": "NaN", "b": 1 }

 The function has an alias `nanif`.


### posinf_if (posinfif) ###

 * Syntax:

        posinf_if(expression1, expression2)

 * Compares two arguments and returns `+INF` value if they are equal, otherwise returns the first argument.
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value
    * `+INF` value of type `double` if `argument1` = `argument2`
    * a value of the first argument otherwise

 * Example:

        {
            "a": to_string(posinf_if("asterixdb", "asterixdb")),
            "b": posinf_if(1, 2)
        };

 * The expected result is:

        { "a": "+INF", "b": 1 }

 The function has an alias `posinfif`.


### neginf_if (neginfif) ###

 * Syntax:

        neginf_if(expression1, expression2)

 * Compares two arguments and returns `-INF` value if they are equal, otherwise returns the first argument.
 * Arguments:
    * `expressionI` : an expression (any type is allowed).
 * Return Value:
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value
    * `-INF` value of type `double` if `argument1` = `argument2`
    * a value of the first argument otherwise

 * Example:

        {
            "a": to_string(neginf_if("asterixdb", "asterixdb")),
            "b": neginf_if(1, 2)
        };

 * The expected result is:

        { "a": "-INF", "b": 1 }

 The function has an alias `neginfif`.
