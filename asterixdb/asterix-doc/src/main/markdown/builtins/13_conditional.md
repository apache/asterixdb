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

### if_missing_or_null (ifmissingornull) ###

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

 The function has an alias `ifmissingornull`.
