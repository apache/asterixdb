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

### string_concat ###
 * Syntax:

        string_concat(array)

 * Concatenates an array of strings `array` into a single string.
 * Arguments:
    * `array` : an `array` or `multiset` of `string`s (could be `null` or `missing`) to be concatenated.
 * Return Value:
    * the concatenated `string` value,
    * `missing` if the argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * `missing` if any element in the input array is `missing`,
    * `null` if any element in the input array is `null` but no element in the input array is `missing`,
    * any other non-array input value or non-integer element in the input array will cause a type error.

 * Example:

        string_concat(["ASTERIX", " ", "ROCKS!"]);


 * The expected result is:

        "ASTERIX ROCKS!"


### string_join ###
 * Syntax:

        string_join(array, string)

 * Joins an array or multiset of strings `array` with the given separator `string` into a single string.
 * Arguments:
    * `array` : an `array` or `multiset` of strings (could be `null`) to be joined.
    * `string` : a `string` to serve as the separator.
 * Return Value:
    * the joined `string`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * `missing` if the first argument array contains a `missing`,
    * `null` if the first argument array contains a `null` but does not contain a `missing`,
    * a type error will be raised if:
        * the first argument is any other non-array value, or contains any other non-string value,
        * or, the second argument is any other non-string value.

 * Example:

        string_join(["ASTERIX", "ROCKS~"], "!! ");


 * The expected result is:

        "ASTERIX!! ROCKS~"


### string_to_codepoint ###
 * Syntax:

        string_to_codepoint(string)

 * Converts the string `string` to its code_based representation.
 * Arguments:
    * `string` : a `string` that will be converted.
 * Return Value:
    * an `array` of the code points for the string `string`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-string input value will cause a type error.

 * Example:

        string_to_codepoint("Hello ASTERIX!");


 * The expected result is:

        [ 72, 101, 108, 108, 111, 32, 65, 83, 84, 69, 82, 73, 88, 33 ]


### codepoint_to_string ###
 * Syntax:

        codepoint_to_string(array)

 * Converts the ordered code_based representation `array` to the corresponding string.
 * Arguments:
    * `array` : an `array` of integer code_points.
 * Return Value:
    * a `string` representation of `array`.
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * `missing` if any element in the input array is `missing`,
    * `null` if any element in the input array is `null` but no element in the input array is `missing`,
    * any other non-array input value or non-integer element in the input array will cause a type error.

 * Example:

        codepoint_to_string([72, 101, 108, 108, 111, 32, 65, 83, 84, 69, 82, 73, 88, 33]);


 * The expected result is:

        "Hello ASTERIX!"


### substring_before ###
 * Syntax:

        substring_before(string, string_pattern)

 * Returns the substring from the given string `string` before the given pattern `string_pattern`.
 * Arguments:
    * `string` : a `string` to be extracted.
    * `string_pattern` : a `string` pattern to be searched.
 * Return Value:
    * a `string` that represents the substring,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.

 * Example:

        substring_before(" like x-phone", "x-phone");


 * The expected result is:

        " like "


### substring_after ###
 * Syntax:

       substring_after(string, string_pattern);

 * Returns the substring from the given string `string` after the given pattern `string_pattern`.
 * Arguments:
    * `string` : a `string` to be extracted.
    * `string_pattern` : a `string` pattern to be searched.
 * Return Value:
    * a `string` that represents the substring,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.


 * Example:

        substring_after(" like x-phone", "xph");


 * The expected result is:

        "one"

