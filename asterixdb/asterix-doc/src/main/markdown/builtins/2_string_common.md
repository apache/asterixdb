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

## <a id="StringFunctions">String Functions</a> ##
### concat ###
 * Syntax:

        concat(string1, string2, ...)

 * Returns a concatenated string from arguments.
 * Arguments:
    * `string1`: a string value,
    * `string2`: a string value,
    * ....
 * Return Value:
    * a concatenated string from arguments,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.

 * Example:

        concat("test ", "driven ", "development");


 * The expected result is:

        "test driven development"


### contains ###
 * Syntax:

        contains(string, substring_to_contain)

 * Checks whether the string `string` contains the string `substring_to_contain`
 * Arguments:
    * `string` : a `string` that might contain the given substring,
    * `substring_to_contain` : a target `string` that might be contained.
 * Return Value:
    * a `boolean` value, `true` if `string` contains `substring_to_contain`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error,
    * `false` otherwise.

 * Note: an [n_gram index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
 * Example:

        { "v1": contains("I like x-phone", "phone"), "v2": contains("one", "phone") };


 * The expected result is:

        { "v1": true, "v2": false }


### ends_with ###
 * Syntax:

        ends_with(string, substring_to_end_with)

 * Checks whether the string `string` ends with the string `substring_to_end_with`.
 * Arguments:
    * `string` : a `string` that might end with the given string,
    * `substring_to_end_with` : a `string` that might be contained as the ending substring.
 * Return Value:
    * a `boolean` value, `true` if `string` contains `substring_to_contain`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error,
    * `false` otherwise.

 * Example:

        {
          "v1": ends_with(" love product-b its shortcut_menu is awesome:)", ":)"),
          "v2": ends_with(" awsome:)", ":-)")
        };


 * The expected result is:

        { "v1": true, "v2": false }


### initcap (or title) ###
 * Syntax:

        initcap(string)

 * Converts a given string `string` so that the first letter of each word is uppercase and
   every other letter is lowercase.
    The function has an alias called "title".
 * Arguments:
    * `string` : a `string` to be converted.
 * Return Value:
    * a `string` as the title form of the given `string`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-string input value will cause a type error.

 * Example:

        { "v1": initcap("ASTERIXDB is here!"), "v2": title("ASTERIXDB is here!") };


 * The expected result is:

        { "v1": "Asterixdb Is Here!", "v2": "Asterixdb Is Here!" }


### length ###
 * Syntax:

        length(string)

 * Returns the length of the string `string`.
 * Arguments:
    * `string` : a `string` or `null` that represents the string to be checked.
 * Return Value:
    * an `bigint` that represents the length of `string`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-string input value will cause a type error.

 * Example:

        length("test string");


 * The expected result is:

        11


### lower ###
 * Syntax:

        lower(string)

 * Converts a given string `string` to its lowercase form.
 * Arguments:
    * `string` : a `string` to be converted.
 * Return Value:
    * a `string` as the lowercase form of the given `string`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-string input value will cause a type error.

 * Example:

        lower("ASTERIXDB");


 * The expected result is:

        "asterixdb"


### ltrim ###
 * Syntax:

        ltrim(string[, chars]);

 * Returns a new string with all leading characters that appear in `chars` removed.
   By default, white space is the character to trim.
 * Arguments:
    * `string` : a `string` to be trimmed,
    * `chars` : a `string` that contains characters that are used to trim.
 * Return Value:
    * a trimmed, new `string`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.


 * Example:

        ltrim("me like x-phone", "eml");


 * The expected result is:

        " like x-phone"


### position ###
 * Syntax:

        position(string, string_pattern)

 * Returns the first position of `string_pattern` within `string`.
 * Arguments:
    * `string` : a `string` that might contain the pattern,
    * `string_pattern` : a pattern `string` to be matched.
 * Return Value:
    * the first position that `string_pattern` appears within `string`
      (starting at 0), or -1 if it does not appear,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.

 * Example:

        {
          "v1": position("ppphonepp", "phone"),
          "v2": position("hone", "phone")
        };


 * The expected result is:

        { "v1": 3, "v2": -1 }


### regexp_contains ###
 * Syntax:

        regexp_contains(string, string_pattern[, string_flags])

 * Checks whether the strings `string` contains the regular expression
    pattern `string_pattern` (a Java regular expression pattern).
 * Arguments:
    * `string` : a `string` that might contain the pattern,
    * `string_pattern` : a pattern `string` to be matched,
    * `string_flag` : (Optional) a `string` with flags to be used during regular expression matching.
        * The following modes are enabled with these flags: dotall (s), multiline (m), case_insensitive (i), and comments and whitespace (x).
 * Return Value:
    * a `boolean`, returns `true` if `string` contains the pattern `string_pattern`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error,
    * `false` otherwise.

 * Example:

        {
          "v1": regexp_contains("pphonepp", "p*hone"),
          "v2": regexp_contains("hone", "p+hone")
        }


 * The expected result is:

        { "v1": true, "v2": false }


### regexp_like ###
 * Syntax:

        regexp_like(string, string_pattern[, string_flags])

 * Checks whether the string `string` exactly matches the regular expression pattern `string_pattern`
   (a Java regular expression pattern).
 * Arguments:
    * `string` : a `string` that might contain the pattern,
    * `string_pattern` : a pattern `string` that might be contained,
    * `string_flag` : (Optional) a `string` with flags to be used during regular expression matching.
        * The following modes are enabled with these flags: dotall (s), multiline (m), case_insensitive (i), and comments and whitespace (x).
 * Return Value:
    * a `boolean` value, `true` if `string` contains the pattern `string_pattern`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error,
    * `false` otherwise.

 * Example:

        {
          "v1": regexp_like(" can't stand acast the network is horrible:(", ".*acast.*"),
          "v2": regexp_like("acast", ".*acst.*")
        };

 * The expected result is:

        { "v1": true, "v2": false }


### regexp_position ###
 * Syntax:

        regexp_position(string, string_pattern[, string_flags])

 * Returns first position of the regular expression `string_pattern` (a Java regular expression pattern)
   within `string`.
 * Arguments:
    * `string` : a `string` that might contain the pattern,
    * `string_pattern` : a pattern `string` to be matched,
    * `string_flag` : (Optional) a `string` with flags to be used during regular expression matching.
        * The following modes are enabled with these flags: dotall (s), multiline (m), case_insensitive (i), and comments and whitespace (x).
 * Return Value:
    * the first position that the regular expression `string_pattern` appears in `string`
      (starting at 0), or -1 if it does not appear.
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.

 * Example:

        {
          "v1": regexp_position("pphonepp", "p*hone"),
          "v2": regexp_position("hone", "p+hone")
        };


 * The expected result is:

        { "v1": 1, "v2": -1 }


### regexp_replace ###
 * Syntax:

        regexp_replace(string, string_pattern, string_replacement[, string_flags])
        regexp_replace(string, string_pattern, string_replacement[, replacement_limit])

 * Checks whether the string `string` matches the given
   regular expression pattern `string_pattern` (a Java regular expression pattern),
   and replaces the matched pattern `string_pattern` with the new pattern `string_replacement`.
 * Arguments:
    * `string` : a `string` that might contain the pattern,
    * `string_pattern` : a pattern `string` to be matched,
    * `string_replacement` : a pattern `string` to be used as the replacement,
    * `string_flag` : (Optional) a `string` with flags to be used during replace.
        * The following modes are enabled with these flags: dotall (s), multiline (m), case_insensitive (i), and comments and whitespace (x).
    * `replacement_limit`: (Optional) an `integer` specifying the maximum number of replacements to make
         (if negative then all occurrences will be replaced)
 * Return Value:
    * Returns a `string` that is obtained after the replacements,
    * `missing` if any argument is a `missing` value,
    * any other non-string input value will cause a type error,
    * `null` if any argument is a `null` value but no argument is a `missing` value.

 * Example:

        regexp_replace(" like x-phone the voicemail_service is awesome", " like x-phone", "like product-a")


 * The expected result is:

        "like product-a the voicemail_service is awesome"


### repeat ###
 * Syntax:

        repeat(string, n)

 * Returns a string formed by repeating the input `string` `n` times.
 * Arguments:
    * `string` : a `string` to be repeated,
    * `n` : an `tinyint`/`smallint`/`integer`/`bigint` value - how many times the string should be repeated.
 * Return Value:
    * a string that repeats the input `string` `n` times,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-string value,
        * or, the second argument is not a `tinyint`, `smallint`, `integer`, or `bigint`.

 * Example:

        repeat("test", 3);


 * The expected result is:

        "testtesttest"

### replace ###
 * Syntax:

        replace(string, search_string, replacement_string[, limit])

 * Finds occurrences of the given substring `search_string` in the input string `string`
   and replaces them with the new substring `replacement_string`.
 * Arguments:
    * `string` : an input `string`,
    * `search_string` : a `string`  substring to be searched for,
    * `replacement_string` : a `string` to be used as the replacement,
    * `limit` : (Optional) an `integer` - maximum number of occurrences to be replaced.
                If not specified or negative then all occurrences will be replaced
 * Return Value:
    * Returns a `string` that is obtained after the replacements,
    * `missing` if any argument is a `missing` value,
    * any other non-string input value or non-integer `limit` will cause a type error,
    * `null` if any argument is a `null` value but no argument is a `missing` value.

 * Example:

        {
          "v1": replace(" like x-phone the voicemail_service is awesome", " like x-phone", "like product-a"),
          "v2": replace("x-phone and x-phone", "x-phone", "product-a", 1)
        };

 * The expected result is:

        {
          "v1": "like product-a the voicemail_service is awesome",
          "v2": "product-a and x-phone"
        }

### reverse ###
 * Syntax:

        reverse(string)

 * Returns a string formed by reversing characters in the input `string`.
 * Arguments:
    * `string` : a `string` to be reversed
 * Return Value:
    * a string containing characters from the the input `string` in the reverse order,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-string value

 * Example:

        reverse("hello");


 * The expected result is:

        "olleh"

### rtrim ###
 * Syntax:

        rtrim(string[, chars]);

 * Returns a new string with all trailing characters that appear in `chars` removed.
   By default, white space is the character to trim.
 * Arguments:
    * `string` : a `string` to be trimmed,
    * `chars` : a `string` that contains characters that are used to trim.
 * Return Value:
    * a trimmed, new `string`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.


 * Example:

        {
          "v1": rtrim("i like x-phone", "x-phone"),
          "v2": rtrim("i like x-phone", "onexph")
        };

 * The expected result is:

        { "v1": "i like ", "v2": "i like " }

### split ###
 * Syntax:

        split(string, sep)

 * Splits the input `string` into an array of substrings separated by the string `sep`.
 * Arguments:
    * `string` : a `string` to be split.
 * Return Value:
    * an array of substrings by splitting the input `string` by `sep`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-string input value will cause a type error.

 * Example:

        split("test driven development", " ");


 * The expected result is:

        [ "test", "driven", "development" ]


### starts_with ###
 * Syntax:

        starts_with(string, substring_to_start_with)

 * Checks whether the string `string` starts with the string `substring_to_start_with`.
 * Arguments:
    * `string` : a `string` that might start with the given string.
    * `substring_to_start_with` : a `string` that might be contained as the starting substring.
 * Return Value:
    * a `boolean`, returns `true` if `string` starts with the string `substring_to_start_with`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error,
    * `false` otherwise.

 * Example:

        {
          "v1" : starts_with(" like the plan, amazing", " like"),
          "v2" : starts_with("I like the plan, amazing", " like")
        };


 * The expected result is:

        { "v1": true, "v2": false }


### substr ###
 * Syntax:

        substr(string, offset[, length])

 * Returns the substring from the given string `string` based on the given start offset `offset` with the optional `length`.
 * Arguments:
    * `string` : a `string` to be extracted,
    * `offset` : an `tinyint`/`smallint`/`integer`/`bigint` value as the starting offset of the substring in `string`
                 (starting at 0). If negative then counted from the end of the string,
    * `length` : (Optional) an an `tinyint`/`smallint`/`integer`/`bigint` value as the length of the substring.
 * Return Value:
    * a `string` that represents the substring,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value, or if the substring could not
             be obtained because the starting offset is not within string bounds or `length` is negative.
    * a type error will be raised if:
        * the first argument is any other non-string value,
        * or, the second argument is not a `tinyint`, `smallint`, `integer`, or `bigint`,
        * or, the third argument is not a `tinyint`, `smallint`, `integer`, or `bigint` if the argument is present.

 * Example:

        substr("test string", 6, 3);


 * The expected result is:

        "str"

The function has an alias `substring`.

### trim ###
 * Syntax:

        trim(string[, chars]);

 * Returns a new string with all leading characters that appear in `chars` removed.
   By default, white space is the character to trim.
 * Arguments:
    * `string` : a `string` to be trimmed,
    * `chars` : a `string` that contains characters that are used to trim.
 * Return Value:
    * a trimmed, new `string`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.


 * Example:

        trim("i like x-phone", "xphoen");


 * The expected result is:

        " like "


### upper ###
 * Syntax:

        upper(string)

 * Converts a given string `string` to its uppercase form.
 * Arguments:
    * `string` : a `string` to be converted.
 * Return Value:
    * a `string` as the uppercase form of the given `string`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-string input value will cause a type error.

 * Example:

        upper("hello")


 * The expected result is:

        "HELLO"

