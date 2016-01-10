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

# Asterix: Using Functions #

## <a id="toc">Table of Contents</a> ##

* [Numeric Functions](#NumericFunctions)
* [String Functions](#StringFunctions)
* [Aggregate Functions](#AggregateFunctions)
* [Spatial Functions](#SpatialFunctions)
* [Similarity Functions](#SimilarityFunctions)
* [Tokenizing Functions](#TokenizingFunctions)
* [Temporal Functions](#TemporalFunctions)
* [Record Functions](#RecordFunctions)
* [Other Functions](#OtherFunctions)

Asterix provides various classes of functions to support operations on numeric, string, spatial, and temporal data. This document explains how to use these functions.

## <a id="NumericFunctions">Numeric Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### abs ###
 * Syntax:

        abs(numeric_value)

 * Computes the absolute value of the argument.
 * Arguments:
    * `numeric_value`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
 * Return Value:
    * The absolute value of the argument with the same type as the input argument, or `null` if the argument is a `null` value.

 * Example:

        let $v1 := abs(2013)
        let $v2 := abs(-4036)
        let $v3 := abs(0)
        let $v4 := abs(float("-2013.5"))
        let $v5 := abs(double("-2013.593823748327284"))
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5 }


 * The expected result is:

        { "v1": 2013, "v2": 4036, "v3": 0, "v4": 2013.5f, "v5": 2013.5938237483274d }


### ceiling ###
 * Syntax:

        ceiling(numeric_value)

 * Computes the smallest (closest to negative infinity) number with no fractional part that is not less than the value of the argument. If the argument is already equal to mathematical integer, then the result is the same as the argument.
 * Arguments:
    * `numeric_value`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
 * Return Value:
    * The ceiling value for the given number in the same type as the input argument, or `null` if the input is `null`.

 * Example:

        let $v1 := ceiling(2013)
        let $v2 := ceiling(-4036)
        let $v3 := ceiling(0.3)
        let $v4 := ceiling(float("-2013.2"))
        let $v5 := ceiling(double("-2013.893823748327284"))
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5 }


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 1.0d, "v4": -2013.0f, "v5": -2013.0d }


### floor ###
 * Syntax:

        floor(numeric_value)

 * Computes the largest (closest to positive infinity) number with no fractional part that is not greater than the value. If the argument is already equal to mathematical integer, then the result is the same as the argument.
 * Arguments:
    * `numeric_value`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
 * Return Value:
    * The floor value for the given number in the same type as the input argument, or `null` if the input is `null`.

 * Example:

        let $v1 := floor(2013)
        let $v2 := floor(-4036)
        let $v3 := floor(0.8)
        let $v4 := floor(float("-2013.2"))
        let $v5 := floor(double("-2013.893823748327284"))
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5 }


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 0.0d, "v4": -2014.0f, "v5": -2014.0d }


### round ###
 * Syntax:

        round(numeric_value)

 * Computes the number with no fractional part that is closest (and also closest to positive infinity) to the argument.
 * Arguments:
    * `numeric_value`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
 * Return Value:
    * The rounded value for the given number in the same type as the input argument, or `null` if the input is `null`.

 * Example:

        let $v1 := round(2013)
        let $v2 := round(-4036)
        let $v3 := round(0.8)
        let $v4 := round(float("-2013.256"))
        let $v5 := round(double("-2013.893823748327284"))
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5 }


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 1.0d, "v4": -2013.0f, "v5": -2014.0d }


### round-half-to-even ###
 * Syntax:

        round-half-to-even(numeric_value, [precision])

 * Computes the closest numeric value to `numeric_value` that is a multiple of ten to the power of minus `precision`. `precision` is optional and by default value `0` is used.
 * Arguments:
    * `numeric_value`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
    * `precision`: An optional integer field representing the number of digits in the fraction of the the result
 * Return Value:
    * The rounded value for the given number in the same type as the input argument, or `null` if the input is `null`.

 * Example:

        let $v1 := round-half-to-even(2013)
        let $v2 := round-half-to-even(-4036)
        let $v3 := round-half-to-even(0.8)
        let $v4 := round-half-to-even(float("-2013.256"))
        let $v5 := round-half-to-even(double("-2013.893823748327284"))
        let $v6 := round-half-to-even(double("-2013.893823748327284"), 2)
        let $v7 := round-half-to-even(2013, 4)
        let $v8 := round-half-to-even(float("-2013.256"), 5)
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5, "v6": $v6, "v7": $v7, "v8": $v8 }


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 1.0d, "v4": -2013.0f, "v5": -2014.0d, "v6": -2013.89d, "v7": 2013, "v8": -2013.256f }


## <a id="StringFunctions">String Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### string-to-codepoint ###
 * Syntax:

        string-to-codepoint(string)

 * Converts the string `string` to its code-based representation.
 * Arguments:
    * `string` : A `string` that will be converted.
 * Return Value:
    * An `OrderedList` of the code points for the string `string`.

### codepoint-to-string ###
 * Syntax:

        codepoint-to-string(list)

 * Converts the ordered code-based representation `list` to the corresponding string.
 * Arguments:
    * `list` : An `OrderedList` of code-points.
 * Return Value:
    * A `string` representation of `list`.

 * Example:

        use dataverse TinySocial;

        let $s := "Hello ASTERIX!"
        let $l := string-to-codepoint($s)
        let $ss := codepoint-to-string($l)
        return {"codes": $l, "string": $ss}


 * The expected result is:

        { "codes": [ 72, 101, 108, 108, 111, 32, 65, 83, 84, 69, 82, 73, 88, 33 ], "string": "Hello ASTERIX!" }


### contains ###
 * Syntax:

        contains(string, substring_to_contain)

 * Checks whether the string `string` contains the string `substring_to_contain`
 * Arguments:
    * `string` : A `string` that might contain the given substring.
    * `substring_to_contain` : A target `string` that might be contained.
 * Return Value:
    * A `boolean` value, `true` if `string` contains `substring_to_contain`, and `false` otherwise.
 * Note: An [n-gram index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where contains($i.message, "phone")
        return {"mid": $i.message-id, "message": $i.message}


 * The expected result is:

        { "mid": 2, "message": " dislike iphone its touch-screen is horrible" }
        { "mid": 13, "message": " dislike iphone the voice-command is bad:(" }
        { "mid": 15, "message": " like iphone the voicemail-service is awesome" }


### like ###
 * Syntax:

        like(string, string_pattern)

 * Checks whether the string `string` contains the string pattern `string_pattern`. Compared to the `contains` function, the `like` function also supports regular expressions.
 * Arguments:
    * `string` : A `string` that might contain the pattern or `null`.
    * `string_pattern` : A pattern `string` that might be contained or `null`.
 * Return Value:
    * A `boolean` value, `true` if `string` contains the pattern `string_pattern`, and `false` otherwise.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where like($i.message, "%at&t%")
        return $i.message


 * The expected result is:

        " can't stand at&t the network is horrible:("
        " can't stand at&t its plan is terrible"
        " love at&t its 3G is good:)"


### starts-with ###
 * Syntax:

        starts-with(string, substring_to_start_with)

 * Checks whether the string `string` starts with the string `substring_to_start_with`.
 * Arguments:
    * `string` : A `string` that might start with the given string.
    * `substring_to_start_with` : A `string` that might be contained as the starting substring.
 * Return Value:
    * A `boolean`, returns `true` if `string` starts with the string `substring_to_start_with`, and `false` otherwise.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where starts-with($i.message, " like")
        return $i.message


 * The expected result is:

        " like samsung the plan is amazing"
        " like t-mobile its platform is mind-blowing"
        " like verizon the 3G is awesome:)"
        " like iphone the voicemail-service is awesome"


### ends-with ###
 * Syntax:

        ends-with(string, substring_to_end_with)

 * Checks whether the string `string` ends with the string `substring_to_end_with`.
 * Arguments:
    * `string` : A `string` that might end with the given string.
    * `substring_to_end_with` : A `string` that might be contained as the ending substring.
 * Return Value:
    * A `boolean`, returns `true` if `string` ends with the string `substring_to_end_with`, and `false` otherwise.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where ends-with($i.message, ":)")
        return $i.message


 * The expected result is:

        " love sprint its shortcut-menu is awesome:)"
        " like verizon the 3G is awesome:)"
        " love at&t its 3G is good:)"


### string-concat ###
 * Syntax:

        string-concat(list)

 * Concatenates a list of strings `list` into a single string.
 * Arguments:
    * `list` : An `OrderedList` or `UnorderedList` of `string`s (could be `null`) to be concatenated.
 * Return Value:
    * Returns the concatenated `string` value.

 * Example:

        let $i := "ASTERIX"
        let $j := " "
        let $k := "ROCKS!"
        return string-concat([$i, $j, $k])


 * The expected result is:

        "ASTERIX ROCKS!"


### string-join ###
 * Syntax:

        string-join(list, string)

 * Joins a list of strings `list` with the given separator `string` into a single string.
 * Arguments:
    * `list` : An `OrderedList` or `UnorderedList` of strings (could be `null`) to be joined.
    * `string` : A `string` as the separator.
 * Return Value:
    * Returns the joined `String`.

 * Example:

        use dataverse TinySocial;

        let $i := ["ASTERIX", "ROCKS~"]
        return string-join($i, "!! ")


 * The expected result is:

        "ASTERIX!! ROCKS~"


### lowercase ###
 * Syntax:

        lowercase(string)

 * Converts a given string `string` to its lowercase form.
 * Arguments:
    * `string` : A `string` to be converted.
 * Return Value:
    * Returns a `string` as the lowercase form of the given `string`.

 * Example:

        use dataverse TinySocial;

        let $i := "ASTERIX"
        return lowercase($i)


 * The expected result is:

        asterix

### uppercase ###
 * Syntax:

        uppercase(string)

 * Converts a given string `string` to its uppercase form.
 * Arguments:
    * `string` : A `string` to be converted.
 * Return Value:
    * Returns a `string` as the uppercase form of the given `string`.

 * Example:

        use dataverse TinySocial;

        let $i := "asterix"
        return uppercase($i)


 * The expected result is:

        ASTERIX

### matches ###
 * Syntax:

        matches(string, string_pattern)

 * Checks whether the strings `string` matches the given pattern `string_pattern` (A Java regular expression pattern).
 * Arguments:
    * `string` : A `string` that might contain the pattern.
    * `string_pattern` : A pattern `string` to be matched.
 * Return Value:
    * A `boolean`, returns `true` if `string` matches the pattern `string_pattern`, and `false` otherwise.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where matches($i.message, "dislike iphone")
        return $i.message


 * The expected result is:

        " dislike iphone its touch-screen is horrible"
        " dislike iphone the voice-command is bad:("


### replace ###
 * Syntax:

        replace(string, string_pattern, string_replacement[, string_flags])

 * Checks whether the string `string` matches the given pattern `string_pattern`, and replace the matched pattern `string_pattern` with the new pattern `string_replacement`.
 * Arguments:
    * `string` : A `string` that might contain the pattern.
    * `string_pattern` : A pattern `string` to be matched.
    * `string_replacement` : A pattern `string` to be used as the replacement.
    * `string_flag` : (Optional) A `string` with flags to be used during replace.
       * The following modes are enabled with these flags: dotall (s), multiline (m), case-insenitive (i), and comments and whitespace (x).
 * Return Value:
    * Returns a `string` that is obtained after the replacements.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where matches($i.message, " like iphone")
        return replace($i.message, " like iphone", "like android")


 * The expected result is:

        "like android the voicemail-service is awesome"


### string-length ###
 * Syntax:

        string-length(string)

 * Returns the length of the string `string`.
 * Arguments:
    * `string` : A `string` or `null` that represents the string to be checked.
 * Return Value:
    * An `int64` that represents the length of `string`.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        return {"mid": $i.message-id, "message-len": string-length($i.message)}


 * The expected result is:

        { "mid": 1, "message-len": 43 }
        { "mid": 2, "message-len": 44 }
        { "mid": 3, "message-len": 33 }
        { "mid": 4, "message-len": 43 }
        { "mid": 5, "message-len": 46 }
        { "mid": 6, "message-len": 43 }
        { "mid": 7, "message-len": 37 }
        { "mid": 8, "message-len": 33 }
        { "mid": 9, "message-len": 34 }
        { "mid": 10, "message-len": 50 }
        { "mid": 11, "message-len": 38 }
        { "mid": 12, "message-len": 52 }
        { "mid": 13, "message-len": 42 }
        { "mid": 14, "message-len": 27 }
        { "mid": 15, "message-len": 45 }


### substring ###
 * Syntax:

        substring(string, offset[, length])

 * Returns the substring from the given string `string` based on the given start offset `offset` with the optional `length`.
 * Arguments:
    * `string` : A `string` to be extracted.
    * `offset` : An `int64` as the starting offset of the substring in `string`.
    * `length` : (Optional) An `int64` as the length of the substring.
 * Return Value:
    * A `string` that represents the substring.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where string-length($i.message) > 50
        return substring($i.message, 50)


 * The expected result is:

        "G:("


### substring-before ###
 * Syntax:

        substring-before(string, string_pattern)

 * Returns the substring from the given string `string` before the given pattern `string_pattern`.
 * Arguments:
    * `string` : A `string` to be extracted.
    * `string_pattern` : A `string` pattern to be searched.
 * Return Value:
    * A `string` that represents the substring.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where contains($i.message, "iphone")
        return substring-before($i.message, "iphone")


 * The expected result is:

        " dislike "
        " dislike "
        " like "


### substring-after ###
 * Syntax:

        substring-after(string, string_pattern)

 * Returns the substring from the given string `string` after the given pattern `string_pattern`.
 * Arguments:
    * `string` : A `string` to be extracted.
    * `string_pattern` : A `string` pattern to be searched.
 * Return Value:
    * A `string` that represents the substring.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where contains($i.message, "iphone")
        return substring-after($i.message, "iphone")


 * The expected result is:

        " its touch-screen is horrible"
        " the voice-command is bad:("
        " the voicemail-service is awesome"

## <a id="AggregateFunctions">Aggregate Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### count ###
 * Syntax:

        count(list)

 * Gets the number of items in the given list.
 * Arguments:
    * `list`: An `orderedList` or `unorderedList` containing the items to be counted, or a `null` value.
 * Return Value:
    * An `int64` value representing the number of items in the given list. `0i64` is returned if the input is `null`.

 * Example:

        use dataverse TinySocial;

        let $l1 := ['hello', 'world', 1, 2, 3]
        let $l2 := for $i in dataset TwitterUsers return $i
        return {"count1": count($l1), "count2": count($l2)}

 * The expected result is:

        { "count1": 5i64, "count2": 4i64 }

### avg ###
 * Syntax:

        avg(num_list)

 * Gets the average value of the items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing numeric or null values, or a `null` value.
 * Return Value:
    * An `double` value representing the average of the numbers in the given list. `null` is returned if the input is `null`, or the input list contains `null`. Non-numeric types in the input list will cause an error.

 * Example:

        use dataverse TinySocial;

        let $l := for $i in dataset TwitterUsers return $i.friends_count
        return {"avg_friend_count": avg($l)}

 * The expected result is:

        { "avg_friend_count": 191.5d }

### sum ###
 * Syntax:

        sum(num_list)

 * Gets the sum of the items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing numeric or null values, or a `null` value.
 * Return Value:
    * The sum of the numbers in the given list. The returning type is decided by the item type with the highest order in the numeric type promotion order (`int8`-> `int16`->`int32`->`int64`->`float`->`double`) among items. `null` is returned if the input is `null`, or the input list contains `null`. Non-numeric types in the input list will cause an error.

 * Example:

        use dataverse TinySocial;

        let $l := for $i in dataset TwitterUsers return $i.friends_count
        return {"sum_friend_count": sum($l)}

 * The expected result is:

        { "sum_friend_count": 766 }

### min/max ###
 * Syntax:

        min(num_list), max(num_list)

 * Gets the min/max value of numeric items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing the items to be compared, or a `null` value.
 * Return Value:
    * The min/max value of the given list. The returning type is decided by the item type with the highest order in the numeric type promotion order (`int8`-> `int16`->`int32`->`int64`->`float`->`double`) among items. `null` is returned if the input is `null`, or the input list contains `null`. Non-numeric types in the input list will cause an error.

 * Example:

        use dataverse TinySocial;

        let $l := for $i in dataset TwitterUsers return $i. friends_count
        return {"min_friend_count": min($l), "max_friend_count": max($l)}

 * The expected result is:

        { "min_friend_count": 18, "max_friend_count": 445 }


### sql-count ###
 * Syntax:

        sql-count(list)

 * Gets the number of non-null items in the given list.
 * Arguments:
    * `list`: An `orderedList` or `unorderedList` containing the items to be counted, or a `null` value.
 * Return Value:
    * An `int64` value representing the number of non-null items in the given list. The value `0i64` is returned if the input is `null`.

 * Example:


        let $l1 := ['hello', 'world', 1, 2, 3, null]
        return {"count": sql-count($l1)}

 * The expected result is:

        { "count": 5i64 }


### sql-avg ###

 * Syntax:

        sql-avg(num_list)

 * Gets the average value of the non-null items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing numeric or null values, or a `null` value.
 * Return Value:
    * A `double` value representing the average of the non-null numbers in the given list. The `null` value is returned if the input is `null`. Non-numeric types in the input list will cause an error.

 * Example:

        let $l := [1.2, 2.3, 3.4, 0, null]
        return {"avg": sql-avg($l)}

 * The expected result is:

        { "avg": 1.725d }


### sql-sum ###
 * Syntax:

        sql-sum(num_list)

 * Gets the sum of the non-null items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing numeric or null values, or a `null` value.
 * Return Value:
    * The sum of the non-null numbers in the given list. The returning type is decided by the item type with the highest order in the numeric type promotion order (`int8`-> `int16`->`int32`->`int64`->`float`->`double`) among items. The value `null` is returned if the input is `null`. Non-numeric types in the input list will cause an error.

 * Example:

        let $l := [1.2, 2.3, 3.4, 0, null]
        return {"sum": sql-sum($l)}

 * The expected result is:

        { "sum": 6.9d }


### sql-min/max ###
 * Syntax:

        sql-min(num_list), sql-max(num_list)

 * Gets the min/max value of the non-null numeric items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing the items to be compared, or a `null` value.
 * Return Value:
    * The min/max value of the given list. The returning type is decided by the item type with the highest order in the numeric type promotion order (`int8`-> `int16`->`int32`->`int64`->`float`->`double`) among items. The value `null` is returned if the input is `null`. Non-numeric types in the input list will cause an error.

 * Example:

        let $l := [1.2, 2.3, 3.4, 0, null]
        return {"min": sql-min($l), "max": sql-max($l)}

 * The expected result is:

        { "min": 0.0d, "max": 3.4d }

## <a id="SpatialFunctions">Spatial Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### create-point ###
 * Syntax:

        create-point(x, y)

 * Creates the primitive type `point` using an `x` and `y` value.
 * Arguments:
   * `x` : A `double` that represents the x-coordinate.
   * `y` : A `double` that represents the y-coordinate.
 * Return Value:
   * A `point` representing the ordered pair (`x`, `y`).

 * Example:

        use dataverse TinySocial;

        let $c :=  create-point(30.0,70.0)
        return {"point": $c}


 * The expected result is:

        { "point": point("30.0,70.0") }


### create-line ###
 * Syntax:

        create-line(point1, point2)

 * Creates the primitive type `line` using `point1` and `point2`.
 * Arguments:
    * `point1` : A `point` that represents the start point of the line.
    * `point2` : A `point` that represents the end point of the line.
 * Return Value:
    * A spatial `line` created using the points provided in `point1` and `point2`.

 * Example:

        use dataverse TinySocial;

        let $c :=  create-line(create-point(30.0,70.0), create-point(50.0,90.0))
        return {"line": $c}


 * The expected result is:

        { "line": line("30.0,70.0 50.0,90.0") }


### create-rectangle ###
 * Syntax:

        create-rectangle(point1, point2)

 * Creates the primitive type `rectangle` using `point1` and `point2`.
 * Arguments:
    * `point1` : A `point` that represents the lower-left point of the rectangle.
    * `point2` : A `point` that represents the upper-right point of the rectangle.
 * Return Value:
    * A spatial `rectangle` created using the points provided in `point1` and `point2`.

 * Example:

        use dataverse TinySocial;

        let $c :=  create-rectangle(create-point(30.0,70.0), create-point(50.0,90.0))
        return {"rectangle": $c}


 * The expected result is:

        { "rectangle": rectangle("30.0,70.0 50.0,90.0") }


### create-circle ###
 * Syntax:

        create-circle(point, radius)

 * Creates the primitive type `circle` using `point` and `radius`.
 * Arguments:
    * `point` : A `point` that represents the center of the circle.
    * `radius` : A `double` that represents the radius of the circle.
 * Return Value:
    * A spatial `circle` created using the center point and the radius provided in `point` and `radius`.

 * Example:

        use dataverse TinySocial;

        let $c :=  create-circle(create-point(30.0,70.0), 5.0)
        return {"circle": $c}


 * The expected result is:

        { "circle": circle("30.0,70.0 5.0") }


### create-polygon ###
 * Syntax:

        create-polygon(list)

 * Creates the primitive type `polygon` using the double values provided in the argument `list`. Each two consecutive double values represent a point starting from the first double value in the list. Note that at least six double values should be specified, meaning a total of three points.
 * Arguments:
   * `list` : An OrderedList of doubles representing the points of the polygon.
 * Return Value:
   * A `polygon`, represents a spatial simple polygon created using the points provided in `list`.

 * Example:

        use dataverse TinySocial;

        let $c :=  create-polygon([1.0,1.0,2.0,2.0,3.0,3.0,4.0,4.0])
        return {"polygon": $c}


 * The expected result is:

        { "polygon": polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0") }


### get-x/get-y ###
 * Syntax:

        get-x(point) or get-y(point)

 * Returns the x or y coordinates of a point `point`.
 * Arguments:
    * `point` : A `point`.
 * Return Value:
    * A `double` representing the x or y coordinates of the point `point`.

 * Example:

        use dataverse TinySocial;

        let $point := create-point(2.3,5.0)
        return {"x-coordinate": get-x($point), "y-coordinate": get-y($point)}


 * The expected result is:

        { "x-coordinate": 2.3d, "y-coordinate": 5.0d }


### get-points ###
 * Syntax:

        get-points(spatial_object)

 * Returns an ordered list of the points forming the spatial object `spatial_object`.
 * Arguments:
    * `spatial_object` : A `point`, `line`, `rectangle`, `circle`, or `polygon`.
 * Return Value:
    * An `OrderedList` of the points forming the spatial object `spatial_object`.

 * Example:

        use dataverse TinySocial;

        let $line := create-line(create-point(100.6,99.4), create-point(-72.0,-76.9))
        let $rectangle := create-rectangle(create-point(9.2,49.0), create-point(77.8,111.1))
        let $polygon := create-polygon([1.0,1.0,2.0,2.0,3.0,3.0,4.0,4.0])
        let $line_list := get-points($line)
        let $rectangle_list := get-points($rectangle)
        let $polygon_list := get-points($polygon)
        return {"line-first-point": $line_list[0], "line-second-point": $line_list[1], "rectangle-left-bottom-point": $rectangle_list[0], "rectangle-top-upper-point": $rectangle_list[1], "polygon-first-point": $polygon_list[0], "polygon-second-point": $polygon_list[1], "polygon-third-point": $polygon_list[2], "polygon-forth-point": $polygon_list[3]}


 * The expected result is:

        { "line-first-point": point("100.6,99.4"), "line-second-point": point("-72.0,-76.9"), "rectangle-left-bottom-point": point("9.2,49.0"), "rectangle-top-upper-point": point("77.8,111.1"), "polygon-first-point": point("1.0,1.0"), "polygon-second-point": point("2.0,2.0"), "polygon-third-point": point("3.0,3.0"), "polygon-forth-point": point("4.0,4.0") }


### get-center/get-radius ###
 * Syntax:

        get-center(circle_expression) or get-radius(circle_expression)

 * Returns the center and the radius of a circle `circle_expression`, respectively.
 * Arguments:
    * `circle_expression` : A `circle`.
 * Return Value:
    * A `point` or `double`, represent the center or radius of the circle `circle_expression`.

 * Example:

        use dataverse TinySocial;

        let $circle := create-circle(create-point(6.0,3.0), 1.0)
        return {"circle-radius": get-radius($circle), "circle-center": get-center($circle)}



 * The expected result is:

        { "circle-radius": 1.0d, "circle-center": point("6.0,3.0") }



### spatial-distance ###
 * Syntax:

        spatial-distance(point1, point2)

 * Returns the Euclidean distance between `point1` and `point2`.
 * Arguments:
    * `point1` : A `point`.
    * `point2` : A `point`.
 * Return Value:
    * A `double` as the Euclidean distance between `point1` and `point2`.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $d :=  spatial-distance($t.sender-location, create-point(30.0,70.0))
        return {"point": $t.sender-location, "distance": $d}



 * The expected result is:

        { "point": point("47.44,80.65"), "distance": 20.434678857275934d }
        { "point": point("29.15,76.53"), "distance": 6.585089217315132d }
        { "point": point("37.59,68.42"), "distance": 7.752709203884797d }
        { "point": point("24.82,94.63"), "distance": 25.168816023007512d }
        { "point": point("32.84,67.14"), "distance": 4.030533463451212d }
        { "point": point("29.72,75.8"), "distance": 5.806754687430835d }
        { "point": point("39.28,70.48"), "distance": 9.292405501268227d }
        { "point": point("40.09,92.69"), "distance": 24.832321679617472d }
        { "point": point("47.51,83.99"), "distance": 22.41250097601782d }
        { "point": point("36.21,72.6"), "distance": 6.73231758015024d }
        { "point": point("46.05,93.34"), "distance": 28.325926286707734d }
        { "point": point("36.86,74.62"), "distance": 8.270671073135482d }


### spatial-area ###
 * Syntax:

        spatial-area(spatial_2d_expression)

 * Returns the spatial area of `spatial_2d_expression`.
 * Arguments:
    * `spatial_2d_expression` : A `rectangle`, `circle`, or `polygon`.
 * Return Value:
    * A `double` representing the area of `spatial_2d_expression`.

 * Example:

        use dataverse TinySocial;

        let $circleArea := spatial-area(create-circle(create-point(0.0,0.0), 5.0))
        return {"Area":$circleArea}



 * The expected result is:

        { "Area": 78.53981625d }


### spatial-intersect ###
 * Syntax:

        spatial-intersect(spatial_object1, spatial_object2)

 * Checks whether `@arg1` and `@arg2` spatially intersect each other.
 * Arguments:
    * `spatial_object1` : A `point`, `line`, `rectangle`, `circle`, or `polygon`.
    * `spatial_object2` : A `point`, `line`, `rectangle`, `circle`, or `polygon`.
 * Return Value:
    * A `boolean` representing whether `spatial_object1` and `spatial_object2` spatially overlap with each other.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        where spatial-intersect($t.sender-location, create-rectangle(create-point(30.0,70.0), create-point(40.0,80.0)))
        return $t


 * The expected result is:

        { "tweetid": "4", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("39.28,70.48"), "send-time": datetime("2011-12-26T10:10:00.000Z"), "referred-topics": {{ "sprint", "voice-command" }}, "message-text": " like sprint the voice-command is mind-blowing:)" }
        { "tweetid": "7", "user": { "screen-name": "ChangEwing_573", "lang": "en", "friends_count": 182, "statuses_count": 394, "name": "Chang Ewing", "followers_count": 32136 }, "sender-location": point("36.21,72.6"), "send-time": datetime("2011-08-25T10:10:00.000Z"), "referred-topics": {{ "samsung", "platform" }}, "message-text": " like samsung the platform is good" }
        { "tweetid": "9", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("36.86,74.62"), "send-time": datetime("2012-07-21T10:10:00.000Z"), "referred-topics": {{ "verizon", "voicemail-service" }}, "message-text": " love verizon its voicemail-service is awesome" }


### spatial-cell ###
 * Syntax:

        spatial-cell(point1, point2, x_increment, y_increment)

 * Returns the grid cell that `point1` belongs to.
 * Arguments:
    * `point1` : A `point` representing the point of interest that its grid cell will be returned.
    * `point2` : A `point` representing the origin of the grid.
    * `x_increment` : A `double`, represents X increments.
    * `y_increment` : A `double`, represents Y increments.
 * Return Value:
    * A `rectangle` representing the grid cell that `point1` belongs to.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        group by $c :=  spatial-cell($t.sender-location, create-point(20.0,50.0), 5.5, 6.0) with $t
        let $num :=  count($t)
        return { "cell": $c, "count": $num}


 * The expected result is:

        { "cell": rectangle("20.0,92.0 25.5,98.0"), "count": 1i64 }
        { "cell": rectangle("25.5,74.0 31.0,80.0"), "count": 2i64 }
        { "cell": rectangle("31.0,62.0 36.5,68.0"), "count": 1i64 }
        { "cell": rectangle("31.0,68.0 36.5,74.0"), "count": 1i64 }
        { "cell": rectangle("36.5,68.0 42.0,74.0"), "count": 2i64 }
        { "cell": rectangle("36.5,74.0 42.0,80.0"), "count": 1i64 }
        { "cell": rectangle("36.5,92.0 42.0,98.0"), "count": 1i64 }
        { "cell": rectangle("42.0,80.0 47.5,86.0"), "count": 1i64 }
        { "cell": rectangle("42.0,92.0 47.5,98.0"), "count": 1i64 }
        { "cell": rectangle("47.5,80.0 53.0,86.0"), "count": 1i64 }




## <a id="SimilarityFunctions">Similarity Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

AsterixDB supports queries with different similarity functions,
including [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) and [Jaccard](https://en.wikipedia.org/wiki/Jaccard_index).

### edit-distance ###
 * Syntax:

        edit-distance(expression1, expression2)

 * Returns the edit distance of `expression1` and `expression2`.
 * Arguments:
    * `expression1` : A `string` or a homogeneous `OrderedList` of a comparable item type.
    * `expression2` : The same type as `expression1`.
 * Return Value:
    * An `int64` that represents the edit distance between `expression1` and `expression2`.
 * Note: An [n-gram index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
 * Example:

        use dataverse TinySocial;

        for $user in dataset('FacebookUsers')
        let $ed := edit-distance($user.name, "Suzanna Tilson")
        where $ed <= 2
        return $user


 * The expected result is:

        {
        "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "user-since": datetime("2012-08-07T10:10:00.000Z"), "friend-ids": {{ 6 }},
        "employment": [ { "organization-name": "Labzatron", "start-date": date("2011-04-19"), "end-date": null } ]
        }


### edit-distance-check ###
 * Syntax:

        edit-distance-check(expression1, expression2, threshold)

 * Checks whether `expression1` and `expression2` have an [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) within a given threshold.  The “check” version of edit distance is faster than the "non-check" version because the former can detect whether two items satisfy a given threshold using early-termination techniques, as opposed to computing their real distance. Although possible, it is not necessary for the user to write queries using the “check” versions explicitly, since a rewrite rule can perform an appropriate transformation from a “non-check” version to a “check” version.

 * Arguments:
    * `expression1` : A `string` or a homogeneous `OrderedList` of a comparable item type.
    * `expression2` : The same type as `expression1`.
    * `threshold` : An `int64` that represents the distance threshold.
 * Return Value:
    * An `OrderedList` with two items:
        * The first item contains a `boolean` value representing whether `expression1` and `expression2` are similar.
        * The second item contains an `int64` that represents the edit distance of `expression1` and `expression2` if it is within the threshold, or 0 otherwise.
 * Note: An [n-gram index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
 * Example:

        use dataverse TinySocial;

        for $user in dataset('FacebookUsers')
        let $ed := edit-distance-check($user.name, "Suzanna Tilson", 2)
        where $ed[0]
        return $ed[1]


 * The expected result is:

        2

### edit-distance-contains ###
* Syntax:

        edit-distance-contains(expression1, expression2, threshold)

* Checks whether `expression1` contains `expression2` with an [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) within a given threshold.

* Arguments:
    * `expression1` : A `string` or a homogeneous `OrderedList` of a comparable item type.
    * `expression2` : The same type as `expression1`.
    * `threshold` : An `int32` that represents the distance threshold.
* Return Value:
    * An `OrderedList` with two items:
        * The first item contains a `boolean` value representing whether `expression1` can contain `expression2`.
        * The second item contains an `int32` that represents the required edit distance for `expression1` to contain `expression2` if the first item is true.
* Note: An [n-gram index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
* Example:

        let $i := edit-distance-contains("happy","hapr",2)
        return $i;


* The expected result is:

        [ true, 1 ]



### similarity-jaccard ###
 * Syntax:

        similarity-jaccard(list1, list2)

 * Returns the [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index) of `list1` and `list2`.
 * Arguments:
    * `list1` : An `UnorderedList` or `OrderedList`.
    * `list2` : An `UnorderedList` or `OrderedList`.
 * Return Value:
    * A `float` that represents the Jaccard similarity of `list1` and `list2`.
 * Note: A [keyword index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
 * Example:

        use dataverse TinySocial;

        for $user in dataset('FacebookUsers')
        let $sim := similarity-jaccard($user.friend-ids, [1,5,9,10])
        where $sim >= 0.6f
        return $user


 * The expected result is:

        {
        "id": 3, "alias": "Emory", "name": "EmoryUnk", "user-since": datetime("2012-07-10T10:10:00.000Z"), "friend-ids": {{ 1, 5, 8, 9 }},
        "employment": [ { "organization-name": "geomedia", "start-date": date("2010-06-17"), "end-date": date("2010-01-26") } ]
        }
        {
        "id": 10, "alias": "Bram", "name": "BramHatch", "user-since": datetime("2010-10-16T10:10:00.000Z"), "friend-ids": {{ 1, 5, 9 }},
        "employment": [ { "organization-name": "physcane", "start-date": date("2007-06-05"), "end-date": date("2011-11-05") } ]
        }


### similarity-jaccard-check ###
 * Syntax:

        similarity-jaccard-check(list1, list2, threshold)

 * Checks whether `list1` and `list2` have a [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index) greater than or equal to threshold.  Again, the “check” version of Jaccard is faster than the "non-check" version.

 * Arguments:
    * `list1` : An `UnorderedList` or `OrderedList`.
    * `list2` : An `UnorderedList` or `OrderedList`.
    * `threshold` : A `float` that represents the similarity threshold.
 * Return Value:
    * An `OrderedList` with two items:
     * The first item contains a `boolean` value representing whether `list1` and `list2` are similar.
     * The second item contains a `float` that represents the Jaccard similarity of `list1` and `list2` if it is greater than or equal to the threshold, or 0 otherwise.
 * Note: A [keyword index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
 * Example:

        use dataverse TinySocial;

        for $user in dataset('FacebookUsers')
        let $sim := similarity-jaccard-check($user.friend-ids, [1,5,9,10], 0.6f)
        where $sim[0]
        return $sim[1]


 * The expected result is:

        0.75f
        1.0f


### Similarity Operator ~= ###
 * "`~=`" is syntactic sugar for expressing a similarity condition with a given similarity threshold.
 * The similarity function and threshold for "`~=`" are controlled via "set" directives.
 * The "`~=`" operator returns a `boolean` value that represents whether the operands are similar.

 * Example for Jaccard similarity:

        use dataverse TinySocial;

        set simfunction "jaccard";
        set simthreshold "0.6f";

        for $user in dataset('FacebookUsers')
        where $user.friend-ids ~= [1,5,9,10]
        return $user


 * The expected result is:

        {
        "id": 3, "alias": "Emory", "name": "EmoryUnk", "user-since": datetime("2012-07-10T10:10:00.000Z"), "friend-ids": {{ 1, 5, 8, 9 }},
        "employment": [ { "organization-name": "geomedia", "start-date": date("2010-06-17"), "end-date": date("2010-01-26") } ]
        }
        {
        "id": 10, "alias": "Bram", "name": "BramHatch", "user-since": datetime("2010-10-16T10:10:00.000Z"), "friend-ids": {{ 1, 5, 9 }},
        "employment": [ { "organization-name": "physcane", "start-date": date("2007-06-05"), "end-date": date("2011-11-05") } ]
        }


 * Example for edit-distance similarity:

        use dataverse TinySocial;

        set simfunction "edit-distance";
        set simthreshold "2";

        for $user in dataset('FacebookUsers')
        where $user.name ~= "Suzanna Tilson"
        return $user


 * The expected output is:

        {
        "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "user-since": datetime("2012-08-07T10:10:00.000Z"), "friend-ids": {{ 6 }},
        "employment": [ { "organization-name": "Labzatron", "start-date": date("2011-04-19"), "end-date": null } ]
        }


## <a id="TokenizingFunctions">Tokenizing Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### word-tokens ###

 * Syntax:

        word-tokens(string)

 * Returns a list of word tokens of `string` using non-alphanumeric characters as delimiters.
 * Arguments:
    * `string` : A `string` that will be tokenized.
 * Return Value:
    * An `OrderedList` of `string` word tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := word-tokens($t.message-text)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "word-tokens": $tokens
        }


 * The expected result is:

        { "tweetid": "9", "word-tokens": [ "love", "verizon", "its", "voicemail", "service", "is", "awesome" ] }


<!--### hashed-word-tokens ###
 * Syntax:

        hashed-word-tokens(string)

 * Returns a list of hashed word tokens of `string`.
 * Arguments:
    * `string` : A `string` that will be tokenized.
 * Return Value:
   * An `OrderedList` of `int32` hashed tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := hashed-word-tokens($t.message-text)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "hashed-word-tokens": $tokens
        }


 * The expected result is:

        { "tweetid": "9", "hashed-word-tokens": [ -1217719622, -447857469, -1884722688, -325178649, 210976949, 285049676, 1916743959 ] }


### counthashed-word-tokens ###
 * Syntax:

        counthashed-word-tokens(string)

 * Returns a list of hashed word tokens of `string`. The hashing mechanism gives duplicate tokens different hash values, based on the occurrence count of that token.
 * Arguments:
    * `string` : A `String` that will be tokenized.
 * Return Value:
    * An `OrderedList` of `Int32` hashed tokens.
 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := counthashed-word-tokens($t.message-text)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "counthashed-word-tokens": $tokens
        }


 * The expected result is:

        { "tweetid": "9", "counthashed-word-tokens": [ -1217719622, -447857469, -1884722688, -325178649, 210976949, 285049676, 1916743959 ] }


### gram-tokens ###
 * Syntax:

        gram-tokens(string, gram_length, boolean_expression)

 * Returns a list of gram tokens of `string`, which can be obtained by scanning the characters using a sliding window of a fixed length.
 * Arguments:
    * `string` : A `String` that will be tokenized.
    * `gram_length` : An `Int32` as the length of grams.
   * `boolean_expression` : A `Boolean` value to indicate whether to generate additional grams by pre- and postfixing `string` with special characters.
 * Return Value:
    * An `OrderedList` of String gram tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := gram-tokens($t.message-text, 3, true)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "gram-tokens": $tokens
        }


 * The expected result is:

        {
        "tweetid": "9",
        "gram-tokens": [ "## ", "# l", " lo", "lov", "ove", "ve ", "e v", " ve", "ver", "eri", "riz", "izo", "zon", "on ", "n i", " it", "its", "ts ", "s v", " vo", "voi", "oic", "ice",
        "cem", "ema", "mai", "ail", "il-", "l-s", "-se", "ser", "erv", "rvi", "vic", "ice", "ce ", "e i", " is", "is ", "s a", " aw", "awe", "wes", "eso", "som", "ome", "me$", "e$$" ]
        }


### hashed-gram-tokens ###
 * Syntax:

        hashed-gram-tokens(string, gram_length, boolean_expression)

 * Returns a list of hashed gram tokens of `string`.
 * Arguments:
    * `string` : A `String` that will be tokenized.
    * `gram_length` : An `Int32` as the length of grams.
    * `boolean_expression` : A `Boolean` to indicate whether to generate additional grams by pre- and postfixing `string` with special characters.
 * Return Value:
    * An `OrderedList` of `Int32` hashed gram tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := hashed-gram-tokens($t.message-text, 3, true)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "hashed-gram-tokens": $tokens
        }


 * The expected result is:

        {
        "tweetid": "9",
        "hashed-gram-tokens": [ 40557178, -2002241593, 161665899, -856104603, -500544946, 693410611, 395674299, -1015235909, 1115608337, 1187999872, -31006095, -219180466, -1676061637,
        1040194153, -1339307841, -1527110163, -1884722688, -179148713, -431014627, -1789789823, -1209719926, 684519765, -486734513, 1734740619, -1971673751, -932421915, -2064668066,
        -937135958, -790946468, -69070309, 1561601454, 26169001, -160734571, 1330043462, -486734513, -18796768, -470303314, 113421364, 1615760212, 1688217556, 1223719184, 536568131,
        1682609873, 2935161, -414769471, -1027490137, 1602276102, 1050490461 ]
        }


### counthashed-gram-tokens ###
 * Syntax:

        counthashed-gram-tokens(string, gram_length, boolean_expression)

 * Returns a list of hashed gram tokens of `string`. The hashing mechanism gives duplicate tokens different hash values, based on the occurrence count of that token.
 * Arguments:
    * `string` : A `String` that will be tokenized.
    * `gram_length` : An `Int32`, length of grams to generate.
    * `boolean_expression` : A `Boolean`, whether to generate additional grams by pre- and postfixing `string` with special characters.
 * Return Value:
    * An `OrderedList` of `Int32` hashed gram tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := counthashed-gram-tokens($t.message-text, 3, true)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "counthashed-gram-tokens": $tokens
        }


 * The expected result is:

        {
        "tweetid": "9",
        "counthashed-gram-tokens": [ 40557178, -2002241593, 161665899, -856104603, -500544946, 693410611, 395674299, -1015235909, 1115608337, 1187999872, -31006095, -219180466, -1676061637,
        1040194153, -1339307841, -1527110163, -1884722688, -179148713, -431014627, -1789789823, -1209719926, 684519765, -486734513, 1734740619, -1971673751, -932421915, -2064668066, -937135958,
        -790946468, -69070309, 1561601454, 26169001, -160734571, 1330043462, -486734512, -18796768, -470303314, 113421364, 1615760212, 1688217556, 1223719184, 536568131, 1682609873, 2935161,
        -414769471, -1027490137, 1602276102, 1050490461 ]
        }
-->

## <a id="TemporalFunctions">Temporal Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##


### get-year/get-month/get-day/get-hour/get-minute/get-second/get-millisecond ###
 * Syntax:

        get-year/get-month/get-day/get-hour/get-minute/get-second/get-millisecond(temporal_value)

 * Accessors for accessing fields in a temporal value
 * Arguments:
    * `temporal_value` : a temporal value represented as one of the following types: `date`, `datetime`, `time`, and `duration`.
 * Return Value:
    * An `int64` value representing the field to be extracted.

 * Example:

        let $c1 := date("2010-10-30")
        let $c2 := datetime("1987-11-19T23:49:23.938")
        let $c3 := time("12:23:34.930+07:00")
        let $c4 := duration("P3Y73M632DT49H743M3948.94S")

        return {"year": get-year($c1), "month": get-month($c2), "day": get-day($c1), "hour": get-hour($c3), "min": get-minute($c4), "second": get-second($c2), "ms": get-millisecond($c4)}


 * The expected result is:

        { "year": 2010, "month": 11, "day": 30, "hour": 5, "min": 28, "second": 23, "ms": 94 }


### adjust-datetime-for-timezone ###
 * Syntax:

        adjust-datetime-for-timezone(datetime, string)

 * Adjusts the given datetime `datetime` by applying the timezone information `string`.
 * Arguments:
    * `datetime` : A `datetime` value to be adjusted.
    * `string` : A `string` representing the timezone information.
 * Return Value:
    * A `string` value representing the new datetime after being adjusted by the timezone information.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('TweetMessages')
        return {"adjusted-send-time": adjust-datetime-for-timezone($i.send-time, "+08:00"), "message": $i.message-text}


 * The expected result is:

        { "adjusted-send-time": "2008-04-26T18:10:00.000+08:00", "message": " love t-mobile its customization is good:)" }
        { "adjusted-send-time": "2010-05-13T18:10:00.000+08:00", "message": " like verizon its shortcut-menu is awesome:)" }
        { "adjusted-send-time": "2006-11-04T18:10:00.000+08:00", "message": " like motorola the speed is good:)" }
        { "adjusted-send-time": "2011-12-26T18:10:00.000+08:00", "message": " like sprint the voice-command is mind-blowing:)" }
        { "adjusted-send-time": "2006-08-04T18:10:00.000+08:00", "message": " can't stand motorola its speed is terrible:(" }
        { "adjusted-send-time": "2010-05-07T18:10:00.000+08:00", "message": " like iphone the voice-clarity is good:)" }
        { "adjusted-send-time": "2011-08-25T18:10:00.000+08:00", "message": " like samsung the platform is good" }
        { "adjusted-send-time": "2005-10-14T18:10:00.000+08:00", "message": " like t-mobile the shortcut-menu is awesome:)" }
        { "adjusted-send-time": "2012-07-21T18:10:00.000+08:00", "message": " love verizon its voicemail-service is awesome" }
        { "adjusted-send-time": "2008-01-26T18:10:00.000+08:00", "message": " hate verizon its voice-clarity is OMG:(" }
        { "adjusted-send-time": "2008-03-09T18:10:00.000+08:00", "message": " can't stand iphone its platform is terrible" }
        { "adjusted-send-time": "2010-02-13T18:10:00.000+08:00", "message": " like samsung the voice-command is amazing:)" }


### adjust-time-for-timezone ###
 * Syntax:

        adjust-time-for-timezone(time, string)

 * Adjusts the given time `time` by applying the timezone information `string`.
 * Arguments:
    * `time` : A `time` value to be adjusted.
    * `string` : A `string` representing the timezone information.
 * Return Value:
    * A `string` value representing the new time after being adjusted by the timezone information.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('TweetMessages')
        return {"adjusted-send-time": adjust-time-for-timezone(time-from-datetime($i.send-time), "+08:00"), "message": $i.message-text}


 * The expected result is:

        { "adjusted-send-time": "18:10:00.000+08:00", "message": " love t-mobile its customization is good:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like verizon its shortcut-menu is awesome:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like motorola the speed is good:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like sprint the voice-command is mind-blowing:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " can't stand motorola its speed is terrible:(" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like iphone the voice-clarity is good:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like samsung the platform is good" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like t-mobile the shortcut-menu is awesome:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " love verizon its voicemail-service is awesome" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " hate verizon its voice-clarity is OMG:(" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " can't stand iphone its platform is terrible" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like samsung the voice-command is amazing:)" }


### calendar-duration-from-datetime ###
 * Syntax:

        calendar-duration-from-datetime(datetime, duration_value)

 * Gets a user-friendly representation of the duration `duration_value` based on the given datetime `datetime`.
 * Arguments:
    * `datetime` : A `datetime` value to be used as the reference time point.
    * `duration_value` : A `duration` value to be converted.
 * Return Value:
    * A `duration` value with the duration as `duration_value` but with a user-friendly representation.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('TweetMessages')
        where $i.send-time > datetime("2011-01-01T00:00:00")
        return {"since-2011": subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")), "since-2011-user-friendly": calendar-duration-from-datetime($i.send-time, subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")))}


 * The expected result is:

        { "since-2011": duration("P359DT10H10M"), "since-2011-user-friendly": duration("P11M23DT10H10M") }
        { "since-2011": duration("P236DT10H10M"), "since-2011-user-friendly": duration("P7M23DT10H10M") }
        { "since-2011": duration("P567DT10H10M"), "since-2011-user-friendly": duration("P1Y6M18DT10H10M") }


### get-year-month-duration/get-day-time-duration ###
 * Syntax:

        get-year-month-duration/get-day-time-duration(duration_value)

 * Extracts the correct `duration` subtype from `duration_value`.
 * Arguments:
    * `duration_value` : A `duration` value to be converted.
 * Return Value:
    * A `year-month-duration` value or a `day-time-duration` value.

 * Example:

        let $i := get-year-month-duration(duration("P12M50DT10H"))
        return $i;


 * The expected result is:

        year-month-duration("P1Y")

### months-from-year-month-duration/milliseconds-from-day-time-duration ###
* Syntax:

        months-from-year-month-duration/milliseconds-from-day-time-duration(duration_value)

* Extracts the number of months or the number of milliseconds from the `duration` subtype.
* Arguments:
    * `duration_value` : A `duration` of the correct subtype.
* Return Value:
    * An `int64` representing the number or months/milliseconds.

* Example:

        let $i := months-from-year-month-duration(get-year-month-duration(duration("P5Y7MT50M")))
        return $i;


* The expected result is:

        67


### duration-from-months/duration-from-ms ###
* Syntax:

        duration-from-months/duration-from-ms(number_value)

* Creates a `duration` from `number_value`.
* Arguments:
    * `number_value` : An `int64` representing the number of months/milliseconds
* Return Value:
    * A `duration` containing `number_value` value for months/milliseconds

* Example:

        let $i := duration-from-months(8)
        return $i;

* The expected result is:

        duration("P8M")


### duration-from-interval ###
* Syntax:

        duration-from-interval(interval_value)

* Creates a `duration` from `interval_value`.
* Arguments:
    * `interval_value` : An `interval` value
* Return Value:
    * A `duration` repesenting the time in the `interval_value`

* Example:

        let $itv1 := interval-from-date("2010-10-30", "2010-12-21")
        let $itv2 := interval-from-datetime("2012-06-26T01:01:01.111", "2012-07-27T02:02:02.222")
        let $itv3 := interval-from-time("12:32:38", "20:29:20")

        return { "dr1" : duration-from-interval($itv1),
          "dr2" : duration-from-interval($itv2),
          "dr3" : duration-from-interval($itv3),
          "dr4" : duration-from-interval(null) }

* The expected result is:

        { "dr1": day-time-duration("P52D"),
          "dr2": day-time-duration("P31DT1H1M1.111S"),
          "dr3": day-time-duration("PT7H56M42S"),
          "dr4": null }


### current-date ###
 * Syntax:

        current-date()

 * Gets the current date.
 * Arguments: None
 * Return Value:
    * A `date` value of the date when the function is called.

### current-time ###
 * Syntax:

        current-time()

 * Get the current time
 * Arguments: None
 * Return Value:
    * A `time` value of the time when the function is called.

### current-datetime ###
 * Syntax:

        current-datetime()

 * Get the current datetime
 * Arguments: None
 * Return Value:
    * A `datetime` value of the datetime when the function is called.

 * Example:

        {"current-date": current-date(),
        "current-time": current-time(),
        "current-datetime": current-datetime()}


 * The expected result is:

        { "current-date": date("2013-04-06"),
        "current-time": time("00:48:44.093Z"),
        "current-datetime": datetime("2013-04-06T00:48:44.093Z") }


### get-date-from-datetime ###
 * Syntax:

        get-date-from-datetime(datetime)

 * Gets the date value from the given datetime value `datetime`.
 * Arguments:
    * `datetime`: A `datetime` value to be extracted from.
 * Return Value:
    * A `date` value from the datetime.

### get-time-from-datetime ###
 * Syntax:

        get-time-from-datetime(datetime)

 * Get the time value from the given datetime value `datetime`
 * Arguments:
    * `datetime`: A `datetime` value to be extracted from
 * Return Value:
    * A `time` value from the datetime.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('TweetMessages')
        where $i.send-time > datetime("2011-01-01T00:00:00")
        return {"send-date": get-date-from-datetime($i.send-time), "send-time": get-time-from-datetime($i.send-time)}


 * The expected result is:

        { "send-date": date("2011-12-26"), "send-time": time("10:10:00.000Z") }
        { "send-date": date("2011-08-25"), "send-time": time("10:10:00.000Z") }
        { "send-date": date("2012-07-21"), "send-time": time("10:10:00.000Z") }


### day-of-week ###
* Syntax:

        day-of-week(date)

* Finds the day of the week for a given date (1-7)
* Arguments:
    * `date`: A `date` value (Can also be a `datetime`)
* Return Value:
    * An `int8` representing the day of the week (1-7)

* Example:

        let $i := day-of-week( datetime("2012-12-30T12:12:12.039Z"))
        return $i;


* The expected result is:

        7


### date-from-unix-time-in-days ###
 * Syntax:

        date-from-unix-time-in-days(numeric_value)

 * Gets a date representing the time after `numeric_value` days since 1970-01-01.
 * Arguments:
    * `numeric_value`: A `int8`/`int16`/`int32`/`int64` value representing the number of days.
 * Return Value:
    * A `date` value as the time after `numeric_value` days since 1970-01-01.

### datetime-from-unix-time-in-ms ###
 * Syntax:

        datetime-from-unix-time-in-ms(numeric_value)

 * Gets a datetime representing the time after `numeric_value` milliseconds since 1970-01-01T00:00:00Z.
 * Arguments:
    * `numeric_value`: A `int8`/`int16`/`int32`/`int64` value representing the number of milliseconds.
 * Return Value:
    * A `datetime` value as the time after `numeric_value` milliseconds since 1970-01-01T00:00:00Z.

### datetime-from-unix-time-in-secs ###
 * Syntax:

        datetime-from-unix-time-in-secs(numeric_value)

 * Gets a datetime representing the time after `numeric_value` seconds since 1970-01-01T00:00:00Z.
 * Arguments:
    * `numeric_value`: A `int8`/`int16`/`int32`/`int64` value representing the number of seconds.
 * Return Value:
    * A `datetime` value as the time after `numeric_value` seconds since 1970-01-01T00:00:00Z.


### datetime-from-date-time ###
* Syntax:

datetime-from-date-time(date,time)

* Gets a datetime representing the combination of `date` and `time`
    * Arguments:
    * `date`: A `date` value
    * `time` A `time` value
* Return Value:
    * A `datetime` value by combining `date` and `time`

### time-from-unix-time-in-ms ###
 * Syntax:

        time-from-unix-time-in-ms(numeric_value)

 * Gets a time representing the time after `numeric_value` milliseconds since 00:00:00.000Z.
 * Arguments:
    * `numeric_value`: A `int8`/`int16`/`int32`/`int64` value representing the number of milliseconds.
 * Return Value:
    * A `time` value as the time after `numeric_value` milliseconds since 00:00:00.000Z.

 * Example:

        use dataverse TinySocial;

        let $d := date-from-unix-time-in-days(15800)
        let $dt := datetime-from-unix-time-in-ms(1365139700000)
        let $t := time-from-unix-time-in-ms(3748)
        return {"date": $d, "datetime": $dt, "time": $t}


 * The expected result is:

        { "date": date("2013-04-05"), "datetime": datetime("2013-04-05T05:28:20.000Z"), "time": time("00:00:03.748Z") }


### parse-date/parse-time/parse-datetime ###
* Syntax:

parse-date/parse-time/parse-datetime(date,formatting_expression)

* Creates a `date/time/date-time` value by treating `date` with formatting `formatting_expression`
* Arguments:
    * `date`: A `string` value representing the `date/time/datetime`.
    * `formatting_expression` A `string` value providing the formatting for `date_expression`.Characters used to create date expression:
       * `h` hours
       * `m` minutes
       * `s` seconds
       * `n` milliseconds
       * `a` am/pm
       * `z` timezone
       * `Y` year
       * `M` month
       * `D` day
       * `W` weekday
       * `-`, `'`, `/`, `.`, `,`, `T` seperators for both time and date
* Return Value:
    * A `date/time/date-time` value corresponding to `date`

* Example:

        let $i := parse-time("30:30","m:s")
        return $i;

* The expected result is:

        time("00:30:30.000Z")


### print-date/print-time/print-datetime ###
* Syntax:

        print-date/print-time/print-datetime(date,formatting_expression)

* Creates a `string` representing a `date/time/date-time` value of the `date` using the formatting `formatting_expression`
* Arguments:
    * `date`: A `date/time/datetime` value.
    * `formatting_expression` A `string` value providing the formatting for `date_expression`. Characters used to create date expression:
       * `h` hours
       * `m` minutes
       * `s` seconds
       * `n` milliseconds
       * `a` am/pm
       * `z` timezone
       * `Y` year
       * `M` month
       * `D` day
       * `W` weekday
       * `-`, `'`, `/`, `.`, `,`, `T` seperators for both time and date
* Return Value:
    * A `string` value corresponding to `date`

* Example:

        let $i := print-time(time("00:30:30.000Z"),"m:s")
        return $i;

* The expected result is:

        "30:30"


### get-interval-start, get-interval-end ###
 * Syntax:

        get-interval-start/get-interval-end(interval)

 * Gets the start/end of the given interval.
 * Arguments:
    * `interval`: the interval to be accessed.
 * Return Value:
    * A `time`, `date`, or `datetime` (depending on the time instances of the interval) representing the starting or ending time.

 * Example:

        let $itv := interval-start-from-date("1984-01-01", "P1Y")
        return {"start": get-interval-start($itv), "end": get-interval-end($itv)}


 * The expected result is:

        { "start": date("1984-01-01"), "end": date("1985-01-01") }


### get-interval-start-date/get-interval-start-datetimeget-interval-start-time, get-interval-end-date/get-interval-end-datetime/get-interval-end-time ###
 * Syntax:

        get-interval-start-date/get-interval-start-datetime/get-interval-start-time/get-interval-end-date/get-interval-end-datetime/get-interval-end-time(interval)

 * Gets the start/end of the given interval for the specific date/datetime/time type.
 * Arguments:
    * `interval`: the interval to be accessed.
 * Return Value:
    * A `time`, `date`, or `datetime` (depending on the function) representing the starting or ending time.

 * Example:

        let $itv1 := interval-start-from-date("1984-01-01", "P1Y")
        let $itv2 := interval-start-from-datetime("1984-01-01T08:30:00.000", "P1Y1H")
        let $itv3 := interval-start-from-time("08:30:00.000", "P1H")
        return {"start": get-interval-start-date($itv1), "end": get-interval-end-date($itv1), "start": get-interval-start-datetime($itv2), "end": get-interval-end-datetime($itv2), "start": get-interval-start-time($itv3), "end": get-interval-end-time($itv3)}


 * The expected result is:

        { "start": date("1984-01-01"), "end": date("1985-01-01"), "start": datetime("1984-01-01T08:30:00.000"), "end": datetime("1984-02-01T09:30:00.000"), "start": date("08:30:00.000"), "end": time("09:30:00.000")  }


### get-overlapping-interval ###
 * Syntax:

        get-overlapping-interval(interval1, interval2)

 * Gets the start/end of the given interval for the specific date/datetime/time type.
 * Arguments:
    * `interval1`: an `interval` value
    * `interval2`: an `interval` value
 * Return Value:
    * Returns an `interval` that is overlapping `interval1` and `interval2`. If `interval1` and `interval2` do not overlap `null` is returned. Note each interval must be of the same type.

 * Example:

        { "overlap1": get-overlapping-interval(interval-from-time(time("11:23:39"), time("18:27:19")), interval-from-time(time("12:23:39"), time("23:18:00"))),
          "overlap2": get-overlapping-interval(interval-from-time(time("12:23:39"), time("18:27:19")), interval-from-time(time("07:19:39"), time("09:18:00"))),
          "overlap3": get-overlapping-interval(interval-from-date(date("1980-11-30"), date("1999-09-09")), interval-from-date(date("2013-01-01"), date("2014-01-01"))),
          "overlap4": get-overlapping-interval(interval-from-date(date("1980-11-30"), date("2099-09-09")), interval-from-date(date("2013-01-01"), date("2014-01-01"))),
          "overlap5": get-overlapping-interval(interval-from-datetime(datetime("1844-03-03T11:19:39"), datetime("2000-10-30T18:27:19")), interval-from-datetime(datetime("1989-03-04T12:23:39"), datetime("2009-10-10T23:18:00"))),
          "overlap6": get-overlapping-interval(interval-from-datetime(datetime("1989-03-04T12:23:39"), datetime("2000-10-30T18:27:19")), interval-from-datetime(datetime("1844-03-03T11:19:39"), datetime("1888-10-10T23:18:00")))  }

 * The expected result is:

        { "overlap1": interval-time("12:23:39.000Z, 18:27:19.000Z"),
          "overlap2": null,
          "overlap3": null,
          "overlap4": interval-date("2013-01-01, 2014-01-01"),
          "overlap5": interval-datetime("1989-03-04T12:23:39.000Z, 2000-10-30T18:27:19.000Z"),
          "overlap6": null }


### interval-before/interval-after/interval-meets/interval-met-by/interval-overlaps/interval-overlapped-by/interval-overlapping/interval-starts/interval-started-by/interval-covers/interval-covered-by/interval-ends/interval-ended-by ###


See the [Allen's Relations](allens.html).


### interval-bin ###
 * Syntax:

        interval-bin(time-to-bin, time-bin-anchor, duration-bin-size)

 * Return the `interval` value representing the bin containing the `time-to-bin` value.
 * Arguments:
    * `time-to-bin`: a date/time/datetime value representing the time to be binned.
    * `time-bin-anchor`: a date/time/datetime value representing an anchor of a bin starts. The type of this argument should be the same as the first `time-to-bin` argument.
    * `duration-bin-size`: the duration value representing the size of the bin, in the type of year-month-duration or day-time-duration. The type of this duration should be compatible with the type of `time-to-bin`, so that the arithmetic operation between `time-to-bin` and `duration-bin-size` is well-defined. Currently AsterixDB supports the following arithmetic operations:
        * datetime +|- year-month-duration
        * datetime +|- day-time-duration
        * date +|- year-month-duration
        * date +|- day-time-duration
        * time +|- day-time-duration
  * Return Value:
    * A `interval` value representing the bin containing the `time-to-bin` value. Note that the internal type of this interval value should be the same as the `time-to-bin` type.

  * Example:

        let $c1 := date("2010-10-30")
        let $c2 := datetime("-1987-11-19T23:49:23.938")
        let $c3 := time("12:23:34.930+07:00")

        return { "bin1": interval-bin($c1, date("1990-01-01"), year-month-duration("P1Y")),
         "bin2": interval-bin($c2, datetime("1990-01-01T00:00:00.000Z"), year-month-duration("P6M")),
         "bin3": interval-bin($c3, time("00:00:00"), day-time-duration("PD1M")),
         "bin4": interval-bin($c2, datetime("2013-01-01T00:00:00.000"), day-time-duration("PT24H"))
       }

   * The expected result is:

        { "bin1": interval-date("2010-01-01, 2011-01-01"),
          "bin2": interval-datetime("-1987-07-01T00:00:00.000Z, -1986-01-01T00:00:00.000Z"),
          "bin3": interval-time("05:23:00.000Z, 05:24:00.000Z"),
          "bin4": interval-datetime("-1987-11-19T00:00:00.000Z, -1987-11-20T00:00:00.000Z")}


### interval-from-date ###
 * Syntax:

        interval-from-date(string1, string2)

 * Constructor function for the `interval` type by parsing two date strings.
 * Arguments:
    * `string1` : The `string` value representing the starting date.
    * `string2` : The `string` value representing the ending date.
 * Return Value:
    * An `interval` value between the two dates.

 * Example:

        {"date-interval": interval-from-date("2012-01-01", "2013-04-01")}


 * The expected result is:

        { "date-interval": interval-date("2012-01-01, 2013-04-01") }


### interval-from-time ###
 * Syntax:

        interval-from-time(string1, string2)

 * Constructor function for the `interval` type by parsing two time strings.
 * Arguments:
    * `string1` : The `string` value representing the starting time.
    * `string2` : The `string` value representing the ending time.
 * Return Value:
    * An `interval` value between the two times.

 * Example:

        {"time-interval": interval-from-time("12:23:34.456Z", "233445567+0800")}


 * The expected result is:

        { "time-interval": interval-time("12:23:34.456Z, 15:34:45.567Z") }


### interval-from-datetime ###
 * Syntax:

        interval-from-datetime(string1, string2)

 * Constructor function for `interval` type by parsing two datetime strings.
 * Arguments:
    * `string1` : The `string` value representing the starting datetime.
    * `string2` : The `string` value representing the ending datetime.
 * Return Value:
    * An `interval` value between the two datetimes.

 * Example:

        {"datetime-interval": interval-from-datetime("2012-01-01T12:23:34.456+08:00", "20130401T153445567Z")}


 * The expected result is:

        { "datetime-interval": interval-datetime("2012-01-01T04:23:34.456Z, 2013-04-01T15:34:45.567Z") }


### interval-start-from-date/time/datetime ###
 * Syntax:

        interval-start-from-date/time/datetime(date/time/datetime, duration)

 * Construct an `interval` value by the given starting `date`/`time`/`datetime` and the `duration` that the interval lasts.
 * Arguments:
    * `date/time/datetime`: a `string` representing a `date`, `time` or `datetime`, or a `date`/`time`/`datetime` value, representing the starting time point.
    * `duration`: a `string` or `duration` value representing the duration of the interval. Note that duration cannot be negative value.
 * Return Value:
    * An `interval` value representing the interval starting from the given time point with the length of duration.

 * Example:

        let $itv1 := interval-start-from-date("1984-01-01", "P1Y")
        let $itv2 := interval-start-from-time(time("02:23:28.394"), "PT3H24M")
        let $itv3 := interval-start-from-datetime("1999-09-09T09:09:09.999", duration("P2M30D"))
        return {"interval1": $itv1, "interval2": $itv2, "interval3": $itv3}

 * The expectecd result is:

        { "interval1": interval-date("1984-01-01, 1985-01-01"), "interval2": interval-time("02:23:28.394Z, 05:47:28.394Z"), "interval3": interval-datetime("1999-09-09T09:09:09.999Z, 1999-12-09T09:09:09.999Z") }


### overlap-bins ###
  * Return Value:
    * A `interval` value representing the bin containing the `time-to-bin` value. Note that the internal type of this interval value should be the same as the `time-to-bin` type.

 * Syntax:

        overlap-bins(interval, time-bin-anchor, duration-bin-size)

 * Returns an ordered list of `interval` values representing each bin that is overlapping the `interval`.
 * Arguments:
    * `interval`: an `interval` value
    * `time-bin-anchor`: a date/time/datetime value representing an anchor of a bin starts. The type of this argument should be the same as the first `time-to-bin` argument.
    * `duration-bin-size`: the duration value representing the size of the bin, in the type of year-month-duration or day-time-duration. The type of this duration should be compatible with the type of `time-to-bin`, so that the arithmetic operation between `time-to-bin` and `duration-bin-size` is well-defined. Currently AsterixDB supports the following arithmetic operations:
        * datetime +|- year-month-duration
        * datetime +|- day-time-duration
        * date +|- year-month-duration
        * date +|- day-time-duration
        * time +|- day-time-duration
  * Return Value:
    * A ordered list of `interval` values representing each bin that is overlapping the `interval`. Note that the internal type as `time-to-bin` and `duration-bin-size`.

  * Example:

        let $itv1 := interval-from-time(time("17:23:37"), time("18:30:21"))
        let $itv2 := interval-from-date(date("1984-03-17"), date("2013-08-22"))
        let $itv3 := interval-from-datetime(datetime("1800-01-01T23:59:48.938"), datetime("2015-07-26T13:28:30.218"))
        return { "timebins": overlap-bins($itv1, time("00:00:00"), day-time-duration("PT30M")),
          "datebins": overlap-bins($itv2, date("1990-01-01"), year-month-duration("P20Y")),
          "datetimebins": overlap-bins($itv3, datetime("1900-01-01T00:00:00.000"), year-month-duration("P100Y")) }

   * The expected result is:

        { "timebins": [ interval-time("17:00:00.000Z, 17:30:00.000Z"), interval-time("17:30:00.000Z, 18:00:00.000Z"), interval-time("18:00:00.000Z, 18:30:00.000Z"), interval-time("18:30:00.000Z, 19:00:00.000Z") ],
          "datebins": [ interval-date("1970-01-01, 1990-01-01"), interval-date("1990-01-01, 2010-01-01"), interval-date("2010-01-01, 2030-01-01") ],
          "datetimebins": [ interval-datetime("1800-01-01T00:00:00.000Z, 1900-01-01T00:00:00.000Z"), interval-datetime("1900-01-01T00:00:00.000Z, 2000-01-01T00:00:00.000Z"), interval-datetime("2000-01-01T00:00:00.000Z, 2100-01-01T00:00:00.000Z") ] }


## <a id="RecordFunctions">Record Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##


### get-record-fields ###
 * Syntax:

        get-record-fields(input_record)

 * Access the record field names, type and open status for a given record.
 * Arguments:
    * `input_record` : a record value.
 * Return Value:
    * An order list of `record` values that include the field-name `string`, field-type `string`, is-open `boolean` (used for debug purposes only: `true` if field is open and `false` otherwise), and optional nested `orderedList` for the values of a nested record.

 * Example:

        let $r1 := {"id": 1,
            "project": "AsterixDB",
            "address": {"city": "Irvine", "state": "CA"},
            "related": ["Hivestrix", "Preglix", "Apache VXQuery"] }
        return get-record-fields($r1)

 * The expected result is:

        [ { "field-name": "id", "field-type": "INT64", "is-open": false },
          { "field-name": "project", "field-type": "STRING", "is-open": false },
          { "field-name": "address", "field-type": "RECORD", "is-open": false, "nested": [
            { "field-name": "city", "field-type": "STRING", "is-open": false },
            { "field-name": "state", "field-type": "STRING", "is-open": false } ] },
          { "field-name": "related", "field-type": "ORDEREDLIST", "is-open": false, "list": [
            { "field-type": "STRING" },
            { "field-type": "STRING" },
            { "field-type": "STRING" } ] } ]

 ]
### get-record-field-value ###
 * Syntax:

        get-record-field-value(input_record, string)

 * Access the field name given in the `string_expression` from the `record_expression`.
 * Arguments:
    * `input_record` : A `record` value.
    * `string` : A `string` representing the top level field name.
 * Return Value:
    * An `any` value saved in the designated field of the record.

 * Example:

        let $r1 := {"id": 1,
            "project": "AsterixDB",
            "address": {"city": "Irvine", "state": "CA"},
            "related": ["Hivestrix", "Preglix", "Apache VXQuery"] }
        return get-record-field-value($r1, "project")

 * The expected result is:

        "AsterixDB"

### record-remove-fields ###
 * Syntax:

        record-remove-fields(input_record, field_names)

 * Remove indicated fields from a record given a list of field names.
 * Arguments:
    * `input_record`:  a record value.
    * `field_names`: an ordered list of strings and/or ordered list of ordered list of strings.

 * Return Value:
    * A new record value without the fields listed in the second argument.


 * Example:

        let $r1 := {"id":1,
            "project":"AsterixDB",
            "address":{"city":"Irvine", "state":"CA"},
            "related":["Hivestrix", "Preglix", "Apache VXQuery"] }
        return remove-fields($r1, [["address", "city"], "related"])

 * The expected result is:

        { "id":1,
        "project":"AsterixDB",
        "address":{"state":"CA"}}

### record-add-fields ###
 * Syntax:

        record-add-fields(input_record, fields)

 * Add fields from a record given a list of field names.
 * Arguments:
    * `input_record` : a record value.
    * `fields`: an ordered list of field descriptor records where each record has field-name and  field-value.
 * Return Value:
    * A new record value with the new fields included.


 * Example:

        let $r1 := {"id":1,
            "project":"AsterixDB",
            "address":{"city":"Irvine", "state":"CA"},
            "related":["Hivestrix", "Preglix", "Apache VXQuery"] }
        return record-add-fields($r1, [{"field-name":"employment-location", "field-value":create-point(30.0,70.0)}])

 * The expected result is:

        {"id":1,
           "project":"AsterixDB",
           "address":{"city":"Irvine", "state":"CA"},
           "related":["Hivestrix", "Preglix", "Apache VXQuery"]
           "employment-location": point("30.0,70.0")}

### record-merge ###
 * Syntax:

        record-merge(record1, record2)

 * Merge two different records into a new record.
 * Arguments:
    * `record1` : a record value.
    * `record2` : a record value.
 * Return Value:
    * A new record value with fields from both input records. If a field’s names in both records are the same, an exception is issued.


 * Example:

        let $r1 := {"id":1,
            "project":"AsterixDB",
            "address":{"city":"Irvine", "state":"CA"},
            "related":["Hivestrix", "Preglix", "Apache VXQuery"] }

        let $r2 := {"user_id": 22,
           "employer": "UC Irvine",
           "employment-type": "visitor" }
        return  record-merge($r1, $r2)

 * The expected result is:

        {"id":1,
         "project":"AsterixDB",
         "address":{"city":"Irvine", "state":"CA"},
         "related":["Hivestrix", "Preglix", "Apache VXQuery"]
         "user-id": 22,
         "employer": "UC Irvine",
         "employment-type": "visitor"}

## <a id="OtherFunctions">Other Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

### create-uuid ###
 * Syntax:

        create-uuid()

* Generates a `uuid`.
* Arguments:
    * none
* Return Value:
    * A generated `uuid`.


### is-null ###
 * Syntax:

        is-null(var)

 * Checks whether the given variable is a `null` value.
 * Arguments:
    * `var` : A variable (any type is allowed).
 * Return Value:
    * A `boolean` on whether the variable is a `null` or not.

 * Example:

        for $m in ['hello', 'world', null]
        where not(is-null($m))
        return $m


 * The expected result is:

        "hello"
        "world"

### is-system-null ###
 * Syntax:

        is-system-null(var)

 * Checks whether the given variable is a `system null` value.
 * Arguments:
    * `var` : A variable (any type is allowed).
 * Return Value:
    * A `boolean` on whether the variable is a `system null` or not.


### len ###
 * Syntax:

    len(list)

 * Returns the length of the list list.
 * Arguments:
    * `list` : An `OrderedList`, `UnorderedList` or `null`, represents the list need to be checked.
 * Return Value:
    * An `Int32` that represents the length of list.

 * Example:

        use dataverse TinySocial;

        let $l := ["ASTERIX", "Hyracks"]
        return len($l)


 * The expected result is:

        2


### not ###
 * Syntax:

        not(var)

 * Inverts a `boolean` value
 * Arguments:
    * `var` : A `boolean` (or `null`)
 * Return Value:
    * A `boolean`, the inverse of `var`. returns `null` if `var` is null
 * Example:

        for $m in ['hello', 'world', null]
        where not(is-null($m))
        return $m

 * The expected result is:

        "hello"
        "world"


### range ###
 * Syntax:

        range(start_numeric_value, end_numeric_value)

* Generates a series of `int64` values based start the `start_numeric_value` until the `end_numeric_value`.
  The `range` fucntion must be used list argument of a `for` expression.
* Arguments:
   * `start_numeric_value`: A `int8`/`int16`/`int32`/`int64` value representing the start value.
   * `end_numeric_value`: A `int8`/`int16`/`int32`/`int64` value representing the max final value.
* Return Value:
    * A generated `uuid`.
* Example:

        for $i in range(0, 3)
        return $i;

 * The expected result is:

        [ 0
        , 1
        , 2
        , 3
        ]


### switch-case ###
 * Syntax:

        switch-case(condition,
            case1, case1-result,
            case2, case2-result,
            ...,
            default, default-result
        )

 * Switches amongst a sequence of cases and returns the result of the first matching case. If no match is found, the result of the default case is returned.
 * Arguments:
    * `condition`: A variable (any type is allowed).
    * `caseI/default`: A variable (any type is allowed).
    * `caseI/default-result`: A variable (any type is allowed).
 * Return Value:
    * Returns `caseI-result` if `condition` matches `caseI`, otherwise `default-result`.
 * Example 1:

        switch-case("a",
            "a", 0,
            "x", 1,
            "y", 2,
            "z", 3
        )


 * The expected result is:

        0

 * Example 2:

        switch-case("a",
            "x", 1,
            "y", 2,
            "z", 3
        )

 * The expected result is:

        3


### deep-equal ###
* Syntax:

        deep-equal(var1, var2)


 * Assess the equality between two variables of any type (e.g., records and lists). Two objects are deeply equal iff both their types and values are equal.
 * Arguments:
    * `var1` : a data value, such as record and list.
    * `var2`: a data value, such as record and list.
 * Return Value:
    * `true` or `false` depending on the data equality.


 * Example:

        let $r1 := {"id":1,
            "project":"AsterixDB",
            "address":{"city":"Irvine", "state":"CA"},
            "related":["Hivestrix", "Preglix", "Apache VXQuery"] }

        let $r2 := {"id":1,
                    "project":"AsterixDB",
                    "address":{"city":"San Diego", "state":"CA"},
                    "related":["Hivestrix", "Preglix", "Apache VXQuery"] }
        return deep-equal($r1, $r2)

 * The expected result is:

        false