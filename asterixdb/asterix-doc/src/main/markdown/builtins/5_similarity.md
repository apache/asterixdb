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

## <a id="SimilarityFunctions">Similarity Functions</a> ##

AsterixDB supports queries with different similarity functions,
including [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) and
[Jaccard](https://en.wikipedia.org/wiki/Jaccard_index).

### edit_distance ###
 * Syntax:

        edit_distance(expression1, expression2)

 * Returns the edit distance of `expression1` and `expression2`.
 * Arguments:
    * `expression1` : a `string` or a homogeneous `array` of a comparable item type.
    * `expression2` : The same type as `expression1`.
 * Return Value:
    * an `bigint` that represents the edit distance between `expression1` and `expression2`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.
 * Note: an [n_gram index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
 * Example:

        edit_distance("SuzannaTillson", "Suzanna Tilson");


 * The expected result is:

        2

### edit_distance_check ###
* Syntax:

        edit_distance_check(expression1, expression2, threshold)

* Checks whether the edit distance of `expression1` and `expression2` is within a given threshold.

* Arguments:
    * `expression1` : a `string` or a homogeneous `array` of a comparable item type.
    * `expression2` : The same type as `expression1`.
    * `threshold` : a `bigint` that represents the distance threshold.
* Return Value:
    * an `array` with two items:
        * The first item contains a `boolean` value representing whether the edit distance of `expression1` and `expression2` is within the given threshold.
        * The second item contains an `integer` that represents the edit distance of `expression1` and `expression2` if the first item is true.
        * If the first item is false, then the second item is set to 2147483647.
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first or second argument is any other non-string value,
        * or, the third argument is any other non-bigint value.
* Note: an [n_gram index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
* Example:

        edit_distance_check("happy","hapr",2);


* The expected result is:

        [ true, 2 ]

### edit_distance_contains ###
* Syntax:

        edit_distance_contains(expression1, expression2, threshold)

* Checks whether `expression1` contains `expression2` with an [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) within a given threshold.

* Arguments:
    * `expression1` : a `string` or a homogeneous `array` of a comparable item type.
    * `expression2` : The same type as `expression1`.
    * `threshold` : a `bigint` that represents the distance threshold.
* Return Value:
    * an `array` with two items:
        * The first item contains a `boolean` value representing whether `expression1` can contain `expression2`.
        * The second item contains an `integer` that represents the required edit distance for `expression1` to contain
         `expression2` if the first item is true.
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first or second argument is any other non-string value,
        * or, the third argument is any other non-bigint value.
* Note: an [n_gram index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
* Example:

        edit_distance_contains("happy","hapr",2);


* The expected result is:

        [ true, 1 ]



### similarity_jaccard ###
 * Syntax:

        similarity_jaccard(array1, array2)

 * Returns the [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index) of `array1` and `array2`.
 * Arguments:
    * `array1` : an `array` or `multiset`.
    * `array2` : an `array` or `multiset`.
 * Return Value:
    * a `float` that represents the Jaccard similarity of `array1` and `array2`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * `missing` if any element in any input array is `missing`,
    * `null` if any element in any input array is `null` but no element in the input array is `missing`,
    * any other non-array input value or non-integer element in any input array will cause a type error.

 * Note: a [keyword index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
 * Example:

        similarity_jaccard([1,5,8,9], [1,5,9,10]);


 * The expected result is:

        0.6


### similarity_jaccard_check ###
 * Syntax:

        similarity_jaccard_check(array1, array2, threshold)

 * Checks whether `array1` and `array2` have a [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index) greater than or equal to threshold.  Again, the “check” version of Jaccard is faster than the "non_check" version.

 * Arguments:
    * `array1` : an `array` or `multiset`.
    * `array2` : an `array` or `multiset`.
    * `threshold` : a `double` that represents the similarity threshold.
 * Return Value:
    * an `array` with two items:
        * The first item contains a `boolean` value representing whether `array1` and `array2` are similar.
        * The second item contains a `float` that represents the Jaccard similarity of `array1` and `array2`
         if it is greater than or equal to the threshold, or 0 otherwise.
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * `missing` if any element in any input array is `missing`,
    * `null` if any element in any input array is `null` but no element in the input array is `missing`,
    * a type error will be raised if:
            * the first or second argument is any other non-array value,
            * or, the third argument is any other non-double value.

 * Note: a [keyword index](similarity.html#UsingIndexesToSupportSimilarityQueries) can be utilized for this function.
 * Example:

        similarity_jaccard_check([1,5,8,9], [1,5,9,10], 0.6);


 * The expected result is:

        [ false, 0.0 ]


