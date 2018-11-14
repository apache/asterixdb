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

## <a id="AggregateFunctions">Aggregate Functions (Array Functions) </a> ##

This section contains detailed descriptions of each aggregate function (i.e., array function) in the query language.
Note that standard SQL aggregate functions (e.g., `MIN`, `MAX`, `SUM`, `COUNT`, and `AVG`)
are not real functions in the query language, but just syntactic sugars over corresponding
builtin aggregate functions (e.g., `ARRAY_MIN`, `ARRAY_MAX`,
`ARRAY_SUM`, `ARRAY_COUNT`, and `ARRAY_AVG`).

### array_count ###
 * Syntax:

        array_count(collection)

 * Gets the number of non-null and non-missing items in the given collection.
 * Arguments:
    * `collection` could be:
        * an `array` or `multiset` to be counted,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `bigint` value representing the number of non-null and non-missing items in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * any other non-array and non-multiset input value will cause an error.

 * Example:

        array_count( ['hello', 'world', 1, 2, 3, null, missing] );


 * The expected result is:

        5


### array_avg ###

 * Syntax:

        array_avg(num_collection)

 * Gets the average value of the non-null and non-missing numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the average of the non-null and non-missing numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if the given collection does not contain any non-null and non-missing items,
    * any other non-array and non-multiset input value will cause a type error,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        array_avg( [1.2, 2.3, 3.4, 0, null] );

 * The expected result is:

        1.725


### array_sum ###
 * Syntax:

        array_sum(num_collection)

 * Gets the sum of non-null and non-missing items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * the sum of the non-null and non-missing numbers in the given collection.
      The returning type is decided by the item type with the highest
      order in the numeric type promotion order (`tinyint`-> `smallint`->`integer`->`bigint`->`float`->`double`) among
      items.
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if the given collection does not contain any non-null and non-missing items,
    * any other non-array and non-multiset input value will cause a type error,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        array_sum( [1.2, 2.3, 3.4, 0, null, missing] );

 * The expected result is:

        6.9


### array_min ###
 * Syntax:

        array_min(num_collection)

 * Gets the min value of non-null and non-missing comparable items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset`,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * the min value of non-null and non-missing values in the given collection.
      The returning type is decided by the item type with the highest order in the
      type promotion order (`tinyint`-> `smallint`->`integer`->`bigint`->`float`->`double`) among numeric items.
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if the given collection does not contain any non-null and non-missing items,
    * multiple incomparable items in the input array or multiset will cause a type error,
    * any other non-array and non-multiset input value will cause a type error.

 * Example:

        array_min( [1.2, 2.3, 3.4, 0, null, missing] );

 * The expected result is:

        0.0


### array_max ###
 * Syntax:

        array_max(num_collection)

 * Gets the max value of the non-null and non-missing comparable items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset`,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * the max value of non-null and non-missing numbers in the given collection.
      The returning type is decided by the item type with the highest order in the
      type promotion order (`tinyint`-> `smallint`->`integer`->`bigint`->`float`->`double`) among numeric items.
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if the given collection does not contain any non-null and non-missing items,
    * multiple incomparable items in the input array or multiset will cause a type error,
    * any other non-array and non-multiset input value will cause a type error.

 * Example:

        array_max( [1.2, 2.3, 3.4, 0, null, missing] );

 * The expected result is:

        3.4


### array_stddev ###

 * Syntax:

        array_stddev(num_collection)

 * Gets the sample standard deviation value of the non-null and non-missing numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the sample standard deviation of the non-null and non-missing numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if the given collection does not contain any non-null and non-missing items,
    * any other non-array and non-multiset input value will cause a type error,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        array_stddev( [1.2, 2.3, 3.4, 0, null] );

 * The expected result is:

        1.4591664287073858

### array_stddev_pop ###

 * Syntax:

        array_stddev_pop(num_collection)

 * Gets the population standard deviation value of the non-null and non-missing numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the population standard deviation of the non-null and non-missing numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if the given collection does not contain any non-null and non-missing items,
    * any other non-array and non-multiset input value will cause a type error,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        array_stddev_pop( [1.2, 2.3, 3.4, 0, null] );

 * The expected result is:

        1.2636751956100112

### array_var ###

 * Syntax:

        array_var(num_collection)

 * Gets the sample variance value of the non-null and non-missing numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the sample variance of the non-null and non-missing numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if the given collection does not contain any non-null and non-missing items,
    * any other non-array and non-multiset input value will cause a type error,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        array_var( [1.2, 2.3, 3.4, 0, null] );

 * The expected result is:

        2.1291666666666664

### array_var_pop ###

 * Syntax:

        array_var_pop(num_collection)

 * Gets the population variance value of the non-null and non-missing numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the population variance of the non-null and non-missing numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if the given collection does not contain any non-null and non-missing items,
    * any other non-array and non-multiset input value will cause a type error,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        array_var_pop( [1.2, 2.3, 3.4, 0, null] );

 * The expected result is:

        1.5968749999999998


### strict_count ###
 * Syntax:

        strict_count(collection)

 * Gets the number of items in the given collection.
 * Arguments:
    * `collection` could be:
        * an `array` or `multiset` containing the items to be counted,
        * or a `null` value,
        * or a `missing` value.
 * Return Value:
    * a `bigint` value representing the number of items in the given collection,
    * `null` is returned if the input is `null` or `missing`.

 * Example:

        strict_count( [1, 2, null, missing] );

 * The expected result is:

        4

### strict_avg ###
 * Syntax:

        strict_avg(num_collection)

 * Gets the average value of the numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the average of the numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if there is a `null` or `missing` in the input collection,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        strict_avg( [100, 200, 300] );

 * The expected result is:

        200.0

### strict_sum ###
 * Syntax:

        strict_sum(num_collection)

 * Gets the sum of the items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * the sum of the numbers in the given collection. The returning type is decided by the item type with the highest
      order in the numeric type promotion order (`tinyint`-> `smallint`->`integer`->`bigint`->`float`->`double`) among
      items.
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if there is a `null` or `missing` in the input collection,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        strict_sum( [100, 200, 300] );

 * The expected result is:

        600

### strict_min ###
 * Syntax:

        strict_min(num_collection)

 * Gets the min value of comparable items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset`,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * the min value of the given collection.
      The returning type is decided by the item type with the highest order in the type promotion order
      (`tinyint`-> `smallint`->`integer`->`bigint`->`float`->`double`) among numeric items.
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if there is a `null` or `missing` in the input collection,
    * multiple incomparable items in the input array or multiset will cause a type error,
    * any other non-array and non-multiset input value will cause a type error.

 * Example:

        strict_min( [10.2, 100, 5] );

 * The expected result is:

        5.0


### strict_max ###
 * Syntax:

        strict_max(num_collection)

 * Gets the max value of numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset`,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * The max value of the given collection.
      The returning type is decided by the item type with the highest order in the type promotion order
      (`tinyint`-> `smallint`->`integer`->`bigint`->`float`->`double`) among numeric items.
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if there is a `null` or `missing` in the input collection,
    * multiple incomparable items in the input array or multiset will cause a type error,
    * any other non-array and non-multiset input value will cause a type error.

 * Example:

        strict_max( [10.2, 100, 5] );

 * The expected result is:

        100.0

### strict_stddev ###
 * Syntax:

        strict_stddev(num_collection)

 * Gets the sample standard deviation value of the numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the sample standard deviation of the numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if there is a `null` or `missing` in the input collection,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        strict_stddev( [100, 200, 300] );

 * The expected result is:

        100.0

### strict_stddev_pop ###
 * Syntax:

        strict_stddev_pop(num_collection)

 * Gets the population standard deviation value of the numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the population standard deviation of the numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if there is a `null` or `missing` in the input collection,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        strict_stddev_pop( [100, 200, 300] );

 * The expected result is:

        81.64965809277261

### strict_var ###
 * Syntax:

        strict_var(num_collection)

 * Gets the sample variance value of the numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the sample variance of the numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if there is a `null` or `missing` in the input collection,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        strict_var( [100, 200, 300] );

 * The expected result is:

        10000.0

### strict_var_pop ###
 * Syntax:

        strict_var_pop(num_collection)

 * Gets the population variance value of the numeric items in the given collection.
 * Arguments:
    * `num_collection` could be:
        * an `array` or `multiset` containing numeric values, `null`s or `missing`s,
        * or, a `null` value,
        * or, a `missing` value.
 * Return Value:
    * a `double` value representing the population variance of the numbers in the given collection,
    * `null` is returned if the input is `null` or `missing`,
    * `null` is returned if there is a `null` or `missing` in the input collection,
    * any other non-numeric value in the input collection will cause a type error.

 * Example:

        strict_var_pop( [100, 200, 300] );

 * The expected result is:

        6666.666666666667
