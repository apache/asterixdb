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

This section contains detailed descriptions of each AQL aggregate function (i.e., array function).


### sql-count ###
 * Syntax:

        sql-count(collection)

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

        sql-count( ['hello', 'world', 1, 2, 3, null, missing] );


 * The expected result is:

        5


### sql-avg ###

 * Syntax:

        sql-avg(num_collection)

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

        sql-avg( [1.2, 2.3, 3.4, 0, null] );

 * The expected result is:

        1.725


### sql-sum ###
 * Syntax:

        sql-sum(num_collection)

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

        sql-sum( [1.2, 2.3, 3.4, 0, null, missing] );

 * The expected result is:

        6.9


### sql-sql_min ###
 * Syntax:

        sql-min(num_collection)

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

        sql-min( [1.2, 2.3, 3.4, 0, null, missing] );

 * The expected result is:

        0.0


### sql-max ###
 * Syntax:

        sql-max(num_collection)

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

        sql-max( [1.2, 2.3, 3.4, 0, null, missing] );

 * The expected result is:

        3.4


### count ###
 * Syntax:

        count(collection)

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

        count( [1, 2, null, missing] );

 * The expected result is:

        4

### avg ###
 * Syntax:

        avg(num_collection)

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

        avg( [100, 200, 300] );

 * The expected result is:

        [ 200.0 ]

### sum ###
 * Syntax:

        sum(num_collection)

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

        sum( [100, 200, 300] );

 * The expected result is:

        600

### sql-min ###
 * Syntax:

        min(num_collection)

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

        min( [10.2, 100, 5] );

 * The expected result is:

        5.0


### sql-max ###
 * Syntax:

        max(num_collection)

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

        max( [10.2, 100, 5] );

 * The expected result is:

        100.0

