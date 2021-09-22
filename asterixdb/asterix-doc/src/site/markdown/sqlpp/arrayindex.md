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

# AsterixDB Support of Array Indexes #

## <a id="toc">Table of Contents</a> ##

* [Overview](#Overview)
* [Quantification Queries](#QuantificationQueries)
* [Explicit Unnesting Queries](#ExplicitUnnestQueries)
* [Join Queries](#JoinQueries)
* [Complex Indexing Examples](#ComplexIndexingExamples)


## <a id="Overview">Overview</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

Array indexes are used in applications where users want to accelerate a query that involves some array-valued or multiset-valued field.
This enables fast evaluation of predicates in queries involving arrays or multisets in datasets.
For brevity, all further mentions of array-valued fields are also applicable to multiset-valued fields.

Array-valued fields are a natural data modeling concept for documents.
In the traditional inventory management example, it is natural for the line items of an order to exist as a part of the order itself.
Previously if an AsterixDB user wanted to optimize a query involving a predicate on the line items of an order, they would a) have to undertake some form of schema migration to separate the line items from the orders into different datasets, b) create an index on the new dataset for line items, and finally c) modify their query to join orders and line items.
With the introduction of array indexes in AsterixDB, users can keep their arrays intact and still reap the performance benefits of an index.

It should be noted that in AsterixDB, array indexes are *not* meant to serve as covering indexes.
In fact due to AsterixDB's record-level locking, index-only plans involving multi-valued fields (i.e. array indexes and inverted indexes) are not currently possible.
Instead, array indexes are simply meant to accelerate queries involving multi-valued fields.


## <a id="QuantificationQueries">Quantification Queries</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

A common use-case for array indexes involves quantifying some or all elements within an array.
Quantification queries have two variants: existential and universal.
Existential queries ask if *any* element in some array satisfies a given predicate.
Membership queries are a specific type of existential query, asking if any element in some array is equal to a particular value.
Universal queries ask if *all* elements in some array satisfy a particular predicate.
Empty arrays are not stored in an array index, meaning that a user must additionally specify that the array is non-empty to tell AsterixDB that it is possible to use an array index as an access method for the given query.

All query examples here will use the orders and products datasets below.

    CREATE TYPE ordersType AS {
        orderno:        int,
        custid:			string,
        items:          [{ itemno: int, productno: int, qty: int, price: float }]
    };
    CREATE DATASET orders (ordersType) PRIMARY KEY orderno;

    CREATE TYPE productsType AS {
        productno:      int,
        categories:     {{ string }}
    };
    CREATE DATASET products (productsType) PRIMARY KEY productno;

Let us now create an index on the `categories` multiset of the `products` dataset.

    CREATE INDEX pCategoriesIdx ON products (UNNEST categories) EXCLUDE UNKNOWN KEY;

Suppose we now want to find all products that have the category "Food".
The following membership query will utilize the index we just created.

    SELECT p
    FROM products p
    WHERE "Food" IN p.categories;

We can also rewrite the query above as an explicit existential quantification query with an equality predicate and the index will be utilized.

    SELECT p
    FROM products p
    WHERE SOME c IN p.categories SATISFIES c = "Food";

Let us now create an index on the `qty` and `price` fields in the `items` array of the `orders` dataset.

    CREATE INDEX oItemsQtyPriceIdx ON orders (UNNEST items SELECT qty, price) EXCLUDE UNKNOWN KEY;

Now suppose we want to find all orders that only have items with large quantities and low prices, not counting orders without any items.
The following universal quantification query will utilize the index we just created.

    SELECT o
    FROM orders o
    WHERE SOME AND EVERY i IN o.items SATISFIES i.qty > 100 AND i.price < 5.00;

Take note of the `SOME AND EVERY` quantifier instead of the `EVERY` quantifier.
Array indexes cannot be used for queries with potentially empty arrays.


## <a id="ExplicitUnnestQueries">Explicit Unnesting Queries</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

Array indexes can also be used to accelerate queries that involve the explicit unnesting of array fields.
We can express the same membership / existential example above using an explicit `UNNEST` query.
(To keep the same cardinality as the query above (i.e. to undo the `UNNEST`), we add a `DISTINCT` clause, though the index would be utilized either way.)

    SELECT DISTINCT p
    FROM products p, p.categories c
    WHERE c = "Food";

As another example, suppose that we want to find all orders that have *some* item with a large quantity.
The following query will utilize the `oItemsQtyPriceIdx` we created, using only the first field in the index `qty`.

    SELECT DISTINCT o
    FROM orders o, o.items i
    WHERE i.qty > 100;


## <a id="JoinQueries">Join Queries</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

Finally, array indexes can also be used for index nested-loop joins if the field being joined is located within an array.
Let us create another index on the `items` array of the `orders` dataset, this time on the `productno` field.

    CREATE INDEX oProductIDIdx ON orders (UNNEST items SELECT productno) EXCLUDE UNKNOWN KEY;

Now suppose we want to find all products located in a specific order.
We can accomplish this with the join query below.
Note that we must specify the `indexnl` join hint to tell AsterixDB that we want to optimize this specific join, as hash joins are the default join method otherwise.

    SELECT DISTINCT p
    FROM products p, orders o
    WHERE o.custid = "C41" AND 
          SOME i IN o.items SATISFIES i.productno /*+ indexnl */ = p.productno;


## <a id="ComplexIndexingExamples">Complex Indexing Examples</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

### Open Indexes

Similar to atomic indexes, array indexes are not limited to closed fields.
The following DDLs illustrate how we could express `CREATE INDEX` statements comparable to those above if the to-be-indexed fields were not included in the their dataset's type definitions.

    CREATE INDEX pCategoriesIdx ON products (UNNEST categories : string) EXCLUDE UNKNOWN KEY;
    CREATE INDEX oItemsQtyPriceIdx ON orders (UNNEST items SELECT qty : int, price : int) EXCLUDE UNKNOWN KEY;
    CREATE INDEX oProductIDIdx ON orders (UNNEST items SELECT productno : int) EXCLUDE UNKNOWN KEY;

### Composite Atomic-Array Indexes

Indexed elements within array indexes are also not limited to fields within arrays.
The following DDLs demonstrate indexing fields that are within an array and fields that are outside any array.

    CREATE INDEX oOrderNoItemPriceIdx ON orders (orderno, ( UNNEST items SELECT price )) EXCLUDE UNKNOWN KEY;
    CREATE INDEX oOrderItemPriceNoIdx ON orders (( UNNEST items SELECT price ), orderno) EXCLUDE UNKNOWN KEY;

### Arrays in Arrays

Array indexes are not just limited to arrays of depth = 1.
We can generalize this to arrays of arbitrary depth, as long as an object encapsulates each array.
The following DDLs describe indexing the `qty` field in an `items` array at various depths.

    // { orderno: ..., items0: [ { items1: [ { qty: int, ... } ] } ] }
    CREATE INDEX oItemItemQtyIdx ON orders (UNNEST items0 UNNEST items1 SELECT qty) EXCLUDE UNKNOWN KEY;

    // { orderno: ..., items0: [ { items1: [ { items2: [ { qty: int, ... } ] } ] } ] }
    CREATE INDEX oItemItemItemQtyIdx ON orders (UNNEST items0 UNNEST items1 UNNEST items2 SELECT qty) EXCLUDE UNKNOWN KEY;

The queries below will utilize the indexes above.
The first query utilizes the `oItemItemQtyIdx` index through nested existential quantification.
The second query utilizes the `oItemItemItemQtyIdx` index with three unnesting clauses.

    SELECT o
    FROM orders o
    WHERE SOME o0 IN o.items0 SATISFIES (
        SOME o1 IN o0.items1 SATISFIES o1.qty = 100
    );

    SELECT DISTINCT o
    FROM orders o, o.items0 o0, o0.items1 o1, o1.items2 o2
    WHERE o2.qty = 100;
