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

# <a id="Introduction">1. Introduction</a><font size="3"/>

This document is intended as a reference guide to the full syntax and semantics of
the SQL++ Query Language, a SQL-inspired language for working with semistructured data.
SQL++ has much in common with SQL, but some differences do exist due to the different
data models that the two languages were designed to serve.
SQL was designed in the 1970's for interacting with the flat, schema-ified world of
relational databases, while SQL++ is much newer and targets the nested, schema-optional
(or even schema-less) world of modern NoSQL systems.

In the context of Apache AsterixDB, SQL++ is intended for working with the Asterix Data Model
([ADM](../datamodel.html)),a data model based on a superset of JSON with an enriched and flexible type system.
New AsterixDB users are encouraged to read and work through the (much friendlier) guide
"[AsterixDB 101: An ADM and SQL++ Primer](primer-sqlpp.html)" before attempting to make use of this document.
In addition, readers are advised to read through the [Asterix Data Model (ADM) reference guide](../datamodel.html)
first as well, as an understanding of the data model is a prerequisite to understanding SQL++.

In what follows, we detail the features of the SQL++ language in a grammar-guided manner.
We list and briefly explain each of the productions in the SQL++ grammar, offering examples
(and results) for clarity.

