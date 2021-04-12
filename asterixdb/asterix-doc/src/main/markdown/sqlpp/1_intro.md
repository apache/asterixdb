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

# <a id="Introduction">1. Introduction</a>

This document is intended as a reference guide to the full syntax and semantics of
AsterixDB's query language, a SQL-based language for working with semistructured data.
The language is a derivative of SQL++, a declarative query language for JSON data which
is largely backwards compatible with SQL.
SQL++ originated from research in the FORWARD project at UC San Diego, and it has
much in common with SQL; some differences exist due to the different data models that
the two languages were designed to serve.
SQL was designed for interacting with the flat, schema-ified world of relational
databases, while SQL++ generalizes SQL to also handle nested data formats (like JSON) and
the schema-optional (or even schema-less) data models of modern NoSQL and BigData systems.

In the context of Apache AsterixDB, SQL++ is intended for working with the Asterix Data Model
([ADM](../datamodel.html)), a data model based on a superset of JSON with an enriched and flexible type system.
New AsterixDB users are encouraged to read and work through the (much friendlier) guide
"[AsterixDB 101: An ADM and SQL++ Primer](primer-sqlpp.html)" before attempting to make use of this document.
In addition, readers are advised to read through the [Asterix Data Model (ADM) reference guide](../datamodel.html)
first as well, as an understanding of the data model is a prerequisite to understanding SQL++.

In what follows, we detail the features of the SQL++ language in a grammar-guided manner.
We list and briefly explain each of the productions in the query grammar, offering examples
(and results) for clarity. In this manual, we will explain how to use the various features of SQL++
using two datasets named `customers` and `orders`. Each dataset is a collection of objects.
The contents of the example datasets can be found at the end of this manual in [Appendix 4](#Manual_data).

For additional reading on SQL++ and more examples, refer to [SQL++ for SQL Users: A Tutorial](https://asterixdb.apache.org/files/SQL_Book.pdf).
