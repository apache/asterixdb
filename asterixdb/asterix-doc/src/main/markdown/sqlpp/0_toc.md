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

# The SQL++ Query Language

## <a id="toc">Table of Contents</a> ##

* [1. Introduction](#Introduction)
* [2. Expressions](#Expressions)
      * [Primary expressions](#Primary_expressions)
           * [Literals](#Literals)
           * [Variable references](#Variable_references)
           * [Parenthesized expressions](#Parenthesized_expressions)
           * [Function call expressions](#Function_call_expressions)
           * [Constructors](#Constructors)
      * [Path expressions](#Path_expressions)
      * [Operator expressions](#Operator_expressions)
           * [Arithmetic operators](#Arithmetic_operators)
           * [Collection operators](#Collection_operators)
           * [Comparison operators](#Comparison_operators)
           * [Logical operators](#Logical_operators)
      * [Case expressions](#Case_expressions)
      * [Quantified expressions](#Quantified_expressions)
* [3. Queries](#Queries)
      * [SELECT statements](#SELECT_statements)
      * [SELECT clauses](#Select_clauses)
           * [Select element/value/raw](#Select_element)
           * [SQL-style select](#SQL_select)
           * [Select *](#Select_star)
           * [Select distinct](#Select_distinct)
           * [Unnamed projections](#Unnamed_projections)
           * [Abbreviatory field access expressions](#Abbreviatory_field_access_expressions)
      * [UNNEST clauses](#Unnest_clauses)
           * [Inner unnests](#Inner_unnests)
           * [Left outer unnests](#Left_outer_unnests)
           * [Expressing joins using unnests](#Expressing_joins_using_unnests)
      * [FROM clauses](#From_clauses)
           * [Binding expressions](#Binding_expressions)
           * [Multiple from terms](#Multiple_from_terms)
           * [Expressing joins using from terms](#Expressing_joins_using_from_terms)
           * [Implicit binding variables](#Implicit_binding_variables)
      * [JOIN clauses](#Join_clauses)
           * [Inner joins](#Inner_joins)
           * [Left outer joins](#Left_outer_joins)
      * [GROUP BY clauses](#Group_By_clauses)
           * [Group variables](#Group_variables)
           * [Implicit group key variables](#Implicit_group_key_variables)
           * [Implicit group variables](#Implicit_group_variables)
           * [Aggregation functions](#Aggregation_functions)
           * [SQL-92 aggregation functions](#SQL-92_aggregation_functions)
           * [SQL-92 compliant GROUP BY aggregations](#SQL-92_compliant_gby)
           * [Column aliases](#Column_aliases)
      * [WHERE clauases and HAVING clauses](#Where_having_clauses)
      * [ORDER BY clauses](#Order_By_clauses)
      * [LIMIT clauses](#Limit_clauses)
      * [WITH clauses](#With_clauses)
      * [LET clauses](#Let_clauses)
      * [UNION ALL](#Union_all)
      * [SQL++ Vs. SQL-92](#Vs_SQL-92)
* [4. Errors](#Errors)
      * [Syntax errors](#Syntax_errors)
      * [Identifier resolution errors](#Parsing_errors)
      * [Type errors](#Type_errors)
      * [Resource errors](#Resource_errors)
* [5. DDL and DML statements](#DDL_and_DML_statements)
      * [Declarations](#Declarations)
      * [Lifecycle management statements](#Lifecycle_management_statements)
           * [Dataverses](#Dataverses)
           * [Datasets](#Datasets)
           * [Types](#Types)
           * [Functions](#Functions)
      * [Modification statements](#Modification_statements)
           * [Inserts](#Inserts)
           * [Upserts](#Upserts)
           * [Deletes](#Deletes)
* [Appendix 1. Reserved keywords](#Reserved_keywords)

