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

# The Query Language

* [1. Introduction](#Introduction)
* [2. Expressions](#Expressions)
      * [Operator Expressions](#Operator_expressions)
           * [Arithmetic Operators](#Arithmetic_operators)
           * [Collection Operators](#Collection_operators)
           * [Comparison Operators](#Comparison_operators)
           * [Logical Operators](#Logical_operators)
      * [Quantified Expressions](#Quantified_expressions)
      * [Path Expressions](#Path_expressions)
      * [Primary Expressions](#Primary_expressions)
           * [Literals](#Literals)
           * [Variable References](#Variable_references)
           * [Parenthesized Expressions](#Parenthesized_expressions)
           * [Function call Expressions](#Function_call_expressions)
           * [Case Expressions](#Case_expressions)
           * [Constructors](#Constructors)
* [3. Queries](#Queries)
      * [Declarations](#Declarations)
      * [SELECT Statements](#SELECT_statements)
      * [SELECT Clauses](#Select_clauses)
           * [Select Element/Value/Raw](#Select_element)
           * [SQL-style Select](#SQL_select)
           * [Select *](#Select_star)
           * [Select Distinct](#Select_distinct)
           * [Unnamed Projections](#Unnamed_projections)
           * [Abbreviated Field Access Expressions](#Abbreviated_field_access_expressions)
      * [UNNEST Clauses](#Unnest_clauses)
           * [Inner Unnests](#Inner_unnests)
           * [Left Outer Unnests](#Left_outer_unnests)
           * [Expressing Joins Using Unnests](#Expressing_joins_using_unnests)
      * [FROM clauses](#From_clauses)
           * [Binding Expressions](#Binding_expressions)
           * [Multiple From Terms](#Multiple_from_terms)
           * [Expressing Joins Using From Terms](#Expressing_joins_using_from_terms)
           * [Implicit Binding Variables](#Implicit_binding_variables)
      * [JOIN Clauses](#Join_clauses)
           * [Inner Joins](#Inner_joins)
           * [Left Outer Joins](#Left_outer_joins)
      * [GROUP BY Clauses](#Group_By_clauses)
           * [Group Variables](#Group_variables)
           * [Implicit Group Key Variables](#Implicit_group_key_variables)
           * [Implicit Group Variables](#Implicit_group_variables)
           * [Aggregation Functions](#Aggregation_functions)
           * [SQL-92 Aggregation Functions](#SQL-92_aggregation_functions)
           * [SQL-92 Compliant GROUP BY Aggregations](#SQL-92_compliant_gby)
           * [Column Aliases](#Column_aliases)
      * [WHERE Clauses and HAVING Clauses](#Where_having_clauses)
      * [ORDER BY Clauses](#Order_By_clauses)
      * [LIMIT Clauses](#Limit_clauses)
      * [WITH Clauses](#With_clauses)
      * [LET Clauses](#Let_clauses)
      * [UNION ALL](#Union_all)
      * [OVER Clauses](#Over_clauses)
           * [Window Function Call](#Window_function_call)
           * [Window Function Options](#Window_function_options)
           * [Window Frame Variable](#Window_frame_variable)
           * [Window Definition](#Window_definition)
      * [Differences from SQL-92](#Vs_SQL-92)
* [4. Errors](#Errors)
      * [Syntax Errors](#Syntax_errors)
      * [Identifier Resolution Errors](#Identifier_resolution_errors)
      * [Type Errors](#Type_errors)
      * [Resource Errors](#Resource_errors)
* [5. DDL and DML Statements](#DDL_and_DML_statements)
      * [Lifecycle Management Statements](#Lifecycle_management_statements)
           * [Dataverses](#Dataverses)
           * [Types](#Types)
           * [Datasets](#Datasets)
           * [Indices](#Indices)
           * [Functions](#Functions)
           * [Removal](#Removal)
           * [Load Statement](#Load_statement)
      * [Modification Statements](#Modification_statements)
           * [Inserts](#Inserts)
           * [Upserts](#Upserts)
           * [Deletes](#Deletes)
* [Appendix 1. Reserved Keywords](#Reserved_keywords)
* [Appendix 2. Performance Tuning](#Performance_tuning)
      * [Parallelism Parameter](#Parallelism_parameter)
      * [Memory Parameters](#Memory_parameters)
* [Appendix 3. Variable Bindings and Name Resolution](#Variable_bindings_and_name_resolution)
