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
           * [Identifiers and Variable References](#Variable_references)
		   * [Parameter References](#Parameter_references)
           * [Parenthesized Expressions](#Parenthesized_expressions)
           * [Function calls](#Function_call_expressions)
           * [Case Expressions](#Case_expressions)
           * [Constructors](#Constructors)
* [3. Queries](#Queries)
      * [SELECT Clauses](#Select_clauses)
           * [Select Value](#Select_element)
           * [SQL-style Select](#SQL_select)
           * [Select *](#Select_star)
           * [Select Distinct](#Select_distinct)
           * [Unnamed Projections](#Unnamed_projections)
           * [Abbreviated Field Access Expressions](#Abbreviated_field_access_expressions)
      * [FROM clauses](#From_clauses)
           * [Joins](#Joins)
	  * [LET Clauses](#Let_clauses)
	  * [WHERE Clause](#WHERE_Clause)
      * [Grouping](#Grouping)
           * [GROUP BY Clause](#GROUP_BY_Clause)
           * [HAVING Clause](#HAVING_Clause)
		   * [Aggregation Pseudo-functions](#Aggregation_PseudoFunctions)
           * [GROUP AS Clause](#GROUP_AS_Clause)
      * [Selection and UNION ALL](#Union_all)
	  * [WITH Clauses](#With_clauses)
      * [ORDER By and LIMIT Clauses](#Order_By_clauses)
	  * [Subqueries](#Subqueries)
* [4. Window Functions](#Over_clauses)
      * [Window Function Call](#Window_function_call)
	       * [Window Function Arguments](#Window_function_arguments)
           * [Window Function Options](#Window_function_options)
           * [Window Frame Variable](#Window_frame_variable)
           * [Window Definition](#Window_definition)
* [5. Errors](#Errors)
      * [Syntax Errors](#Syntax_errors)
      * [Identifier Resolution Errors](#Identifier_resolution_errors)
      * [Type Errors](#Type_errors)
      * [Resource Errors](#Resource_errors)
* [6.Differences from SQL-92](#Vs_SQL-92)
* [7. DDL and DML Statements](#DDL_and_DML_statements)
      * [Lifecycle Management Statements](#Lifecycle_management_statements)
		   * [Use Statement](#Use)
		   * [Set Statement](#Sets)
		   * [Function Declaration](#Functions)
		   * [Create Statement](#Create)
			* [Create Dataverse](#Dataverses)
			* [Create Type](#Types)
			* [Create Dataset](#Datasets)
			* [Create Index](#Indices)
			* [Create Synonym](#Synonyms)
			* [Create Function](#Create_function)
		   * [Drop Statement](#Removal)
		   * [Load Statement](#Load_statement)
      * [Modification Statements](#Modification_statements)
           * [Insert Statement](#Inserts)
           * [Upsert Statement](#Upserts)
           * [Delete Statement](#Deletes)
* [Appendix 1. Reserved Keywords](#Reserved_keywords)
* [Appendix 2. Performance Tuning](#Performance_tuning)
      * [Parallelism Parameter](#Parallelism_parameter)
      * [Memory Parameters](#Memory_parameters)
      * [Query Hints](#Query_hints)
* [Appendix 3. Variable Bindings and Name Resolution](#Variable_bindings_and_name_resolution)
* [Appendix 4. Example Data](#Manual_data)
	  * [Data Definitions](#definition_statements)
	  * [Customers Dataset](#customers_data)
	  * [Orders Dataset](#orders_data)
