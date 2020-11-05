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

Window functions are special functions that compute aggregate values over a "window" of input data. Like an ordinary function, a window function returns a value for every item in the input dataset. But in the case of a window function, the value returned by the function can depend not only on the argument of the function, but also on other items in the same collection. For example, a window function applied to a set of employees might return the rank of each employee in the set, as measured by salary. As another example, a window function applied to a set of items, ordered by purchase date, might return the running total of the cost of the items.

A window function call is identified by an `OVER` clause, which can specify three things: partitioning, ordering, and framing. The partitioning specification is like a `GROUP BY`: it splits the input data into partitions. For example, a set of employees might be partitioned by department. The window function, when applied to a given object, is influenced only by other objects in the same partition. The ordering specification is like an `ORDER BY`: it determines the ordering of the objects in each partition. The framing specification defines a "frame" that moves through the partition, defining how the result for each object depends on nearby objects. For example, the frame for a current object might consist of the two objects before and after the current one; or it might consist of all the objects before the current one in the same partition. A window function call may also specify some options that control (for example) how nulls are handled by the function.

  
Here is an example of a window function call:

  

	SELECT deptno, purchase_date, item, cost,
		SUM(cost) OVER (
		    PARTITION BY deptno
		    ORDER BY purchase_date
		    ROWS UNBOUNDED PRECEDING) AS running_total_cost
	FROM purchases
	ORDER BY deptno, purchase_date

  

This example partitions the `purchases` dataset by department number. Within each department, it orders the `purchases` by date and computes a running total cost for each item, using the frame specification `ROWS UNBOUNDED PRECEDING`. Note that the `ORDER BY` clause in the window function is separate and independent from the `ORDER BY` clause of the query as a whole.

  

The general syntax of a window function call is specified in this section. SQL++ has a set of builtin window functions, which are listed and explained in their respective [section](builtins.html#WindowFunctions) of the builtin functions page. In addition,  standard SQL aggregate functions such as `SUM` and `AVG` can be used as window functions if they are used with an `OVER` clause.
The query language has a dedicated set of window functions.
Aggregate functions can also be used as window functions, when they are used with an `OVER` clause.

## <a id="Window_function_call">Window Function Call</a> ##

---
### WindowFunctionCall
**![](../images/diagrams/WindowFunctionCall.png)**

### WindowFunctionType
**![](../images/diagrams/WindowFunctionType.png)**

----

Refer to the [Aggregate Functions](builtins.html#AggregateFunctions) section
for a list of aggregate functions.

Refer to the [Window Functions](builtins.html#WindowFunctions) section for a
list of window functions.

### <a id="Window_function_arguments">Window Function Arguments</a> ###

---

### WindowFunctionArguments
**![](../images/diagrams/WindowFunctionArguments.png)**


---

Refer to the [Aggregate Functions](builtins.html#AggregateFunctions) section or the [Window Functions](builtins.html#WindowFunctions) section for details of the arguments for individual functions.

### <a id="Window_function_options">Window Function Options</a> ###

---

### WindowFunctionOptions
**![](../images/diagrams/WindowFunctionOptions.png)**


---

Window function options cannot be used with [aggregate functions](builtins.html#AggregateFunctions).

Window function options can only be used with some [window functions](builtins.html#WindowFunctions), as described below.

The *FROM modifier* determines whether the computation begins at the first or last tuple in the window. It is optional and can only be used with the `nth_value()` function. If it is omitted, the default setting is `FROM FIRST`.

The *NULLS modifier*  determines whether NULL values are included in the computation, or ignored. MISSING values are treated the same way as NULL values. It is also optional and can only be used with the `first_value()`, `last_value()`, `nth_value()`, `lag()`, and `lead()` functions. If omitted, the default setting is `RESPECT NULLS`.

### <a id="Window_frame_variable">Window Frame Variable</a> ###

The `AS` keyword enables you to specify an alias for the window frame contents. It introduces a variable which will be bound to the contents of the frame. When using a built-in [aggregate function](builtins.html#AggregateFunctions) as a window function, the function’s argument must be a subquery which refers to this alias, for example:

    SELECT ARRAY_COUNT(DISTINCT (FROM alias SELECT VALUE alias.src.field))
    OVER alias AS (PARTITION BY … ORDER BY …)
    FROM source AS src

The alias is not necessary when using a [window function](builtins.html#WindowFunctions), or when using a standard SQL aggregate function with the `OVER` clause.


### <a id="Window_definition">Window Definition</a> ###
---

### WindowDefinition
**![](../images/diagrams/WindowDefinition.png)**


---

The *window definition* specifies the partitioning, ordering, and framing for window functions.

#### <a id="Window_partition_clause">Window Partition Clause</a> ####

---

### WindowPartitionClause
**![](../images/diagrams/WindowPartitionClause.png)**


---

The *window partition clause* divides the tuples into logical partitions
using one or more expressions.

This clause may be used with any [window function](builtins.html#WindowFunctions),
or any [aggregate function](builtins.html#AggregateFunctions) used as a window
function.

This clause is optional.
If omitted, all tuples are united in a single partition.

#### <a id="Window_order_clause">Window Order Clause</a> ####
---

### WindowOrderClause
**![](../images/diagrams/WindowOrderClause.png)**


---

The *window order clause* determines how tuples are ordered within each partition. The window function works on tuples in the order specified by this clause.

This clause may be used with any [window function](builtins.html#WindowFunctions), or any [aggregate function](builtins.html#AggregateFunctions) used as a window function.

This clause is optional. If omitted, all tuples are considered peers, i.e. their order is tied. When tuples in the window partition are tied, each window function behaves differently.

* The `row_number()` function returns a distinct number for each tuple.
  If tuples are tied, the results may be unpredictable.

* The `rank()`, `dense_rank()`, `percent_rank()`, and `cume_dist()` functions
  return the same result for each tuple.

* For other functions, if the [window frame](#Window_frame_clause) is
  defined by `ROWS`, the results may be unpredictable.
  If the window frame is defined by `RANGE` or `GROUPS`, the results are same
  for each tuple.

##### Note #####

This clause does not guarantee the overall order of the query results. To guarantee the order of the final results, use the query `ORDER BY` clause.


#### <a id="Window_frame_clause">Window Frame Clause</a> ####

### WindowFrameClause
**![](../images/diagrams/WindowFrameClause.png)**


The *window frame clause* defines the window frame. It can be used with all [aggregate functions](builtins.html#AggregateFunctions) and some [window functions](builtins.html#WindowFunctions) - refer to the descriptions of individual functions for more details.  It is optional and allowed only when the [window order clause](#Window_order_clause) is present.

* If this clause is omitted and there is no [window order clause](#Window_order_clause), the window frame is the entire partition.

* If this clause is omitted but there is a [window order clause](#Window_order_clause), the window frame becomes all tuples
  in the partition preceding the current tuple and its peers - the same as `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

The window frame can be defined in the following ways:

* `ROWS`: Counts the exact number of tuples within the frame. If window ordering doesn’t result in unique ordering, the function may produce unpredictable results. You can add a unique expression or more window ordering expressions to produce unique ordering.

* `RANGE`: Looks for a value offset within the frame. The function produces deterministic results.

* `GROUPS`: Counts all groups of tied rows within the frame. The function produces deterministic results.

##### Note #####

If this clause uses `RANGE` with either *Expr* `PRECEDING` or *Expr* ` FOLLOWING`, the [window order clause](#Window_order_clause) must have only a single ordering term.

The ordering term expression must evaluate to a number.

If these conditions are not met, the window frame will be empty, which means the window function will return its default value: in most cases this is `null`, except for `strict_count()` or `array_count()`, whose default value is 0. This restriction does not apply when the window frame uses `ROWS` or `GROUPS`.

##### Tip #####

The `RANGE` window frame is commonly used to define window frames based
on date or time.

If you want to use `RANGE` with either *Expr* `PRECEDING` or *Expr* `FOLLOWING`, and you want to use an ordering expression based on date or time, the expression in *Expr* `PRECEDING` or *Expr* `FOLLOWING` must use a data type that can be added to the ordering expression.

#### <a id="Window_frame_extent">Window Frame Extent</a> ####
---

### WindowFrameExtent
**![](../images/diagrams/WindowFrameExtent.png)**


---

The *window frame extent clause* specifies the start point and end point of the window frame.
The expression before `AND` is the start point and the expression after `AND` is the end point.
If `BETWEEN` is omitted, you can only specify the start point; the end point becomes `CURRENT ROW`.

The window frame end point can’t be before the start point. If this clause violates this restriction explicitly, an error will result. If it violates this restriction implicitly, the window frame will be empty, which means the window function will return its default value: in most cases this is `null`, except for `strict_count()` or
`array_count()`, whose default value is 0.

Window frame extents that result in an explicit violation are:

* `BETWEEN CURRENT ROW AND` *Expr* `PRECEDING`

* `BETWEEN` *Expr* `FOLLOWING AND` *Expr* `PRECEDING`

* `BETWEEN` *Expr* `FOLLOWING AND CURRENT ROW`

Window frame extents that result in an implicit violation are:

* `BETWEEN UNBOUNDED PRECEDING AND` *Expr* `PRECEDING` - if *Expr* is too high, some tuples may generate an empty window frame.

* `BETWEEN` *Expr* `PRECEDING AND` *Expr* `PRECEDING` - if the second  *Expr* is greater than or equal to the first *Expr*, all result sets will generate an empty window frame.

* `BETWEEN` *Expr* `FOLLOWING AND` *Expr* `FOLLOWING` - if the first *Expr* is greater than or equal to the second *Expr*, all result sets will generate an empty window frame.

* `BETWEEN` *Expr* `FOLLOWING AND UNBOUNDED FOLLOWING` - if *Expr* is too high, some tuples may generate an empty window frame.

* If the [window frame exclusion clause](#Window_frame_exclusion) is present, any window frame specification may result in empty window frame.

The *Expr* must be a positive constant or an expression that evaluates as a positive number. For `ROWS` or `GROUPS`, the *Expr* must be an integer.

#### <a id="Window_frame_exclusion">Window Frame Exclusion</a> ####

---

### WindowFrameExclusion
**![](../images/diagrams/WindowFrameExclusion.png)**


---

The *window frame exclusion clause* enables you to exclude specified tuples from the window frame.

This clause can be used with all [aggregate functions](builtins.html#AggregateFunctions) and some [window functions](builtins.html#WindowFunctions) - refer to the descriptions of individual functions for more details.

This clause is allowed only when the [window frame clause](#Window_frame_clause) is present.

This clause is optional. If this clause is omitted, the default is no exclusion - the same as `EXCLUDE NO OTHERS`.

* `EXCLUDE CURRENT ROW`: If the current tuple is still part of the window frame, it is removed from the window frame.

* `EXCLUDE GROUP`: The current tuple and any peers of the current tuple are removed from the window frame.

* `EXCLUDE TIES`: Any peers of the current tuple, but not the current tuple itself, are removed from the window frame.

* `EXCLUDE NO OTHERS`: No additional tuples are removed from the window frame.

If the current tuple is already removed from the window frame, then it remains removed from the window frame.
