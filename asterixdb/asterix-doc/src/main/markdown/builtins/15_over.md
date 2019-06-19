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

## <a id="OverClause">OVER Clause (Window Function Calls)</a> ##

All window functions must have an OVER clause to define the window partitions,
the order of tuples within those partitions, and the extent of the window frame.
Some window functions take additional window options, which are specified by
modifiers before the OVER clause.

The query language has a dedicated set of window functions.
Aggregate functions can also be used as window functions, when they are used
with an OVER clause.

### <a id="window-function-call">Window Function Call</a> ###

    WindowFunctionCall ::= WindowFunctionType "(" WindowFunctionArguments ")"
    (WindowFunctionOptions)? <OVER> (Variable <AS>)? "(" WindowClause ")"

### <a id="window-function-type">Window Function Type</a> ###

    WindowFunctionType ::= AggregateFunctions | WindowFunctions

Refer to the [Aggregate Functions](#AggregateFunctions) section for a list of
aggregate functions.

Refer to the [Window Functions](#WindowFunctions) section for a list of window
functions.

### <a id="window-function-arguments">Window Function Arguments</a> ###

    WindowFunctionArguments ::= ( (<DISTINCT>)? Expression |
    (Expression ("," Expression ("," Expression)? )? )? )

Refer to the [Aggregate Functions](#AggregateFunctions) section or the
[Window Functions](#WindowFunctions) section for details of the arguments for
individual functions.

### <a id="window-function-options">Window Function Options</a> ###

    WindowFunctionOptions ::= (NthValFrom)? (NullsTreatment)?

Window function options cannot be used with [aggregate
functions](#AggregateFunctions).

Window function options can only be used with some [window
functions](#WindowFunctions), as described below.

#### <a id="nthval-from">Nth Val From</a> ####

    NthValFrom ::= <FROM> ( <FIRST> | <LAST> )

The **nth val from** modifier determines whether the computation begins at the
first or last tuple in the window.

This modifier can only be used with the `nth_value()` function.

This modifier is optional.
If omitted, the default setting is `FROM FIRST`.

#### <a id="nulls-treatment">Nulls Treatment</a> ####

    nulls-treatment ::= ( <RESPECT> | <IGNORE> ) <NULLS>

The **nulls treatment** modifier determines whether NULL values are included in
the computation, or ignored.
MISSING values are treated the same way as NULL values.

This modifier can only be used with the `first_value()`, `last_value()`,
`nth_value()`, `lag()`, and `lead()` functions.

This modifier is optional.
If omitted, the default setting is `RESPECT NULLS`.

### <a id="window-frame-variable">Window Frame Variable</a> ###

The AS keyword enables you to specify an alias for the window frame contents.
It introduces a variable which will be bound to the contents of the frame.
When using a built-in [aggregate function](#AggregateFunctions) as a
window function, the function’s argument must be a subquery which refers to
this alias, for example:

    FROM source AS src
    SELECT ARRAY_COUNT(DISTINCT (FROM alias SELECT VALUE alias.src.field))
    OVER alias AS (PARTITION BY … ORDER BY …)

The alias is not necessary when using a [window function](#WindowFunctions),
or when using a standard SQL aggregate function with the OVER clause.

#### Standard SQL Aggregate Functions with the Window Clause ####

A standard SQL aggregate function with an OVER clause is rewritten by the
query compiler using a built-in aggregate function over a frame variable.
For example, the following query with the `sum()` function:

    FROM source AS src
    SELECT SUM(field)
    OVER (PARTITION BY … ORDER BY …)

Is rewritten as the following query using the `array_sum()` function:

    FROM source AS src
    SELECT ARRAY_SUM( (FROM alias SELECT VALUE alias.src.field) )
    OVER alias AS (PARTITION BY … ORDER BY …)

This is similar to the way that standard SQL aggregate functions are rewritten
as built-in aggregate functions in the presence of the GROUP BY clause.

### <a id="window-definition">Window Definition</a> ###

    WindowDefinition ::= (WindowPartitionClause)? (WindowOrderClause
    (WindowFrameClause (WindowFrameExclusion)? )? )?

The **window definition** specifies the partitioning, ordering, and framing for
window functions.

#### <a id="window-partition-clause">Window Partition Clause</a> ####

    WindowPartitionClause ::= <PARTITION> <BY> Expression ("," Expression)*

The **window partition clause** divides the tuples into partitions using
one or more expressions.

This clause may be used with any [window function](#WindowFunctions), or any
[aggregate function](#AggregateFunctions) used as a window function.

This clause is optional.
If omitted, all tuples are united in a single partition.

#### <a id="window-order-clause">Window Order Clause</a> ####

    WindowOrderClause ::= <ORDER> <BY> OrderingTerm ("," OrderingTerm)*

The **window order clause** determines how tuples are ordered within each
partition.
The window function works on tuples in the order specified by this clause.

This clause may be used with any [window function](#WindowFunctions), or any
[aggregate function](#AggregateFunctions) used as a window function.

This clause is optional for some functions, and required for others.
Refer to the [Aggregate Functions](#AggregateFunctions) section or the
[Window Functions](#WindowFunctions) section for details of the syntax of
individual functions.

If this clause is omitted, all tuples are considered peers, i.e. their order
is tied.
When tuples in the window partition are tied, each window function behaves
differently.

* The `row_number()` function returns a distinct number for each tuple.
  If tuples are tied, the results may be unpredictable.

* The `rank()`, `dense_rank()`, `percent_rank()`, and `cume_dist()` functions
  return the same result for each tuple.

* For other functions, if the [window frame](#window-frame-clause) is
  defined by `ROWS`, the results may be unpredictable.
  If the window frame is defined by `RANGE` or `GROUPS`, the results are same
  for each tuple.

This clause may have multiple [ordering terms](#ordering-term).
To reduce the number of ties, add additional [ordering terms](#ordering-term).

##### NOTE #####

This clause does not guarantee the overall order of the query results.
To guarantee the order of the final results, use the query ORDER BY clause.

#### <a id="ordering-term">Ordering Term</a> ####

    OrderingTerm ::= Expression ( <ASC> | <DESC> )?

The **ordering term** specifies an ordering expression and collation.

This clause has the same syntax and semantics as the ordering term for queries.
Refer to the [ORDER BY clause](manual.html#Order_By_clauses) section
for details.

#### <a id="window-frame-clause">Window Frame Clause</a> ####

    WindowFrameClause ::= ( <ROWS> | <RANGE> | <GROUPS> ) WindowFrameExtent

The **window frame clause** defines the window frame.

This clause can be used with all [aggregate functions](#AggregateFunctions) and
some [window functions](#WindowFunctions) — refer to the descriptions of
individual functions for more details.

This clause is allowed only when the [window order
clause](#window-order-clause) is present.

This clause is optional.

* If this clause is omitted and there is no [window order
  clause](#window-order-clause), the window frame is the entire partition.

* If this clause is omitted but there is a [window order
  clause](#window-order-clause), the window frame becomes all tuples
  in the partition preceding the current tuple and its peers — the
  same as `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

The window frame can be defined in the following ways:

* `ROWS`: Counts the exact number of tuples within the frame.
  If window ordering doesn’t result in unique ordering, the function may
  produce unpredictable results.
  You can add a unique expression or more window ordering expressions to
  produce unique ordering.

* `RANGE`: Looks for a value offset within the frame.
  The function produces deterministic results.

* `GROUPS`: Counts all groups of tied rows within the frame.
  The function produces deterministic results.

##### NOTE #####

If this clause uses `RANGE` with either `Expression PRECEDING` or
`Expression FOLLOWING`, the [window order clause](#window-order-clause) must
have only a single ordering term.
The ordering term expression must evaluate to a number, a date, a time, or a
datetime.
If the ordering term expression evaluates to a date, a time, or a datetime, the
expression in `Expression PRECEDING` or `Expression FOLLOWING` must evaluate to
a duration.

If these conditions are not met, the window frame will be empty,
which means the window function will return its default
value: in most cases this is NULL, except for `strict_count()` or
`array_count()`, whose default value is 0.

This restriction does not apply when the window frame uses `ROWS` or
`GROUPS`.

##### TIP #####

The `RANGE` window frame is commonly used to define window frames based
on date or time.

If you want to use `RANGE` with either `Expression PRECEDING` or `Expression
FOLLOWING`, and you want to use an ordering expression based on date or time,
the expression in `Expression PRECEDING` or `Expression FOLLOWING` must use a
data type that can be added to the ordering expression.

#### <a id="window-frame-extent">Window Frame Extent</a> ####

    WindowFrameExtent ::= ( <UNBOUNDED> <PRECEDING> | <CURRENT> <ROW> |
    Expression <FOLLOWING> ) | <BETWEEN> ( <UNBOUNDED> <PRECEDING> | <CURRENT>
    <ROW> | Expression ( <PRECEDING> | <FOLLOWING> ) ) <AND> ( <UNBOUNDED>
    <FOLLOWING> | <CURRENT> <ROW> | Expression ( <PRECEDING> | <FOLLOWING> ) )

The **window frame extent clause** specifies the start point and end point of
the window frame.
The expression before `AND` is the start point and the expression after `AND`
is the end point.
If `BETWEEN` is omitted, you can only specify the start point; the end point
becomes `CURRENT ROW`.

The window frame end point can’t be before the start point.
If this clause violates this restriction explicitly, an error will result.
If it violates this restriction implicitly, the window frame will be empty,
which means the window function will return its default value:
in most cases this is NULL, except for `strict_count()` or
`array_count()`, whose default value is 0.

Window frame extents that result in an explicit violation are:

* `BETWEEN CURRENT ROW AND Expression PRECEDING`

* `BETWEEN Expression FOLLOWING AND Expression PRECEDING`

* `BETWEEN Expression FOLLOWING AND CURRENT ROW`

Window frame extents that result in an implicit violation are:

* `BETWEEN UNBOUNDED PRECEDING AND Expression PRECEDING` — if `Expression` is
  too high, some tuples may generate an empty window frame.

* `BETWEEN Expression PRECEDING AND Expression PRECEDING` — if the second
  `Expression` is greater than or equal to the first `Expression`,
  all result sets will generate an empty window frame.

* `BETWEEN Expression FOLLOWING AND Expression FOLLOWING` — if the first
  `Expression` is greater than or equal to the second `Expression`, all result
  sets will generate an empty window frame.

* `BETWEEN Expression FOLLOWING AND UNBOUNDED FOLLOWING` — if `Expression` is
  too high, some tuples may generate an empty window frame.

* If the [window frame exclusion clause](#window-frame-exclusion) is present,
  any window frame specification may result in empty window frame.

The `Expression` must be a positive constant or an expression that evaluates as
a positive number.
For `ROWS` or `GROUPS`, the `Expression` must be an integer.

#### <a id="window-frame-exclusion">Window Frame Exclusion</a> ####

    WindowFrameExclusion ::= <EXCLUDE> ( <CURRENT> <ROW> | <GROUP> | <TIES> |
    <NO> <OTHERS> )

The **window frame exclusion clause** enables you to exclude specified
tuples from the window frame.

This clause can be used with all [aggregate functions](#AggregateFunctions) and
some [window functions](#WindowFunctions) — refer to the descriptions of
individual functions for more details.

This clause is allowed only when the [window frame
clause](#window-frame-clause) is present.

This clause is optional.
If this clause is omitted, the default is no exclusion —
the same as `EXCLUDE NO OTHERS`.

* `EXCLUDE CURRENT ROW`: If the current tuple is still part of the window
  frame, it is removed from the window frame.

* `EXCLUDE GROUP`: The current tuple and any peers of the current tuple are
  removed from the window frame.

* `EXCLUDE TIES`: Any peers of the current tuple, but not the current tuple
  itself, are removed from the window frame.

* `EXCLUDE NO OTHERS`: No additional tuples are removed from the window frame.

If the current tuple is already removed from the window frame, then it remains
removed from the window frame.
