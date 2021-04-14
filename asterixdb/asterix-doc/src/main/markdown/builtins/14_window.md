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

## <a id="WindowFunctions">Window Functions</a> ##

Window functions are used to compute an aggregate or cumulative value, based on
a portion of the tuples selected by a query.
For each input tuple, a movable window of tuples is defined.
The window determines the tuples to be used by the window function.

The tuples are not grouped into a single output tuple — each tuple remains
separate in the query output.

All window functions must be used with an OVER clause.
Refer to [Window Queries](manual.html#Over_clauses) for details.

Window functions cannot appear in the FROM clause clause or LIMIT clause.

### cume_dist ###

* Syntax:

        CUME_DIST() OVER ([window-partition-clause] [window-order-clause])

* Returns the percentile rank of the current tuple as part of the cumulative
  distribution – that is, the number of tuples ranked lower than or equal to
  the current tuple, including the current tuple, divided by the total number
  of tuples in the window partition.

    The window order clause determines the sort order of the tuples.
    If the window order clause is omitted, the function returns the same
    result (1.0) for each tuple.

* Arguments:

    * None.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * A number greater than 0 and less than or equal to 1.
      The higher the value, the higher the ranking.

* Example:

    For each customer, find the cumulative distribution of all orders
    by order number.

        FROM orders AS o
        SELECT o.custid, o.orderno, CUME_DIST() OVER (
          PARTITION BY o.custid
          ORDER BY o.orderno
        ) AS `rank`
        ORDER BY o.custid, o.orderno;

* The expected result is:

        [
          {
            "rank": 0.25,
            "custid": "C13",
            "orderno": 1002
          },
          {
            "rank": 0.5,
            "custid": "C13",
            "orderno": 1007
          },
          {
            "rank": 0.75,
            "custid": "C13",
            "orderno": 1008
          },
          {
            "rank": 1,
            "custid": "C13",
            "orderno": 1009
          },
          {
            "rank": 1,
            "custid": "C31",
            "orderno": 1003
          },
          {
            "rank": 1,
            "custid": "C35",
            "orderno": 1004
          },
          {
            "rank": 1,
            "custid": "C37",
            "orderno": 1005
          },
          {
            "rank": 0.5,
            "custid": "C41",
            "orderno": 1001
          },
          {
            "rank": 1,
            "custid": "C41",
            "orderno": 1006
          }
        ]

### dense_rank ###

* Syntax:

        DENSE_RANK() OVER ([window-partition-clause] [window-order-clause])

* Returns the dense rank of the current tuple – that is, the number of
  distinct tuples preceding this tuple in the current window partition, plus
  one.

    The tuples are ordered by the window order clause.
    If any tuples are tied, they will have the same rank.
    If the window order clause is omitted, the function returns the same
    result (1) for each tuple.

    For this function, when any tuples have the same rank, the rank of the next
    tuple will be consecutive, so there will not be a gap in the sequence of
    returned values.
    For example, if there are five tuples ranked 3, the next dense rank is 4.

* Arguments:

    * None.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * An integer, greater than or equal to 1.

* Example:

    Find the dense rank of all orders by number of items.

        FROM orders AS o
        SELECT o.orderno, LEN(o.items) AS items,
        DENSE_RANK() OVER (
          ORDER BY LEN(o.items)
        ) AS `rank`
        ORDER BY `rank`, o.orderno;

* The expected result is:

        [
          {
            "items": 0,
            "rank": 1,
            "orderno": 1009
          },
          {
            "items": 1,
            "rank": 2,
            "orderno": 1008
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1001
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1002
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1003
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1004
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1007
          },
          {
            "items": 3,
            "rank": 4,
            "orderno": 1006
          },
          {
            "items": 4,
            "rank": 5,
            "orderno": 1005
          }
        ]

### first_value ###

* Syntax:

        FIRST_VALUE(expr) [nulls-modifier] OVER (window-definition)

* Returns the requested value from the first tuple in the current window
  frame, where the window frame is specified by the window definition.

* Arguments:

    * `expr`: The value that you want to return from the first
      tuple in the window frame. <sup>\[[1](#fn_1)\]</sup>

* Modifiers:

    * [NULLS Modifier](manual.html#Window_function_options): (Optional) Determines how
      NULL or MISSING values are treated when finding the first value in the
      window frame.

        - `IGNORE NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are ignored when finding the first tuple.
          In this case, the function returns the first non-NULL, non-MISSING
          value.

        - `RESPECT NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are included when finding the first tuple.

        If this modifier is omitted, the default is `RESPECT NULLS`.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

    * (Optional) [Window Frame Clause](manual.html#Window_frame_clause).

* Return Value:

    * The specified value from the first tuple.
      The order of the tuples is determined by the window order clause.

    * NULL, if the frame was empty or if all values were NULL or MISSING and
      the `IGNORE NULLS` modifier was specified.

    * In the following cases, this function may return unpredictable results.

        - If the window order clause is omitted.

        - If the window frame is defined by `ROWS`, and there are tied tuples
          in the window frame.

    * To make the function return deterministic results, add a window order
      clause, or add further ordering terms to the window order clause so that
      no tuples are tied.

    * If the window frame is defined by `RANGE` or `GROUPS`, and there are
      tied tuples in the window frame, the function returns the first value
      of the input expression.

* Example:

    For each order, show the customer and the value, including the
    value of the smallest order from that customer.

        FROM orders AS o
        LET revenue = ROUND((
          FROM o.items
          SELECT VALUE SUM(qty * price)
        )[0], 2)
        SELECT o.custid, o.orderno, revenue,
        FIRST_VALUE(revenue) OVER (
          PARTITION BY o.custid
          ORDER BY revenue
        ) AS smallest_order;

* The expected result is:

        [
          {
            "custid": "C13",
            "orderno": 1009,
            "revenue": null,
            "smallest_order": null
          },
          {
            "custid": "C13",
            "orderno": 1007,
            "revenue": 130.45,
            "smallest_order": null
          },
          {
            "custid": "C13",
            "orderno": 1008,
            "revenue": 1999.8,
            "smallest_order": null
          },
          {
            "custid": "C13",
            "orderno": 1002,
            "revenue": 10906.55,
            "smallest_order": null
          },
          {
            "custid": "C31",
            "orderno": 1003,
            "revenue": 477.95,
            "smallest_order": 477.95
          },
          {
            "custid": "C35",
            "orderno": 1004,
            "revenue": 199.94,
            "smallest_order": 199.94
          },
          {
            "custid": "C37",
            "orderno": 1005,
            "revenue": 4639.92,
            "smallest_order": 4639.92
          },
          {
            "custid": "C41",
            "orderno": 1001,
            "revenue": 157.73,
            "smallest_order": 157.73
          },
          {
            "custid": "C41",
            "orderno": 1006,
            "revenue": 18847.58,
            "smallest_order": 157.73
          }
        ]

### lag ###

* Syntax:

        LAG(expr[, offset[, default]]) [nulls-modifier] OVER ([window-partition-clause] [window-order-clause])

* Returns the value from a tuple at a given offset prior to the current tuple
  position.

    The window order clause determines the sort order of the tuples.
    If the window order clause is omitted, the return values may be
    unpredictable.

* Arguments:

    * `expr`: The value that you want to return from the offset
      tuple. <sup>\[[1](#fn_1)\]</sup>

    * `offset`: (Optional) A positive integer.
      If omitted, the default is 1.

    * `default`: (Optional) The value to return when the offset goes out of
      partition scope.
      If omitted, the default is NULL.

* Modifiers:

    * [NULLS Modifier](manual.html#Window_function_options): (Optional) Determines how
      NULL or MISSING values are treated when finding the offset tuple in the
      window partition.

        - `IGNORE NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are ignored when finding the offset tuple.

        - `RESPECT NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are included when finding the offset tuple.

        If this modifier is omitted, the default is `RESPECT NULLS`.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * The specified value from the offset tuple.

    * If the offset tuple is out of partition scope, it returns the default value,
      or NULL if no default is specified.

* Example:

    For each order, show the customer and the value, including the
    value of the next-smallest order from that customer.

        FROM orders AS o
        LET revenue = ROUND((
          FROM o.items
          SELECT VALUE SUM(qty * price)
        )[0], 2)
        SELECT o.custid, o.orderno, revenue,
        LAG(revenue, 1, "No smaller order") OVER (
          PARTITION BY o.custid
          ORDER BY revenue
        ) AS next_smallest_order;

* The expected result is:

        [
          {
            "custid": "C13",
            "orderno": 1009,
            "revenue": null,
            "next_smallest_order": "No smaller order"
          },
          {
            "custid": "C13",
            "orderno": 1007,
            "revenue": 130.45,
            "next_smallest_order": null
          },
          {
            "custid": "C13",
            "orderno": 1008,
            "revenue": 1999.8,
            "next_smallest_order": 130.45
          },
          {
            "custid": "C13",
            "orderno": 1002,
            "revenue": 10906.55,
            "next_smallest_order": 1999.8
          },
          {
            "custid": "C31",
            "orderno": 1003,
            "revenue": 477.95,
            "next_smallest_order": "No smaller order"
          },
          {
            "custid": "C35",
            "orderno": 1004,
            "revenue": 199.94,
            "next_smallest_order": "No smaller order"
          },
          {
            "custid": "C37",
            "orderno": 1005,
            "revenue": 4639.92,
            "next_smallest_order": "No smaller order"
          },
          {
            "custid": "C41",
            "orderno": 1001,
            "revenue": 157.73,
            "next_smallest_order": "No smaller order"
          },
          {
            "custid": "C41",
            "orderno": 1006,
            "revenue": 18847.58,
            "next_smallest_order": 157.73
          }
        ]

### last_value ###

* Syntax:

        LAST_VALUE(expr) [nulls-modifier] OVER (window-definition)

* Returns the requested value from the last tuple in the current window frame,
  where the window frame is specified by the window definition.

* Arguments:

    * `expr`: The value that you want to return from the last tuple
      in the window frame. <sup>\[[1](#fn_1)\]</sup>

* Modifiers:

    * [NULLS Modifier](manual.html#Window_function_options): (Optional) Determines how
      NULL or MISSING values are treated when finding the last tuple in the
      window frame.

        - `IGNORE NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are ignored when finding the last tuple.
          In this case, the function returns the last non-NULL, non-MISSING
          value.

        - `RESPECT NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are included when finding the last tuple.

        If this modifier is omitted, the default is `RESPECT NULLS`.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

    * (Optional) [Window Frame Clause](manual.html#Window_frame_clause).

* Return Value:

    * The specified value from the last tuple.
      The order of the tuples is determined by the window order clause.

    * NULL, if the frame was empty or if all values were NULL or MISSING and
      the `IGNORE NULLS` modifier was specified.

    * In the following cases, this function may return unpredictable results.

        - If the window order clause is omitted.

        - If the window frame clause is omitted.

        - If the window frame is defined by `ROWS`, and there are tied tuples
          in the window frame.

    * To make the function return deterministic results, add a window order
      clause, or add further ordering terms to the window order clause so that
      no tuples are tied.

    * If the window frame is defined by `RANGE` or `GROUPS`, and there are
      tied tuples in the window frame, the function returns the last
      value of the input expression.

* Example:

    For each order, show the customer and the value, including the
    value of the largest order from that customer.

        FROM orders AS o
        LET revenue = ROUND((
          FROM o.items
          SELECT VALUE SUM(qty * price)
        )[0], 2)
        SELECT o.custid, o.orderno, revenue,
        LAST_VALUE(revenue) OVER (
          PARTITION BY o.custid
          ORDER BY revenue
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING -- ➊
        ) AS largest_order;

* The expected result is:

        [
          {
            "custid": "C13",
            "orderno": 1009,
            "revenue": null,
            "largest_order": 10906.55
          },
          {
            "custid": "C13",
            "orderno": 1007,
            "revenue": 130.45,
            "largest_order": 10906.55
          },
          {
            "custid": "C13",
            "orderno": 1008,
            "revenue": 1999.8,
            "largest_order": 10906.55
          },
          {
            "custid": "C13",
            "orderno": 1002,
            "revenue": 10906.55,
            "largest_order": 10906.55
          },
          {
            "custid": "C31",
            "orderno": 1003,
            "revenue": 477.95,
            "largest_order": 477.95
          },
          {
            "custid": "C35",
            "orderno": 1004,
            "revenue": 199.94,
            "largest_order": 199.94
          },
          {
            "custid": "C37",
            "orderno": 1005,
            "revenue": 4639.92,
            "largest_order": 4639.92
          },
          {
            "custid": "C41",
            "orderno": 1001,
            "revenue": 157.73,
            "largest_order": 18847.58
          },
          {
            "custid": "C41",
            "orderno": 1006,
            "revenue": 18847.58,
            "largest_order": 18847.58
          }
        ]

    ➀ This clause specifies that the window frame should extend to the
    end of the window partition.
    Without this clause, the end point of the window frame would always be the
    current tuple.
    This would mean that the largest order would always be the same as the
    current order.

### lead ###

* Syntax:

        LEAD(expr[, offset[, default]]) [nulls-modifier] OVER ([window-partition-clause] [window-order-clause])

* Returns the value from a tuple at a given offset ahead of the current tuple
  position.

    The window order clause determines the sort order of the tuples.
    If the window order clause is omitted, the return values may be
    unpredictable.

* Arguments:

    * `expr`: The value that you want to return from the offset
      tuple. <sup>\[[1](#fn_1)\]</sup>

    * `offset`: (Optional) A positive integer. If omitted, the
      default is 1.

    * `default`: (Optional) The value to return when the offset goes out of
      window partition scope.
      If omitted, the default is NULL.

* Modifiers:

    * [NULLS Modifier](manual.html#Window_function_options): (Optional) Determines how
      NULL or MISSING values are treated when finding the offset tuple in the
      window partition.

        - `IGNORE NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are ignored when finding the offset tuple.

        - `RESPECT NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are included when finding the offset tuple.

        If this modifier is omitted, the default is `RESPECT NULLS`.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * The specified value from the offset tuple.

    * If the offset tuple is out of partition scope, it returns the default value, or
      NULL if no default is specified.

* Example:

    For each order, show the customer and the value, including the
    value of the next-largest order from that customer.

        FROM orders AS o
        LET revenue = ROUND((
          FROM o.items
          SELECT VALUE SUM(qty * price)
        )[0], 2)
        SELECT o.custid, o.orderno, revenue,
        LEAD(revenue, 1, "No larger order") OVER (
          PARTITION BY o.custid
          ORDER BY revenue
        ) AS next_largest_order;

* The expected result is:

        [
          {
            "custid": "C13",
            "orderno": 1009,
            "revenue": null,
            "next_largest_order": 130.45
          },
          {
            "custid": "C13",
            "orderno": 1007,
            "revenue": 130.45,
            "next_largest_order": 1999.8
          },
          {
            "custid": "C13",
            "orderno": 1008,
            "revenue": 1999.8,
            "next_largest_order": 10906.55
          },
          {
            "custid": "C13",
            "orderno": 1002,
            "revenue": 10906.55,
            "next_largest_order": "No larger order"
          },
          {
            "custid": "C31",
            "orderno": 1003,
            "revenue": 477.95,
            "next_largest_order": "No larger order"
          },
          {
            "custid": "C35",
            "orderno": 1004,
            "revenue": 199.94,
            "next_largest_order": "No larger order"
          },
          {
            "custid": "C37",
            "orderno": 1005,
            "revenue": 4639.92,
            "next_largest_order": "No larger order"
          },
          {
            "custid": "C41",
            "orderno": 1001,
            "revenue": 157.73,
            "next_largest_order": 18847.58
          },
          {
            "custid": "C41",
            "orderno": 1006,
            "revenue": 18847.58,
            "next_largest_order": "No larger order"
          }
        ]

### nth_value ###

* Syntax:

        NTH_VALUE(expr, offset) [from-modifier] [nulls-modifier] OVER (window-definition)

* Returns the requested value from a tuple in the current window frame, where
  the window frame is specified by the window definition.

* Arguments:

    * `expr`: The value that you want to return from the offset
      tuple in the window frame. <sup>\[[1](#fn_1)\]</sup>

    * `offset`: The number of the offset tuple within the window
      frame, counting from 1.

* Modifiers:

    * [FROM Modifier](manual.html#Window_function_options): (Optional) Determines where the
      function starts counting the offset.

        - `FROM FIRST`: Counting starts at the first tuple in the window frame.
          In this case, an offset of 1 is the first tuple in the window frame,
          2 is the second tuple, and so on.

        - `FROM LAST`: Counting starts at the last tuple in the window frame.
          In this case, an offset of 1 is the last tuple in the window frame,
          2 is the second-to-last tuple, and so on.

        The order of the tuples is determined by the window order clause.
        If this modifier is omitted, the default is `FROM FIRST`.

    * [NULLS Modifier](manual.html#Window_function_options): (Optional) Determines how
      NULL or MISSING values are treated when finding the offset tuple in the
      window frame.

        - `IGNORE NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are ignored when finding the offset tuple.

        - `RESPECT NULLS`: If the values for any tuples evaluate to NULL or
          MISSING, those tuples are included when finding the offset tuple.

        If this modifier is omitted, the default is `RESPECT NULLS`.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

    * (Optional) [Window Frame Clause](manual.html#Window_frame_clause).

* Return Value:

    * The specified value from the offset tuple.

    * In the following cases, this function may return unpredictable results.

        - If the window order clause is omitted.

        - If the window frame is defined by `ROWS`, and there are tied tuples
          in the window frame.

    * To make the function return deterministic results, add a window order
      clause, or add further ordering terms to the window order clause so that
      no tuples are tied.

    * If the window frame is defined by `RANGE` or `GROUPS`, and there are
      tied tuples in the window frame, the function returns the first value
      of the input expression when counting `FROM FIRST`, or the last
      value of the input expression when counting `FROM LAST`.

* Example 1:

    For each order, show the customer and the value, including the
    value of the second smallest order from that customer.

        FROM orders AS o
        LET revenue = ROUND((
          FROM o.items
          SELECT VALUE SUM(qty * price)
        )[0], 2)
        SELECT o.custid, o.orderno, revenue,
        NTH_VALUE(revenue, 2) FROM FIRST OVER (
          PARTITION BY o.custid
          ORDER BY revenue
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING -- ➊
        ) AS smallest_order_but_1;

* The expected result is:

        [
          {
            "custid": "C13",
            "orderno": 1009,
            "revenue": null,
            "smallest_order_but_1": 130.45
          },
          {
            "custid": "C13",
            "orderno": 1007,
            "revenue": 130.45, // ➋
            "smallest_order_but_1": 130.45
          },
          {
            "custid": "C13",
            "orderno": 1008,
            "revenue": 1999.8,
            "smallest_order_but_1": 130.45
          },
          {
            "custid": "C13",
            "orderno": 1002,
            "revenue": 10906.55,
            "smallest_order_but_1": 130.45
          },
          {
            "custid": "C31",
            "orderno": 1003,
            "revenue": 477.95,
            "smallest_order_but_1": null
          },
          {
            "custid": "C35",
            "orderno": 1004,
            "revenue": 199.94,
            "smallest_order_but_1": null
          },
          {
            "custid": "C37",
            "orderno": 1005,
            "revenue": 4639.92,
            "smallest_order_but_1": null
          },
          {
            "custid": "C41",
            "orderno": 1001,
            "revenue": 157.73,
            "smallest_order_but_1": 18847.58
          },
          {
            "custid": "C41",
            "orderno": 1006,
            "revenue": 18847.58, // ➋
            "smallest_order_but_1": 18847.58
          }
        ]

    ➀ This clause specifies that the window frame should extend to the
    end of the window partition.
    Without this clause, the end point of the window frame would always be the
    current tuple.
    This would mean that for the smallest order, the function
    would be unable to find the route with the second smallest order.

    ➁ The second smallest order from this customer.

* Example 2:

    For each order, show the customer and the value, including the
    value of the second largest order from that customer.

        FROM orders AS o
        LET revenue = ROUND((
          FROM o.items
          SELECT VALUE SUM(qty * price)
        )[0], 2)
        SELECT o.custid, o.orderno, revenue,
        NTH_VALUE(revenue, 2) FROM LAST OVER (
          PARTITION BY o.custid
          ORDER BY revenue
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING -- ➊
        ) AS largest_order_but_1;

* The expected result is:

        [
          {
            "custid": "C13",
            "orderno": 1002,
            "revenue": 10906.55,
            "largest_order_but_1": 1999.8
          },
          {
            "custid": "C13",
            "orderno": 1008,
            "revenue": 1999.8, // ➋
            "largest_order_but_1": 1999.8
          },
          {
            "custid": "C13",
            "orderno": 1007,
            "revenue": 130.45,
            "largest_order_but_1": 1999.8
          },
          {
            "custid": "C13",
            "orderno": 1009,
            "revenue": null,
            "largest_order_but_1": 1999.8
          },
          {
            "custid": "C31",
            "orderno": 1003,
            "revenue": 477.95,
            "largest_order_but_1": null
          },
          {
            "custid": "C35",
            "orderno": 1004,
            "revenue": 199.94,
            "largest_order_but_1": null
          },
          {
            "custid": "C37",
            "orderno": 1005,
            "revenue": 4639.92,
            "largest_order_but_1": null
          },
          {
            "custid": "C41",
            "orderno": 1006,
            "revenue": 18847.58,
            "largest_order_but_1": 157.73
          },
          {
            "custid": "C41",
            "orderno": 1001,
            "revenue": 157.73, // ➋
            "largest_order_but_1": 157.73
          }
        ]

    ➀ This clause specifies that the window frame should extend to the
    end of the window partition.
    Without this clause, the end point of the window frame would always be the
    current tuple.
    This would mean the function would be unable to find the second largest
    order for smaller orders.

    ➁ The second largest order from this customer.

### ntile ###

* Syntax:

        NTILE(num_tiles) OVER ([window-partition-clause] [window-order-clause])

* Divides the window partition into the specified number of tiles, and
  allocates each tuple in the window partition to a tile, so that as far as
  possible each tile has an equal number of tuples.
  When the set of tuples is not equally divisible by the number of tiles, the
  function puts more tuples into the lower-numbered tiles.
  For each tuple, the function returns the number of the tile into which that
  tuple was placed.

    The window order clause determines the sort order of the tuples.
    If the window order clause is omitted then the tuples are processed in
    an undefined order.

* Arguments:

    * `num_tiles`: The number of tiles into which you want to divide
      the window partition.
      This argument can be an expression and must evaluate to a number.
      If the number is not an integer, it will be truncated.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * An value greater than or equal to 1 and less than or equal to the number
      of tiles.

* Example:

    Allocate each order to one of three tiles by value.

        FROM orders AS o
        LET revenue = ROUND((
          FROM o.items
          SELECT VALUE SUM(qty * price)
        )[0], 2)
        SELECT o.orderno, revenue,
        NTILE(3) OVER (
          ORDER BY revenue
        ) AS `ntile`;

* The expected result is:

        [
          {
            "ntile": 1,
            "orderno": 1009,
            "revenue": null
          },
          {
            "ntile": 1,
            "orderno": 1007,
            "revenue": 130.45
          },
          {
            "ntile": 1,
            "orderno": 1001,
            "revenue": 157.73
          },
          {
            "ntile": 2,
            "orderno": 1004,
            "revenue": 199.94
          },
          {
            "ntile": 2,
            "orderno": 1003,
            "revenue": 477.95
          },
          {
            "ntile": 2,
            "orderno": 1008,
            "revenue": 1999.8
          },
          {
            "ntile": 3,
            "orderno": 1005,
            "revenue": 4639.92
          },
          {
            "ntile": 3,
            "orderno": 1002,
            "revenue": 10906.55
          },
          {
            "ntile": 3,
            "orderno": 1006,
            "revenue": 18847.58
          }
        ]

### percent_rank ###

* Syntax:

        PERCENT_RANK() OVER ([window-partition-clause] [window-order-clause])

* Returns the percentile rank of the current tuple – that is, the rank of the
  tuples minus one, divided by the total number of tuples in the window
  partition minus one.

    The window order clause determines the sort order of the tuples.
    If the window order clause is omitted, the function returns the same
    result (0) for each tuple.

* Arguments:

    * None.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * A number between 0 and 1.
      The higher the value, the higher the ranking.

* Example:

    For each customer, find the percentile rank of all orders by order number.

        FROM orders AS o
        SELECT o.custid, o.orderno, PERCENT_RANK() OVER (
          PARTITION BY o.custid
          ORDER BY o.orderno
        ) AS `rank`;

* The expected result is:

        [
          {
            "rank": 0,
            "custid": "C13",
            "orderno": 1002
          },
          {
            "rank": 0.3333333333333333,
            "custid": "C13",
            "orderno": 1007
          },
          {
            "rank": 0.6666666666666666,
            "custid": "C13",
            "orderno": 1008
          },
          {
            "rank": 1,
            "custid": "C13",
            "orderno": 1009
          },
          {
            "rank": 0,
            "custid": "C31",
            "orderno": 1003
          },
          {
            "rank": 0,
            "custid": "C35",
            "orderno": 1004
          },
          {
            "rank": 0,
            "custid": "C37",
            "orderno": 1005
          },
          {
            "rank": 0,
            "custid": "C41",
            "orderno": 1001
          },
          {
            "rank": 1,
            "custid": "C41",
            "orderno": 1006
          }
        ]

### rank ###

* Syntax:

        RANK() OVER ([window-partition-clause] [window-order-clause])

* Returns the rank of the current tuple – that is, the number of distinct
  tuples preceding this tuple in the current window partition, plus one.

    The tuples are ordered by the window order clause.
    If any tuples are tied, they will have the same rank.
    If the window order clause is omitted, the function returns the same
    result (1) for each tuple.

    When any tuples have the same rank, the rank of the next tuple will include
    all preceding tuples, so there may be a gap in the sequence of returned
    values.
    For example, if there are five tuples ranked 3, the next rank is 8.

    To avoid gaps in the returned values, use the DENSE_RANK() function instead.

* Arguments:

    * None.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * An integer, greater than or equal to 1.

* Example:

    Find the rank of all orders by number of items.

        FROM orders AS o
        SELECT o.orderno, LEN(o.items) AS items,
        RANK() OVER (
          ORDER BY LEN(o.items)
        ) AS `rank`;

* The expected result is:

        [
          {
            "items": 0,
            "rank": 1,
            "orderno": 1009
          },
          {
            "items": 1,
            "rank": 2,
            "orderno": 1008
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1004
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1007
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1002
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1001
          },
          {
            "items": 2,
            "rank": 3,
            "orderno": 1003
          },
          {
            "items": 3,
            "rank": 8,
            "orderno": 1006
          },
          {
            "items": 4,
            "rank": 9,
            "orderno": 1005
          }
        ]

### ratio_to_report ###

* Syntax:

        RATIO_TO_REPORT(expr) OVER (window-definition)

* Returns the fractional ratio of the specified value for each tuple to the
  sum of values for all tuples in the window frame.

* Arguments:

    * `expr`: The value for which you want to calculate the
      fractional ratio. <sup>\[[1](#fn_1)\]</sup>

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

    * (Optional) [Window Frame Clause](manual.html#Window_frame_clause).

* Return Value:

    * A number between 0 and 1, representing the fractional ratio of the value
      for the current tuple to the sum of values for all tuples in the
      current window frame.
      The sum of returned values for all tuples in the current window frame is 1.

    * If the input expression does not evaluate to a number, or the sum of
      values for all tuples is zero, it returns NULL.

* Example:

    For each customer, calculate the value of each order as a
    fraction of the total value of all orders.

        FROM orders AS o
        LET revenue = ROUND((
          FROM o.items
          SELECT VALUE SUM(qty * price)
        )[0], 2)
        SELECT o.custid, o.orderno,
        RATIO_TO_REPORT(revenue) OVER (
          PARTITION BY o.custid
        ) AS fractional_ratio;

* The expected result is:

        [
          {
            "custid": "C13",
            "orderno": 1007,
            "fractional_ratio": 0.010006289887088855
          },
          {
            "custid": "C13",
            "orderno": 1002,
            "fractional_ratio": 0.8365971710849288
          },
          {
            "custid": "C13",
            "orderno": 1009,
            "fractional_ratio": null
          },
          {
            "custid": "C13",
            "orderno": 1008,
            "fractional_ratio": 0.15339653902798234
          },
          {
            "custid": "C31",
            "orderno": 1003,
            "fractional_ratio": 1
          },
          {
            "custid": "C35",
            "orderno": 1004,
            "fractional_ratio": 1
          },
          {
            "custid": "C37",
            "orderno": 1005,
            "fractional_ratio": 1
          },
          {
            "custid": "C41",
            "orderno": 1006,
            "fractional_ratio": 0.9917007404772666
          },
          {
            "custid": "C41",
            "orderno": 1001,
            "fractional_ratio": 0.008299259522733382
          }
        ]

### row_number ###

* Syntax:

        ROW_NUMBER() OVER ([window-partition-clause] [window-order-clause])

* Returns a unique row number for every tuple in every window partition.
  In each window partition, the row numbering starts at 1.

    The window order clause determines the sort order of the tuples.
    If the window order clause is omitted, the return values may be
    unpredictable.

* Arguments:

    * None.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Optional) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * An integer, greater than or equal to 1.

* Example:

    For each customer, number all orders by value.

        FROM orders AS o
        LET revenue = ROUND((
          FROM o.items
          SELECT VALUE SUM(qty * price)
        )[0], 2)
        SELECT o.custid, o.orderno,
        ROW_NUMBER() OVER (
          PARTITION BY o.custid
          ORDER BY revenue
        ) AS `row`;

* The expected result is:

        [
          {
            "row": 1,
            "custid": "C13",
            "orderno": 1009
          },
          {
            "row": 2,
            "custid": "C13",
            "orderno": 1007
          },
          {
            "row": 3,
            "custid": "C13",
            "orderno": 1008
          },
          {
            "row": 4,
            "custid": "C13",
            "orderno": 1002
          },
          {
            "row": 1,
            "custid": "C31",
            "orderno": 1003
          },
          {
            "row": 1,
            "custid": "C35",
            "orderno": 1004
          },
          {
            "row": 1,
            "custid": "C37",
            "orderno": 1005
          },
          {
            "row": 1,
            "custid": "C41",
            "orderno": 1001
          },
          {
            "row": 2,
            "custid": "C41",
            "orderno": 1006
          }
        ]

---

<a id="fn_1">1</a>.
If the query contains the GROUP BY clause or any
[aggregate functions](#AggregateFunctions), this expression must only
depend on GROUP BY expressions or aggregate functions.
