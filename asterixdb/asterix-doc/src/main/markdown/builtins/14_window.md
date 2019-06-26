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
Refer to [OVER Clauses](manual.html#Over_clauses) for details.

Window functions cannot appear in the FROM clause clause or LIMIT clause.

The examples in this section use the `GleambookMessages` dataset,
described in the section on [SELECT Statements](manual.html#SELECT_statements).

### cume_dist ###

* Syntax:

        CUME_DIST() OVER ([window-partition-clause] window-order-clause)

* Returns the percentile rank of the current tuple as part of the cumulative
  distribution – that is, the number of tuples ranked lower than or equal to
  the current tuple, including the current tuple, divided by the total number
  of tuples in the window partition.

* Arguments:

    * None.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Required) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * A number greater than 0 and less than or equal to 1.
      The higher the value, the higher the ranking.

* Example:

    For each author, find the cumulative distribution of all messages
    in order of message ID.

        SELECT m.messageId, m.authorId, CUME_DIST() OVER (
          PARTITION BY m.authorId
          ORDER BY m.messageId
        ) AS `rank`
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "rank": 0.2,
            "messageId": 2,
            "authorId": 1
          },
          {
            "rank": 0.4,
            "messageId": 4,
            "authorId": 1
          },
          {
            "rank": 0.6,
            "messageId": 8,
            "authorId": 1
          },
          {
            "rank": 0.8,
            "messageId": 10,
            "authorId": 1
          },
          {
            "rank": 1,
            "messageId": 11,
            "authorId": 1
          },
          {
            "rank": 0.5,
            "messageId": 3,
            "authorId": 2
          },
          {
            "rank": 1,
            "messageId": 6,
            "authorId": 2
          }
        ]

### dense_rank ###

* Syntax:

        DENSE_RANK() OVER ([window-partition-clause] window-order-clause)

* Returns the dense rank of the current tuple – that is, the number of
  distinct tuples preceding this tuple in the current window partition, plus
  one.

    The tuples are ordered by the window order clause.
    If any tuples are tied, they will have the same rank.

    For this function, when any tuples have the same rank, the rank of the next
    tuple will be consecutive, so there will not be a gap in the sequence of
    returned values.
    For example, if there are three tuples ranked 2, the next dense rank is 3.

* Arguments:

    * None.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Required) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * An integer, greater than or equal to 1.

* Example:

    For each author, find the dense rank of all messages in order of location.

        SELECT m.authorId, m.messageId, m.senderLocation[1] as longitude,
        DENSE_RANK() OVER (
          PARTITION BY m.authorId
          ORDER BY m.senderLocation[1]
        ) AS `rank`
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "rank": 1,
            "authorId": 1,
            "messageId": 10,
            "longitude": 70.01
          },
          {
            "rank": 2,
            "authorId": 1,
            "messageId": 11,
            "longitude": 77.49
          },
          {
            "rank": 3,
            "authorId": 1,
            "messageId": 2,
            "longitude": 80.87
          },
          {
            "rank": 3,
            "authorId": 1,
            "messageId": 8,
            "longitude": 80.87
          },
          {
            "rank": 4,
            "authorId": 1,
            "messageId": 4,
            "longitude": 97.04
          },
          {
            "rank": 1,
            "authorId": 2,
            "messageId": 6,
            "longitude": 75.56
          },
          {
            "rank": 2,
            "authorId": 2,
            "messageId": 3,
            "longitude": 81.01
          }
        ]

### first_value ###

* Syntax:

        FIRST_VALUE(expr) [nulls-treatment] OVER (window-definition)

* Returns the requested value from the first tuple in the current window
  frame, where the window frame is specified by the window definition.

* Arguments:

    * `expr`: The value that you want to return from the first
      tuple in the window frame. <sup>\[[1](#fn_1)\]</sup>

* Modifiers:

    * [Nulls Treatment](manual.html#Nulls_treatment): (Optional) Determines how
      NULL or MISSING values are treated when finding the first tuple in the
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

    * If all values are NULL or MISSING it returns NULL.

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

    For each author, show the length of each message, including the
    length of the shortest message from that author.

        SELECT m.authorId, m.messageId,
        LENGTH(m.message) AS message_length,
        FIRST_VALUE(LENGTH(m.message)) OVER (
          PARTITION BY m.authorId
          ORDER BY LENGTH(m.message)
        ) AS shortest_message
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "message_length": 31,
            "shortest_message": 31,
            "authorId": 1,
            "messageId": 8
          },
          {
            "message_length": 39,
            "shortest_message": 31,
            "authorId": 1,
            "messageId": 11
          },
          {
            "message_length": 44,
            "shortest_message": 31,
            "authorId": 1,
            "messageId": 4
          },
          {
            "message_length": 45,
            "shortest_message": 31,
            "authorId": 1,
            "messageId": 2
          },
          {
            "message_length": 51,
            "shortest_message": 31,
            "authorId": 1,
            "messageId": 10
          },
          {
            "message_length": 35,
            "shortest_message": 35,
            "authorId": 2,
            "messageId": 3
          },
          {
            "message_length": 44,
            "shortest_message": 35,
            "authorId": 2,
            "messageId": 6
          }
        ]

### lag ###

* Syntax:

        LAG(expr[, offset[, default]]) [nulls-treatment] OVER ([window-partition-clause] window-order-clause)

* Returns the value of a tuple at a given offset prior to the current tuple
  position.

* Arguments:

    * `expr`: The value that you want to return from the offset
      tuple. <sup>\[[1](#fn_1)\]</sup>

    * `offset`: (Optional) A positive integer greater than 0.
      If omitted, the default is 1.

    * `default`: (Optional) The value to return when the offset goes out of
      window scope.
      If omitted, the default is NULL.

* Modifiers:

    * [Nulls Treatment](manual.html#Nulls_treatment): (Optional) Determines how
      NULL or MISSING values are treated when finding the first tuple in the
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

    * (Required) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * The specified value from the offset tuple.

    * If the offset tuple is out of scope, it returns the default value,
      or NULL if no default is specified.

* Example:

    For each author, show the length of each message, including the
    length of the next-shortest message.

        SELECT m.authorId, m.messageId,
        LENGTH(m.message) AS message_length,
        LAG(LENGTH(m.message), 1, "No shorter message") OVER (
          PARTITION BY m.authorId
          ORDER BY LENGTH(m.message)
        ) AS next_shortest_message
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "message_length": 31,
            "authorId": 1,
            "messageId": 8,
            "next_shortest_message": "No shorter message"
          },
          {
            "message_length": 39,
            "authorId": 1,
            "messageId": 11,
            "next_shortest_message": 31
          },
          {
            "message_length": 44,
            "authorId": 1,
            "messageId": 4,
            "next_shortest_message": 39
          },
          {
            "message_length": 45,
            "authorId": 1,
            "messageId": 2,
            "next_shortest_message": 44
          },
          {
            "message_length": 51,
            "authorId": 1,
            "messageId": 10,
            "next_shortest_message": 45
          },
          {
            "message_length": 35,
            "authorId": 2,
            "messageId": 3,
            "next_shortest_message": "No shorter message"
          },
          {
            "message_length": 44,
            "authorId": 2,
            "messageId": 6,
            "next_shortest_message": 35
          }
        ]

### last_value ###

* Syntax:

        LAST_VALUE(expr) [nulls-treatment] OVER (window-definition)

* Returns the requested value from the last tuple in the current window frame,
  where the window frame is specified by the window definition.

* Arguments:

    * `expr`: The value that you want to return from the last tuple
      in the window frame. <sup>\[[1](#fn_1)\]</sup>

* Modifiers:

    * [Nulls Treatment](manual.html#Nulls_treatment): (Optional) Determines how
      NULL or MISSING values are treated when finding the first tuple in the
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

    * The specified value from the last tuple.
      The order of the tuples is determined by the window order clause.

    * If all values are NULL or MISSING it returns NULL.

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

    For each author, show the length of each message, including the
    length of the longest message from that author.

        SELECT m.authorId, m.messageId,
        LENGTH(m.message) AS message_length,
        LAST_VALUE(LENGTH(m.message)) OVER (
          PARTITION BY m.authorId
          ORDER BY LENGTH(m.message)
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING -- ➊
        ) AS longest_message
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "message_length": 31,
            "longest_message": 51,
            "authorId": 1,
            "messageId": 8
          },
          {
            "message_length": 39,
            "longest_message": 51,
            "authorId": 1,
            "messageId": 11
          },
          {
            "message_length": 44,
            "longest_message": 51,
            "authorId": 1,
            "messageId": 4
          },
          {
            "message_length": 45,
            "longest_message": 51,
            "authorId": 1,
            "messageId": 2
          },
          {
            "message_length": 51,
            "longest_message": 51,
            "authorId": 1,
            "messageId": 10
          },
          {
            "message_length": 35,
            "longest_message": 44,
            "authorId": 2,
            "messageId": 3
          },
          {
            "message_length": 44,
            "longest_message": 44,
            "authorId": 2,
            "messageId": 6
          }
        ]

    ➀ This clause specifies that the window frame should extend to the
    end of the window partition.
    Without this clause, the end point of the window frame would always be the
    current tuple.
    This would mean that the longest message would always be the same as the
    current message.

### lead ###

* Syntax:

        LEAD(expr[, offset[, default]]) [nulls-treatment] OVER ([window-partition-clause] window-order-clause)

* Returns the value of a tuple at a given offset ahead of the current tuple
  position.

* Arguments:

    * `expr`: The value that you want to return from the offset
      tuple. <sup>\[[1](#fn_1)\]</sup>

    * `offset`: (Optional) A positive integer greater than 0. If omitted, the
      default is 1.

    * `default`: (Optional) The value to return when the offset goes out of
      window scope.
      If omitted, the default is NULL.

* Modifiers:

    * [Nulls Treatment](manual.html#Nulls_treatment): (Optional) Determines how
      NULL or MISSING values are treated when finding the first tuple in the
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

    * (Required) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * The specified value from the offset tuple.

    * If the offset tuple is out of scope, it returns the default value, or
      NULL if no default is specified.

* Example:

    For each author, show the length of each message, including the
    length of the next-longest message.

        SELECT m.authorId, m.messageId,
        LENGTH(m.message) AS message_length,
        LEAD(LENGTH(m.message), 1, "No longer message") OVER (
          PARTITION BY m.authorId
          ORDER BY LENGTH(m.message)
        ) AS next_longest_message
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "message_length": 31,
            "authorId": 1,
            "messageId": 8,
            "next_longest_message": 39
          },
          {
            "message_length": 39,
            "authorId": 1,
            "messageId": 11,
            "next_longest_message": 44
          },
          {
            "message_length": 44,
            "authorId": 1,
            "messageId": 4,
            "next_longest_message": 45
          },
          {
            "message_length": 45,
            "authorId": 1,
            "messageId": 2,
            "next_longest_message": 51
          },
          {
            "message_length": 51,
            "authorId": 1,
            "messageId": 10,
            "next_longest_message": "No longer message"
          },
          {
            "message_length": 35,
            "authorId": 2,
            "messageId": 3,
            "next_longest_message": 44
          },
          {
            "message_length": 44,
            "authorId": 2,
            "messageId": 6,
            "next_longest_message": "No longer message"
          }
        ]

### nth_value ###

* Syntax:

        NTH_VALUE(expr, offset) [nthval-from] [nulls-treatment] OVER (window-definition)

* Returns the requested value from a tuple in the current window frame, where
  the window frame is specified by the window definition.

* Arguments:

    * `expr`: The value that you want to return from the offset
      tuple in the window frame. <sup>\[[1](#fn_1)\]</sup>

    * `offset`: The number of the offset tuple within the window
      frame, counting from 1.

* Modifiers:

    * [Nth Val From](manual.html#Nth_val_from): (Optional) Determines where the
      function starts counting the offset.

        - `FROM FIRST`: Counting starts at the first tuple in the window frame.
          In this case, an offset of 1 is the first tuple in the window frame,
          2 is the second tuple, and so on.

        - `FROM LAST`: Counting starts at the last tuple in the window frame.
          In this case, an offset of 1 is the last tuple in the window frame,
          2 is the second-to-last tuple, and so on.

        The order of the tuples is determined by the window order clause.
        If this modifier is omitted, the default is `FROM FIRST`.

    * [Nulls Treatment](manual.html#Nulls_treatment): (Optional) Determines how
      NULL or MISSING values are treated when finding the first tuple in the
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

    * The specified value from the offset tuple.

    * In the following cases, this function may return unpredictable results.

        - If the window order clause is omitted.

        - If the window frame clause is omitted.

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

    For each author, show the length of each message, including the
    length of the second shortest message from that author.

        SELECT m.authorId, m.messageId,
        LENGTH(m.message) AS message_length,
        NTH_VALUE(LENGTH(m.message), 2) FROM FIRST OVER (
          PARTITION BY m.authorId
          ORDER BY LENGTH(m.message)
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING -- ➊
        ) AS shortest_message_but_1
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "message_length": 31,
            "shortest_message_but_1": 39,
            "authorId": 1,
            "messageId": 8
          },
          {
            "message_length": 39,
            "shortest_message_but_1": 39,
            "authorId": 1,
            "messageId": 11 // ➋
          },
          {
            "message_length": 44,
            "shortest_message_but_1": 39,
            "authorId": 1,
            "messageId": 4
          },
          {
            "message_length": 45,
            "shortest_message_but_1": 39,
            "authorId": 1,
            "messageId": 2
          },
          {
            "message_length": 51,
            "shortest_message_but_1": 39,
            "authorId": 1,
            "messageId": 10
          },
          {
            "message_length": 35,
            "shortest_message_but_1": 44,
            "authorId": 2,
            "messageId": 3
          },
          {
            "message_length": 44,
            "shortest_message_but_1": 44,
            "authorId": 2,
            "messageId": 6 // ➋
          }
        ]

    ➀ This clause specifies that the window frame should extend to the
    end of the window partition.
    Without this clause, the end point of the window frame would always be the
    current tuple.
    This would mean that for the shortest message, the function
    would be unable to find the route with the second shortest message.

    ➁ The second shortest message from this author.

* Example 2:

    For each author, show the length of each message, including the
    length of the second longest message from that author.

        SELECT m.authorId, m.messageId,
        LENGTH(m.message) AS message_length,
        NTH_VALUE(LENGTH(m.message), 2) FROM LAST OVER (
          PARTITION BY m.authorId
          ORDER BY LENGTH(m.message)
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING -- ➊
        ) AS longest_message_but_1
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "message_length": 31,
            "longest_message_but_1": 45,
            "authorId": 1,
            "messageId": 8
          },
          {
            "message_length": 39,
            "longest_message_but_1": 45,
            "authorId": 1,
            "messageId": 11
          },
          {
            "message_length": 44,
            "longest_message_but_1": 45,
            "authorId": 1,
            "messageId": 4
          },
          {
            "message_length": 45,
            "longest_message_but_1": 45,
            "authorId": 1,
            "messageId": 2 // ➋
          },
          {
            "message_length": 51,
            "longest_message_but_1": 45,
            "authorId": 1,
            "messageId": 10
          },
          {
            "message_length": 35,
            "longest_message_but_1": 35,
            "authorId": 2,
            "messageId": 3 // ➋
          },
          {
            "message_length": 44,
            "longest_message_but_1": 35,
            "authorId": 2,
            "messageId": 6
          }
        ]

    ➀ This clause specifies that the window frame should extend to the
    end of the window partition.
    Without this clause, the end point of the window frame would always be the
    current tuple.
    This would mean the function would be unable to find the second longest
    message for shorter messages.

    ➁ The second longest message from this author.

### ntile ###

* Syntax:

        NTILE(num_tiles) OVER ([window-partition-clause] window-order-clause)

* Divides the window partition into the specified number of tiles, and
  allocates each tuple in the window partition to a tile, so that as far as
  possible each tile has an equal number of tuples.
  When the set of tuples is not equally divisible by the number of tiles, the
  function puts more tuples into the lower-numbered tiles.
  For each tuple, the function returns the number of the tile into which that
  tuple was placed.

* Arguments:

    * `num_tiles`: The number of tiles into which you want to divide
      the window partition.
      This argument can be an expression and must evaluate to a number.
      If the number is not an integer, it will be truncated.
      If the expression depends on a tuple, it evaluates from the first
      tuple in the window partition.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Required) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * An value greater than or equal to 1 and less than or equal to the number
      of tiles.

* Example:

    Allocate each message to one of three tiles by length and message ID.

        SELECT m.messageId, LENGTH(m.message) AS `length`,
        NTILE(3) OVER (
          ORDER BY LENGTH(m.message), m.messageId
        ) AS `ntile`
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "length": 31,
            "ntile": 1,
            "messageId": 8
          },
          {
            "length": 35,
            "ntile": 1,
            "messageId": 3
          },
          {
            "length": 39,
            "ntile": 1,
            "messageId": 11
          },
          {
            "length": 44,
            "ntile": 2,
            "messageId": 4
          },
          {
            "length": 44,
            "ntile": 2,
            "messageId": 6
          },
          {
            "length": 45,
            "ntile": 3,
            "messageId": 2
          },
          {
            "length": 51,
            "ntile": 3,
            "messageId": 10
          }
        ]

### percent_rank ###

* Syntax:

        PERCENT_RANK() OVER ([window-partition-clause] window-order-clause)

* Returns the percentile rank of the current tuple – that is, the rank of the
  tuples minus one, divided by the total number of tuples in the window
  partition minus one.

* Arguments:

    * None.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Required) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * A number between 0 and 1.
      The higher the value, the higher the ranking.

* Example:

    For each author, find the percentile rank of all messages in order
    of message ID.

        SELECT m.messageId, m.authorId, PERCENT_RANK() OVER (
          PARTITION BY m.authorId
          ORDER BY m.messageId
        ) AS `rank`
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "rank": 0,
            "messageId": 2,
            "authorId": 1
          },
          {
            "rank": 0.25,
            "messageId": 4,
            "authorId": 1
          },
          {
            "rank": 0.5,
            "messageId": 8,
            "authorId": 1
          },
          {
            "rank": 0.75,
            "messageId": 10,
            "authorId": 1
          },
          {
            "rank": 1,
            "messageId": 11,
            "authorId": 1
          },
          {
            "rank": 0,
            "messageId": 3,
            "authorId": 2
          },
          {
            "rank": 1,
            "messageId": 6,
            "authorId": 2
          }
        ]

### rank ###

* Syntax:

        RANK() OVER ([window-partition-clause] window-order-clause)

* Returns the rank of the current tuple – that is, the number of distinct
  tuples preceding this tuple in the current window partition, plus one.

    The tuples are ordered by the window order clause.
    If any tuples are tied, they will have the same rank.

    When any tuples have the same rank, the rank of the next tuple will include
    all preceding tuples, so there may be a gap in the sequence of returned
    values.
    For example, if there are three tuples ranked 2, the next rank is 5.

* Arguments:

    * None.

* Clauses:

    * (Optional) [Window Partition Clause](manual.html#Window_partition_clause).

    * (Required) [Window Order Clause](manual.html#Window_order_clause).

* Return Value:

    * An integer, greater than or equal to 1.

* Example:

    For each author, find the rank of all messages in order of location.

        SELECT m.authorId, m.messageId, m.senderLocation[1] as longitude,
        RANK() OVER (
          PARTITION BY m.authorId
          ORDER BY m.senderLocation[1]
        ) AS `rank`
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "rank": 1,
            "authorId": 1,
            "messageId": 10,
            "longitude": 70.01
          },
          {
            "rank": 2,
            "authorId": 1,
            "messageId": 11,
            "longitude": 77.49
          },
          {
            "rank": 3,
            "authorId": 1,
            "messageId": 2,
            "longitude": 80.87
          },
          {
            "rank": 3,
            "authorId": 1,
            "messageId": 8,
            "longitude": 80.87
          },
          {
            "rank": 5,
            "authorId": 1,
            "messageId": 4,
            "longitude": 97.04
          },
          {
            "rank": 1,
            "authorId": 2,
            "messageId": 6,
            "longitude": 75.56
          },
          {
            "rank": 2,
            "authorId": 2,
            "messageId": 3,
            "longitude": 81.01
          }
        ]

### ratio_to_report ###

* Syntax:

        RATIO_TO_REPORT(expr) OVER (window-definition)

* Returns the fractional ratio of the specified value for each tuple to the
  sum of values for all tuples in the window partition.

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
      The sum of values for all tuples in the current window frame is 1.

    * If the input expression does not evaluate to a number, or the sum of
      values for all tuples is zero, it returns NULL.

* Example:

    For each author, calculate the length of each message as a
    fraction of the total length of all messages.

        SELECT m.messageId, m.authorId,
        RATIO_TO_REPORT(LENGTH(m.message)) OVER (
          PARTITION BY m.authorId
        ) AS length_ratio
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "length_ratio": 0.21428571428571427,
            "messageId": 2,
            "authorId": 1
          },
          {
            "length_ratio": 0.20952380952380953,
            "messageId": 4,
            "authorId": 1
          },
          {
            "length_ratio": 0.14761904761904762,
            "messageId": 8,
            "authorId": 1
          },
          {
            "length_ratio": 0.24285714285714285,
            "messageId": 10,
            "authorId": 1
          },
          {
            "length_ratio": 0.18571428571428572,
            "messageId": 11,
            "authorId": 1
          },
          {
            "length_ratio": 0.4430379746835443,
            "messageId": 3,
            "authorId": 2
          },
          {
            "length_ratio": 0.5569620253164557,
            "messageId": 6,
            "authorId": 2
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

    For each author, number all messages in order of length.

        SELECT m.messageId, m.authorId,
        ROW_NUMBER() OVER (
          PARTITION BY m.authorId
          ORDER BY LENGTH(m.message)
        ) AS `row`
        FROM GleambookMessages AS m;

* The expected result is:

        [
          {
            "row": 1,
            "messageId": 8,
            "authorId": 1
          },
          {
            "row": 2,
            "messageId": 11,
            "authorId": 1
          },
          {
            "row": 3,
            "messageId": 4,
            "authorId": 1
          },
          {
            "row": 4,
            "messageId": 2,
            "authorId": 1
          },
          {
            "row": 5,
            "messageId": 10,
            "authorId": 1
          },
          {
            "row": 1,
            "messageId": 3,
            "authorId": 2
          },
          {
            "row": 2,
            "messageId": 6,
            "authorId": 2
          }
        ]

---

<a id="fn_1">1</a>.
If the query contains the GROUP BY clause or any
[aggregate functions](#AggregateFunctions), this expression must only
depend on GROUP BY expressions or aggregate functions.
