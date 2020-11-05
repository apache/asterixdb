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

## <a id="Interval_joins">Interval Joins</a>
This system allows for the 13 types of Allen's interval-join relations.
The default, when using these joins, is either Nested Loop, or Hybrid Hash Join.
The optimal algorithm will be automatically selected based on the query.
Hybrid Hash Join will be selected in cases where the relation includes an equality;
these cases are: `interval_starts()`, `interval_started_by()`, `interval_ends()`, `interval_ended_by()`,
`interval_meets()`, and `interval_met_by()`.
Otherwise, the system will default to nested loop join.
To use interval merge join you must include a range hint.
Adding a range hint allows for the system to pick interval merge join.

The 13 interval functions are `interval_after()`, `interval_before()`, `interval_covers()`, `interval_covered_by()`,
`interval_ends()`, `interval_ended_by()`, `interval_meets()`, `interval_met_by()`, `interval_overlaps()`,
`interval_overlapping()`, `interval_overlapped_by()`, `interval_starts()`, and `interval_started_by()`.

##### How to use an interval join

    select f.name as staff, d.name as student
    from Staff as f, Students as d
    where interval_after(f.employment, d.attendance)

In this scenario, `interval_after()` can be replaced with any of the 13 join functions.
Here is what each of the functions represent if A represents the first interval parameter,
and B represents the second set interval parameter:

| Function | Condition |
|-------------------------|-------------------------|
| Before(A, B) and After(B, A) | A.end < B.start |
| Covers(A, B) and Covered_by(B, A) | A.start <= B.start and A.end >= B.end |
| Ends(A, B) and Ended_by(B, A) | A.end = B.end and A.start >= B.start |
| Meets(A, B) and Met_by(B, A) | A.end = B.start |
| Overlaps(A, B) and Overlapped_by(B, A) | A.start < B.start and B.start > A.end and A.end > B.start |
| Overlapping(A, B)| (A.start >= B.start and B.start < A.end) or (B.end <= A.end and B.end < A.start)|
| Starts(A, B) and Started_by(B, A) | A.start = B.start and A.end <= B.end |

### <a id="Range_hint"> Using a Range Hint </a>

To use an efficient interval join the data must be partitioned with the details in a range hint.
Interval joins with a range hint currently work for intervals types of date, datetime, or time;
the range hint type must match the interval type.
Adding a range hint directly before the interval join function will cause the system to pick interval
merge join for these interval functions: `interval_after()`, `interval_before()`, `interval_covers()`,
`interval_covered_by()`, `interval_overlaps()`, `interval_overlapping()`, `interval_overlapped_by()`.
The other relations will ignore the range hint and pick Hybrid Hash Join as described earlier.

Here is an example of how interval joins work with a range hint for all the supported data types.
Suppose that we have two sets of data, a data set of staff members with an interval for length of
employment and an id.
The other dataset represents students, which may include an interval for attendance and an id.
Each partition receives data based on the split points;
The split points in the range hint must be strategically set by the
user so that the data divides evenly among partitions.
For example, if your query contains 1 split point, and the system is using two partitions,
the data before the split point will be sent to the first partition,
and the data after the split point will be sent to the second partition.
This continues to work respectively based on the number of split points and number of partitions.
Ideally, the number of split points should equal the number of partitions - 1.

##### Range Hint Example

    /*+ range [<Expression>, ..., ] */

##### Range Hint Example with Date

    select f.name as staff, d.name as student
    from Staff as f, Students as d
    where
    /*+ range [date("2003-06-30"), date("2005-12-31"), date("2008-06-30")] */
    interval_after(f.employment, d.attendance)
    order by f.name, d.name;
