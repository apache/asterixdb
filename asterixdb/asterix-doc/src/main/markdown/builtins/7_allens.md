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

### interval_before, interval_after ###

 * Syntax:

        interval_before(interval1, interval2)
        interval_after(interval1, interval2)

 * These two functions check whether an interval happens before/after another interval.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
    * a `boolean` value. Specifically, `interval_before(interval1, interval2)` is true if and
      only if `interval1.end < interval2.start`, and `interval_after(interval1, interval2)` is true
      if and only if `interval1.start > interval2.end`.
    * `missing` if the argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-interval input value will cause a type error.

 * Examples:

        {
          "interval_before": interval_before(interval(date("2000-01-01"), date("2005-01-01")),
                                             interval(date("2005-05-01"), date("2012-09-09"))),
          "interval_after": interval_after(interval(date("2005-05-01"), date("2012-09-09")),
                                           interval(date("2000-01-01"), date("2005-01-01")))
        };

 * The expected result is:

        { "interval_before": true, "interval_after": true }


### interval_covers, interval_covered_by ###

 * Syntax:

        interval_covers(interval1, interval2)
        interval_covered_by(interval1, interval2)

 * These two functions check whether one interval covers the other interval.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
    * a `boolean` value. Specifically, `interval_covers(interval1, interval2)` is true if and only if

        interval1.start <= interval2.start AND interval1.end >= interval2.end

        `interval_covered_by(interval1, interval2)` is true if and only if

        interval2.start <= interval1.start AND interval2.end >= interval1.end

    * `missing` if the argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-interval input value will cause a type error.

 * Examples:

        {
          "interval_covers": interval_covers(interval(date("2000-01-01"), date("2005-01-01")),
                                             interval(date("2000-03-01"), date("2004-09-09"))),
          "interval_covered_by": interval_covered_by(interval(date("2006-08-01"), date("2007-03-01")),
                                                     interval(date("2004-09-10"), date("2012-08-01")))
        };

 * The expected result is:

        { "interval_covers": true, "interval_covered_by": true }


### interval_overlaps, interval_overlapped_by ###

 * Syntax:

        interval_overlaps(interval1, interval2)
        interval_overlapped_by(interval1, interval2)

 * These functions check whether two intervals overlap with each other.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:

    * a `boolean` value. Specifically, `interval_overlaps(interval1, interval2)` is true if and only if

        interval1.start < interval2.start
        AND interval2.end > interval1.end
        AND interval1.end > interval2.start

    `interval_overlapped_by(interval1, interval2)` is true if and only if

        interval2.start < interval1.start
        AND interval1.end > interval2.end
        AND interval2.end > interval1.start

    * `missing` if the argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-interval input value will cause a type error.

    Note that `interval_overlaps` and `interval_overlapped_by` are following the Allen's relations on the definition of overlap.

 * Examples:

        {
          "overlaps": interval_overlaps(interval(date("2000-01-01"), date("2005-01-01")),
                                        interval(date("2004-05-01"), date("2012-09-09"))),
          "overlapped_by": interval_overlapped_by(interval(date("2006-08-01"), date("2007-03-01")),
                                                  interval(date("2004-05-01"), date("2012-09-09"))))
        };

 * The expected result is:

        { "overlaps": true, "overlapped_by": true }


###  interval_overlapping ###
Note that `interval_overlapping` is not an Allen's Relation, but syntactic sugar we added for the case that the intersect of two intervals is not empty. Basically this function returns true if any of these functions return true: `interval_overlaps`, `interval_overlapped_by`, `interval_covers`, or `interval_covered_by`.

 * Syntax:

        interval_overlapping(interval1, interval2)

 * This functions check whether two intervals share any points with each other.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
    * a `boolean` value. Specifically, `interval_overlapping(interval1, interval2)` is true if

        interval1.start < interval2.end
        AND interval1.end > interval2.start

    * `missing` if the argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-interval input value will cause a type error.

 * Examples:

        {
          "overlapping1": interval_overlapping(interval(date("2000-01-01"), date("2005-01-01")),
                                               interval(date("2004-05-01"), date("2012-09-09"))),
          "overlapping2": interval_overlapping(interval(date("2006-08-01"), date("2007-03-01")),
                                               interval(date("2004-09-10"), date("2006-12-31")))
        };

 * The expected result is:

        { "overlapping1": true, "overlapping2": true }


### interval_meets, interval_met_by ###

 * Syntax:

        interval_meets(interval1, interval2)
        interval_met_by(interval1, interval2)

 * These two functions check whether an interval meets with another interval.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
    * a `boolean` value. Specifically, `interval_meets(interval1, interval2)` is true if and only if
      `interval1.end = interval2.start`, and `interval_met_by(interval1, interval2)` is true if and only
      if `interval1.start = interval2.end`. If any of the two inputs is `null`, `null` is returned.
    * `missing` if the argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-interval input value will cause a type error.

 * Examples:

        {
          "meets": interval_meets(interval(date("2000-01-01"), date("2005-01-01")),
                                  interval(date("2005-01-01"), date("2012-09-09"))),
          "metby": interval_met_by(interval(date("2006-08-01"), date("2007-03-01")),
                                   interval(date("2004-09-10"), date("2006-08-01")))
        };

 * The expected result is:

        { "meets": true, "metby": true }


### interval_starts, interval_started_by ###

 * Syntax:

        interval_starts(interval1, interval2)
        interval_started_by(interval1, interval2)

 * These two functions check whether one interval starts with the other interval.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
    * a `boolean` value. Specifically, `interval_starts(interval1, interval2)` returns true if and only if

        interval1.start = interval2.start
        AND interval1.end <= interval2.end

       `interval_started_by(interval1, interval2)` returns true if and only if

        interval1.start = interval2.start
        AND interval2.end <= interval1.end

    * `missing` if the argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-interval input value will cause a type error.

 * Examples:

        {
          "interval_starts": interval_starts(interval(date("2000-01-01"), date("2005-01-01")),
                                             interval(date("2000-01-01"), date("2012-09-09"))),
          "interval_started_by": interval_started_by(interval(date("2006-08-01"), date("2007-03-01")),
                                                     interval(date("2006-08-01"), date("2006-08-02")))
        };

 * The expected result is:

        { "interval_starts": true, "interval_started_by": true }


### interval_ends, interval_ended_by ###

* Syntax:

        interval_ends(interval1, interval2)
        interval_ended_by(interval1, interval2)

 * These two functions check whether one interval ends with the other interval.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
    * a `boolean` value. Specifically, `interval_ends(interval1, interval2)` returns true if and only if

        interval1.end = interval2.end
        AND interval1.start >= interval2.start

        `interval_ended_by(interval1, interval2)` returns true if and only if

        interval2.end = interval1.end
        AND interval2.start >= interval1.start

    * `missing` if the argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-interval input value will cause a type error.

* Examples:

        {
          "interval_ends": interval_ends(interval(date("2000-01-01"), date("2005-01-01")),
                                         interval(date("1998-01-01"), date("2005-01-01"))),
          "interval_ended_by": interval_ended_by(interval(date("2006-08-01"), date("2007-03-01")),
                                                 interval(date("2006-09-10"), date("2007-03-01")))
        };

* The expected result is:

        { "interval_ends": true, "interval_ended_by": true }

