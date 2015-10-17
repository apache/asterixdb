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

# AsterixDB Temporal Functions: Allen's Relations #

## <a id="toc">Table of Contents</a> ##

* [About Allen's Relations](#AboutAllensRelations)
* [Allen's Relations Functions](#AllensRelatonsFunctions)


## <a id="AboutAllensRelations">About Allen's Relations</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

AsterixDB supports Allen's relations over interval types. Allen's relations are also called Allen's interval algebra. There are totally 13 base relations described by this algebra, and all of them are supported in AsterixDB (note that `interval-equals` is supported by the `=` comparison symbol so there is no extra function for it). 

A detailed description of Allen's relations can be found from its [wikipedia entry](http://en.wikipedia.org/wiki/Allen's_interval_algebra). 

## <a id="AllensRelatonsFunctions">Allen's Relations Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

### interval-before, interval-after ###

 * Syntax:

        interval-before(interval1, interval2)
        interval-after(interval1, interval2)

 * These two functions check whether an interval happens before/after another interval. 
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
   
    A `boolean` value. Specifically, `interval-before(interval1, interval2)` is true if and only if `interval1.end < interval2.start`, and `interval-after(interval1, interval2)` is true if and only if `interval1.start > interval2.end`. If any of the two inputs is `null`, `null` is returned.

 * Examples:

        let $itv1 := interval-from-date("2000-01-01", "2005-01-01")
        let $itv2 := interval-from-date("2005-05-01", "2012-09-09")
        return {"interval-before": interval-before($itv1, $itv2), "interval-after": interval-after($itv2, $itv1)}
        
 * The expected result is:

        { "interval-before": true, "interval-after": true }


### interval-covers, interval-covered-by ###

 * Syntax:

        interval-covers(interval1, interval2)
        interval-covered-by(interval1, interval2)

 * These two functions check whether one interval covers the other interval.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
   
    A `boolean` value. Specifically, `interval-covers(interval1, interval2)` is true if and only if

        interval1.start <= interval2.start
        AND interval1.end >= interval2.end

    `interval-covered-by(interval1, interval2)` is true if and only if

        interval2.start <= interval1.start
        AND interval2.end >= interval1.end

    For both functions, if any of the two inputs is `null`, `null` is returned.

 * Examples:

        let $itv1 := interval-from-date("2000-01-01", "2005-01-01")
        let $itv2 := interval-from-date("2000-03-01", "2004-09-09")
        let $itv3 := interval-from-date("2006-08-01", "2007-03-01")
        let $itv4 := interval-from-date("2004-09-10", "2012-08-01")
        return {"interval-covers": interval-covers($itv1, $itv2), "interval-covered-by": interval-covered-by($itv3, $itv4)}
        
 * The expected result is:
 
        { "interval-covers": true, "interval-covered-by": true }


### interval-overlaps, interval-overlapped-by ###

 * Syntax:

        interval-overlaps(interval1, interval2)
        interval-overlapped-by(interval1, interval2)

 * These functions check whether two intervals overlap with each other.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
   
    A `boolean` value. Specifically, `interval-overlaps(interval1, interval2)` is true if and only if

        interval1.start < interval2.start
        AND interval2.end > interval1.end
        AND interval1.end > interval2.start

    `interval-overlapped-by(interval1, interval2)` is true if and only if

        interval2.start < interval1.start
        AND interval1.end > interval2.end
        AND interval2.end > interval1.start

    For all these functions, if any of the two inputs is `null`, `null` is returned.

    Note that `interval-overlaps` and `interval-overlapped-by` are following the Allen's relations on the definition of overlap.

 * Examples:

        let $itv1 := interval-from-date("2000-01-01", "2005-01-01")
        let $itv2 := interval-from-date("2004-05-01", "2012-09-09")
        let $itv3 := interval-from-date("2006-08-01", "2007-03-01")
        let $itv4 := interval-from-date("2004-09-10", "2006-12-31")
        return {"overlaps": interval-overlaps($itv1, $itv2), 
                "overlapped-by": interval-overlapped-by($itv3, $itv4)}
        
 * The expected result is:
 
        { "overlaps": true, "overlapped-by": true }


###  interval-overlapping ###
Note that `interval-overlapping` is not an Allen's Relation, but syntactic sugar we added for the case that the intersect of two intervals is not empty. Basically this function returns true if any of these functions return true: `interval-overlaps`, `interval-overlapped-by`, `interval-covers`, or `interval-covered-by`.

 * Syntax:

        interval-overlapping(interval1, interval2)

 * This functions check whether two intervals share any points with each other. 
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
   
    A `boolean` value. Specifically, `interval-overlapping(interval1, interval2)` is true if

        (interval2.start >= interval1.start
        AND interval2.start < interval1.end)
        OR
        (interval2.end > interval1.start
        AND interval2.end <= interval1.end)

    If any of the two inputs is `null`, `null` is returned.

 * Examples:

        let $itv1 := interval-from-date("2000-01-01", "2005-01-01")
        let $itv2 := interval-from-date("2004-05-01", "2012-09-09")
        let $itv3 := interval-from-date("2006-08-01", "2007-03-01")
        let $itv4 := interval-from-date("2004-09-10", "2006-12-31")
        return {"overlapping1": interval-overlapping($itv1, $itv2), 
                "overlapping2": interval-overlapping($itv3, $itv4)}
        
 * The expected result is:
 
        { "overlapping1": true, "overlapping2": true }


### interval-meets, interval-met-by ###

 * Syntax:

        interval-meets(interval1, interval2)
        interval-met-by(interval1, interval2)

 * These two functions check whether an interval meets with another interval. 
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
   
    A `boolean` value. Specifically, `interval-meets(interval1, interval2)` is true if and only if `interval1.end = interval2.start`, and `interval-met-by(interval1, interval2)` is true if and only if `interval1.start = interval2.end`. If any of the two inputs is `null`, `null` is returned.

 * Examples:

        let $itv1 := interval-from-date("2000-01-01", "2005-01-01")
        let $itv2 := interval-from-date("2005-01-01", "2012-09-09")
        let $itv3 := interval-from-date("2006-08-01", "2007-03-01")
        let $itv4 := interval-from-date("2004-09-10", "2006-08-01")
        return {"meets": interval-meets($itv1, $itv2), "metby": interval-met-by($itv3, $itv4)}

 * The expected result is:
 
        { "meets": true, "metby": true }


### interval-starts, interval-started-by ###

 * Syntax:

        interval-starts(interval1, interval2)
        interval-started-by(interval1, interval2)

 * These two functions check whether one interval starts with the other interval.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
   
    A `boolean` value. Specifically, `interval-starts(interval1, interval2)` returns true if and only if

        interval1.start = interval2.start
        AND interval1.end <= interval2.end

    `interval-started-by(interval1, interval2)` returns true if and only if

        interval1.start = interval2.start
        AND interval2.end <= interval1.end

    For both functions, if any of the two inputs is `null`, `null` is returned.

 * Examples:

        let $itv1 := interval-from-date("2000-01-01", "2005-01-01")
        let $itv2 := interval-from-date("2000-01-01", "2012-09-09")
        let $itv3 := interval-from-date("2006-08-01", "2007-03-01")
        let $itv4 := interval-from-date("2006-08-01", "2006-08-01")
        return {"interval-starts": interval-starts($itv1, $itv2), "interval-started-by": interval-started-by($itv3, $itv4)}

 * The expected result is:
 
        { "interval-starts": true, "interval-started-by": true }


### interval-ends, interval-ended-by ###

* Syntax:

        interval-ends(interval1, interval2)
        interval-ended-by(interval1, interval2)

 * These two functions check whether one interval ends with the other interval.
 * Arguments:
    * `interval1`, `interval2`: two intervals to be compared
 * Return Value:
   
    A `boolean` value. Specifically, `interval-ends(interval1, interval2)` returns true if and only if

        interval1.end = interval2.end
        AND interval1.start >= interval2.start

    `interval-ended-by(interval1, interval2)` returns true if and only if

        interval2.end = interval1.end
        AND interval2.start >= interval1.start

    For both functions, if any of the two inputs is `null`, `null` is returned.

* Examples:

        let $itv1 := interval-from-date("2000-01-01", "2005-01-01")
        let $itv2 := interval-from-date("1998-01-01", "2005-01-01")
        let $itv3 := interval-from-date("2006-08-01", "2007-03-01")
        let $itv4 := interval-from-date("2006-09-10", "2007-03-01")
        return {"interval-ends": interval-ends($itv1, $itv2), "interval-ended-by": interval-ended-by($itv3, $itv4) }
        
* The expected result is:

        { "interval-ends": true, "interval-ended-by": true }
