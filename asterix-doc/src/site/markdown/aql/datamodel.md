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

# Asterix Data Model (ADM) #

## <a id="toc">Table of Contents</a> ##

* [Primitive Types](#PrimitiveTypes)
    * [Boolean](#PrimitiveTypesBoolean)
    * [Int8 / Int16 / Int32 / Int64](#PrimitiveTypesInt)
    * [Float](#PrimitiveTypesFloat)
    * [Double](#PrimitiveTypesDouble)
    * [String](#PrimitiveTypesString)
    * [Point](#PrimitiveTypesPoint)
    * [Line](#PrimitiveTypesLine)
    * [Rectangle](#PrimitiveTypesRectangle)
    * [Circle](#PrimitiveTypesCircle)
    * [Polygon](#PrimitiveTypesPolygon)
    * [Date](#PrimitiveTypesDate)
    * [Time](#PrimitiveTypesTime)
    * [Datetime](#PrimitiveTypesDateTime)
    * [Duration/Year-month-duration/Day-time-duration](#PrimitiveTypesDuration)
    * [Interval](#PrimitiveTypesInterval)
    * [UUID](#PrimitiveTypesUUID)
* [Derived Types](#DerivedTypes)
    * [Record](#DerivedTypesRecord)
    * [OrderedList](#DerivedTypesOrderedList)
    * [UnorderedList](#DerivedTypesUnorderedList)

An instance of Asterix data model (ADM) can be a _*primitive type*_ (`int32`, `int64`, `string`, `float`, `double`, `date`, `time`, `datetime`, etc. or `null`) or a _*derived type*_.

## <a id="PrimitiveTypes">Primitive Types</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

### <a id="PrimitiveTypesBoolean">Boolean</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`boolean` data type can have one of the two values: _*true*_ or _*false*_.

 * Example:

        let $t := true
        let $f := false
        return { "true": $t, "false": $f }


 * The expected result is:

        { "true": true, "false": false }



### <a id="PrimitiveTypesInt">Int8 / Int16 / Int32 / Int64</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ###
Integer types using 8, 16, 32, or 64 bits. The ranges of these types are:

- `int8`: -127 to 127
- `int16`: -32767 to 32767
- `int32`: -2147483647 to 2147483647
- `int64`: -9223372036854775808 to 9223372036854775807

 * Example:

        let $v8 := int8("125")
        let $v16 := int16("32765")
        let $v32 := 294967295
        let $v64 := int64("1700000000000000000")
        return { "int8": $v8, "int16": $v16, "int32": $v32, "int64": $v64}


 * The expected result is:

        { "int8": 125i8, "int16": 32765i16, "int32": 294967295, "int64": 1700000000000000000i64 }


### <a id="PrimitiveTypesFloat">Float</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`float` represents approximate numeric data values using 4 bytes. The range of a float value can be from 2^(-149) to (2-2^(-23)·2^(127) for both positive and negative. Beyond these ranges will get `INF` or `-INF`.

 * Example:

        let $v1 := float("NaN")
        let $v2 := float("INF")
        let $v3 := float("-INF")
        let $v4 := float("-2013.5")
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4 }


 * The expected result is:

        { "v1": NaNf, "v2": Infinityf, "v3": -Infinityf, "v4": -2013.5f }


### <a id="PrimitiveTypesDouble">Double</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`double` represents approximate numeric data values using 8 bytes. The range of a double value can be from (2^(-1022)) to (2-2^(-52))·2^(1023) for both positive and negative. Beyond these ranges will get `INF` or `-INF`.

 * Example:

        let $v1 := double("NaN")
        let $v2 := double("INF")
        let $v3 := double("-INF")
        let $v4 := double("-2013.593823748327284")
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4 }


 * The expected result is:

        { "v1": NaNd, "v2": Infinityd, "v3": -Infinityd, "v4": -2013.5938237483274d }


### <a id="PrimitiveTypesString">String</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`string` represents a sequence of characters.

 * Example:

        let $v1 := string("This is a string.")
        let $v2 := string("\"This is a quoted string\"")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": "This is a string.", "v2": "\"This is a quoted string\"" }


### <a id="PrimitiveTypesPoint">Point</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`point` is the fundamental two-dimensional building block for spatial types. It consists of two `double` coordinates x and y.

 * Example:

        let $v1 := point("80.10d, -10E5")
        let $v2 := point("5.10E-10d, -10E5")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": point("80.1,-1000000.0"), "v2": point("5.1E-10,-1000000.0") }


### <a id="PrimitiveTypesLine">Line</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`line` consists of two points that represent the start and the end points of a line segment.

 * Example:

        let $v1 := line("10.1234,11.1e-1 +10.2E-2,-11.22")
        let $v2 := line("0.1234,-1.00e-10 +10.5E-2,-01.02")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": line("10.1234,1.11 0.102,-11.22"), "v2": line("0.1234,-1.0E-10 0.105,-1.02") }


### <a id="PrimitiveTypesRectangle">Rectangle</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`rectangle` consists of two points that represent the _*bottom left*_ and _*upper right*_ corners of a rectangle.

 * Example:

        let $v1 := rectangle("5.1,11.8 87.6,15.6548")
        let $v2 := rectangle("0.1234,-1.00e-10 5.5487,0.48765")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": rectangle("5.1,11.8 87.6,15.6548"), "v2": rectangle("0.1234,-1.0E-10 5.5487,0.48765") }


### <a id="PrimitiveTypesCircle">Circle</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`circle` consists of one point that represents the center of the circle and a radius of type `double`.

 * Example:

        let $v1 := circle("10.1234,11.1e-1 +10.2E-2")
        let $v2 := circle("0.1234,-1.00e-10 +10.5E-2")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": circle("10.1234,1.11 0.102"), "v2": circle("0.1234,-1.0E-10 0.105") }


### <a id="PrimitiveTypesPolygon">Polygon</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`polygon` consists of _*n*_ points that represent the vertices of a _*simple closed*_ polygon.

 * Example:

        let $v1 := polygon("-1.2,+1.3e2 -2.14E+5,2.15 -3.5e+2,03.6 -4.6E-3,+4.81")
        let $v2 := polygon("-1.0,+10.5e2 -02.15E+50,2.5 -1.0,+3.3e3 -2.50E+05,20.15 +3.5e+2,03.6 -4.60E-3,+4.75 -2,+1.0e2 -2.00E+5,20.10 30.5,03.25 -4.33E-3,+4.75")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": polygon("-1.2,130.0 -214000.0,2.15 -350.0,3.6 -0.0046,4.81"), "v2": polygon("-1.0,1050.0 -2.15E50,2.5 -1.0,3300.0 -250000.0,20.15 350.0,3.6 -0.0046,4.75 -2.0,100.0 -200000.0,20.1 30.5,3.25 -0.00433,4.75") }


### <a id="PrimitiveTypesDate">Date</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`date` represents a time point along the Gregorian calendar system specified by the year, month and day. ASTERIX supports the date from `-9999-01-01` to `9999-12-31`.

A date value can be represented in two formats, extended format and basic format.

 * Extended format is represented as `[-]yyyy-mm-dd` for `year-month-day`. Each field should be padded if there are less digits than the format specified.
 * Basic format is in the format of `[-]yyyymmdd`.

 * Example:

        let $v1 := date("2013-01-01")
        let $v2 := date("-19700101")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": date("2013-01-01"), "v2": date("-1970-01-01") }


### <a id="PrimitiveTypesTime">Time</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`time` type describes the time within the range of a day. It is represented by three fields: hour, minute and second. Millisecond field is optional as the fraction of the second field. Its extended format is as `hh:mm:ss[.mmm]` and the basic format is `hhmmss[mmm]`. The value domain is from `00:00:00.000` to `23:59:59.999`.

Timezone field is optional for a time value. Timezone is represented as `[+|-]hh:mm` for extended format or `[+|-]hhmm` for basic format. Note that the sign designators cannot be omitted. `Z` can also be used to represent the UTC local time. If no timezone information is given, it is UTC by default.

 * Example:

        let $v1 := time("12:12:12.039Z")
        let $v2 := time("000000000-0800")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": time("12:12:12.039Z"), "v2": time("08:00:00.000Z") }


### <a id="PrimitiveTypesDateTime">Datetime</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
A `datetime` value is a combination of an `date` and `time`, representing a fixed time point along the Gregorian calendar system. The value is among `-9999-01-01 00:00:00.000` and `9999-12-31 23:59:59.999`.

A `datetime` value is represented as a combination of the representation of its `date` part and `time` part, separated by a separator `T`. Either extended or basic format can be used, and the two parts should be the same format.

Millisecond field and timezone field are optional, as specified in the `time` type.

 * Example:

        let $v1 := datetime("2013-01-01T12:12:12.039Z")
        let $v2 := datetime("-19700101T000000000-0800")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": datetime("2013-01-01T12:12:12.039Z"), "v2": datetime("-1970-01-01T08:00:00.000Z") }


### <a id="PrimitiveTypesDuration">Duration/Year-month-duration/Day-time-duration</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`duration` represents a duration of time. A duration value is specified by integers on at least one of the following fields: year, month, day, hour, minute, second, and millisecond.

A duration value is in the format of `[-]PnYnMnDTnHnMn.mmmS`. The millisecond part (as the fraction of the second field) is optional, and when no millisecond field is used, the decimal point should also be absent.

Negative durations are also supported for the arithmetic operations between time instance types (`date`, `time` and `datetime`), and is used to roll the time back for the given duration. For example `date("2012-01-01") + duration("-P3D")` will return `date("2011-12-29")`.

There are also two sub-duration types, namely `year-month-duration` and `day-time-duration`. `year-month-duration` represents only the years and months of a duration, while `day-time-duration` represents only the day to millisecond fields. Different from the `duration` type, both these two subtypes are totally ordered, so they can be used for comparison and index construction.

Note that a canonical representation of the duration is always returned, regardless whether the duration is in the canonical representation or not from the user's input. More information about canonical representation can be found from [XPath dayTimeDuration Canonical Representation](http://www.w3.org/TR/xpath-functions/#canonical-dayTimeDuration) and [yearMonthDuration Canonical Representation](http://www.w3.org/TR/xpath-functions/#canonical-yearMonthDuration).

 * Example:

        let $v1 := duration("P100Y12MT12M")
        let $v2 := duration("-PT20.943S")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": duration("P101YT12M"), "v2": duration("-PT20.943S") }


### <a id="PrimitiveTypesInterval">Interval</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`interval` represents inclusive-exclusive ranges of time. It is defined by two time point values with the same temporal type(`date`, `time` or `datetime`).

 * Example:

        let $v1 := interval-from-date(date("2013-01-01"), date("20130505"))
        let $v2 := interval-from-time(time("00:01:01"), time("213901049+0800"))
        let $v3 := interval-from-datetime(datetime("2013-01-01T00:01:01"), datetime("20130505T213901049+0800"))
        return { "v1": $v1, "v2": $v2, "v3": $v3 }


 * The expected result is:

        { "v1": interval-date("2013-01-01, 2013-05-05"), "v2": interval-time("00:01:01.000Z, 13:39:01.049Z"), "v3": interval-datetime("2013-01-01T00:01:01.000Z, 2013-05-05T13:39:01.049Z") }

### <a id="PrimitiveTypesUUID">UUID</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
`uuid` represents a UUID value, which stands for Universally unique identifier. It is defined by a canonical format using hexadecimal text with inserted hyphen characters. (E.g.: 5a28ce1e-6a74-4201-9e8f-683256e5706f). This type is generally used to store auto-generated primary key values.

 * Example:

        let $v1 := uuid("5c848e5c-6b6a-498f-8452-8847a2957421")
        return { "v1":$v1 }


 * The expected result is:

        { "v1": uuid("5c848e5c-6b6a-498f-8452-8847a2957421") }


## <a id="DerivedTypes">Derived Types</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ##

### <a id="DerivedTypesRecord">Record</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
A `record` contains a set of ﬁelds, where each ﬁeld is described by its name and type. A record type is either open or closed. Open records can contain ﬁelds that are not part of the type deﬁnition, while closed records cannot. Syntactically, record constructors are surrounded by curly braces "{...}".

An example would be


        { "id": 213508, "name": "Alice Bob" }


### <a id="DerivedTypesOrderedList">OrderedList</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
An `orderedList` is a sequence of values for which the order is determined by creation or insertion. OrderedList constructors are denoted by brackets: "[...]".

An example would be


        ["alice", 123, "bob", null]


### <a id="DerivedTypesUnorderedList">UnorderedList</a><font size="4"><a href="#toc">[Back to TOC]</a></font> ###
An `unorderedList` is an unordered sequence of values, similar to bags in SQL. UnorderedList constructors are denoted by two opening flower braces followed by data and two closing flower braces, like "{{...}}".

An example would be


        {{"hello", 9328, "world", [1, 2, null]}}

