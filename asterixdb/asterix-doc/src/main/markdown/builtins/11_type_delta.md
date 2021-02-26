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

### is_binary (is_bin) ###
 * Syntax:

        is_binary(expr)

 * Checks whether the given expression is evaluated to be a `binary` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `binary` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_binary(true),
          "b": is_binary(false),
          "c": isbinary(null),
          "d": isbinary(missing),
          "e": isbin(point("1,2")),
          "f": isbin(hex("ABCDEF0123456789")),
          "g": is_bin(sub_binary(hex("AABBCCDD"), 4)),
          "h": is_bin(2),
          "i": is_bin({"a":1})
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": false, "f": true, "g": true, "h": false, "i": false }

 The function has three aliases: `isbinary`, `is_bin`, and `isbin`.

### is_uuid ###
 * Syntax:

        is_uuid(expr)

 * Checks whether the given expression is evaluated to be a `uuid` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `uuid` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

         {
          "a": is_uuid(true),
          "b": is_uuid(false),
          "c": is_uuid(null),
          "d": is_uuid(missing),
          "e": isuuid(4.0),
          "f": isuuid(date("2013-01-01")),
          "g": isuuid(uuid("5c848e5c-6b6a-498f-8452-8847a2957421"))
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": false, "f": false, "g": true }

 The function has an alias `isuuid`.

### is_point ###
 * Syntax:

        is_point(expr)

 * Checks whether the given expression is evaluated to be a `point` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `point` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_point(true),
          "b": is_point(false),
          "c": is_point(null),
          "d": is_point(missing),
          "e": is_point(point("1,2")),
          "f": ispoint(line("30.0,70.0 50.0,90.0")),
          "g": ispoint(rectangle("30.0,70.0 50.0,90.0")),
          "h": ispoint(circle("30.0,70.0 5.0")),
          "i": ispoint(polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0")),
          "j": ispoint(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": true, "f": false, "g": false, "h": false, "i": false, "j": false }

 The function has an alias `ispoint`.

### is_line ###
 * Syntax:

        is_line(expr)

 * Checks whether the given expression is evaluated to be a `line` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `line` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_line(true),
          "b": is_line(false),
          "c": is_line(null),
          "d": is_line(missing),
          "e": is_line(point("1,2")),
          "f": isline(line("30.0,70.0 50.0,90.0")),
          "g": isline(rectangle("30.0,70.0 50.0,90.0")),
          "h": isline(circle("30.0,70.0 5.0")),
          "i": isline(polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0")),
          "j": isline(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": false, "f": true, "g": false, "h": false, "i": false, "j": false }

 The function has an alias `isline`.
 
### is_rectangle ###
 * Syntax:

        is_rectangle(expr)

 * Checks whether the given expression is evaluated to be a `rectangle` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `rectangle` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_rectangle(true),
          "b": is_rectangle(false),
          "c": is_rectangle(null),
          "d": is_rectangle(missing),
          "e": is_rectangle(point("1,2")),
          "f": isrectangle(line("30.0,70.0 50.0,90.0")),
          "g": isrectangle(rectangle("30.0,70.0 50.0,90.0")),
          "h": isrectangle(circle("30.0,70.0 5.0")),
          "i": isrectangle(polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0")),
          "j": isrectangle(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": false, "f": false, "g": true, "h": false, "i": false, "j": false }

 The function has an alias `isrectangle`.
 
### is_circle ###
 * Syntax:

        is_circle(expr)

 * Checks whether the given expression is evaluated to be a `circle` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `circle` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_circle(true),
          "b": is_circle(false),
          "c": is_circle(null),
          "d": is_circle(missing),
          "e": is_circle(point("1,2")),
          "f": iscircle(line("30.0,70.0 50.0,90.0")),
          "g": iscircle(rectangle("30.0,70.0 50.0,90.0")),
          "h": iscircle(circle("30.0,70.0 5.0")),
          "i": iscircle(polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0")),
          "j": iscircle(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": false, "f": false, "g": false, "h": true, "i": false, "j": false }

 The function has an alias `iscircle`.
 
### is_polygon ###
 * Syntax:

        is_polygon(expr)

 * Checks whether the given expression is evaluated to be a `polygon` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `polygon` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_polygon(true),
          "b": is_polygon(false),
          "c": is_polygon(null),
          "d": is_polygon(missing),
          "e": is_polygon(point("1,2")),
          "f": ispolygon(line("30.0,70.0 50.0,90.0")),
          "g": ispolygon(rectangle("30.0,70.0 50.0,90.0")),
          "h": ispolygon(circle("30.0,70.0 5.0")),
          "i": ispolygon(polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0")),
          "j": ispolygon(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": false, "f": false, "g": false, "h": false, "i": true, "j": false }

 The function has an alias `ispolygon`.
 
### is_spatial ###
 * Syntax:

        is_spatial(expr)

 * Checks whether the given expression is evaluated to be a spatial value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `point`/`line`/`rectangle`/`circle`/`polygon` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_spatial(true),
          "b": is_spatial(false),
          "c": is_spatial(null),
          "d": is_spatial(missing),
          "e": is_spatial(point("1,2")),
          "f": isspatial(line("30.0,70.0 50.0,90.0")),
          "g": isspatial(rectangle("30.0,70.0 50.0,90.0")),
          "h": isspatial(circle("30.0,70.0 5.0")),
          "i": isspatial(polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0")),
          "j": isspatial(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": true, "f": true, "g": true, "h": true, "i": true, "j": false }

 The function has an alias `isspatial`.

### is_date ###
 * Syntax:

        is_date(expr)

 * Checks whether the given expression is evaluated to be a `date` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `date` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_date(true),
          "b": is_date(false),
          "c": is_date(null),
          "d": is_date(missing),
          "e": is_date(date("-19700101")),
          "f": isdate(date("2013-01-01")),
          "g": isdate(time("12:12:12.039Z")),
          "h": isdate(datetime("2013-01-01T12:12:12.039Z")),
          "i": isdate(duration("P100Y12MT12M")),
          "j": isdate(interval(date("2013-01-01"), date("20130505"))),
          "k": isdate(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": true, "f": true, "g": false, "h": false, "i": false, "j": false, "k": false }

 The function has an alias `isdate`.
 
### is_datetime (is_timestamp) ###
 * Syntax:

        is_datetime(expr)

 * Checks whether the given expression is evaluated to be a `datetime` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `datetime` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

        {
          "a": is_datetime(true),
          "b": is_datetime(false),
          "c": is_datetime(null),
          "d": is_datetime(missing),
          "e": is_datetime(datetime("2016-02-02T12:09:22.023Z")),
          "f": isdatetime(datetime("2011-03-03T12:10:42.011Z")),
          "g": isdatetime(time("12:12:12.039Z")),
          "h": is_timestamp(datetime("2013-01-01T12:12:12.039Z")),
          "i": is_timestamp(duration("P100Y12MT12M")),
          "j": istimestamp(interval(date("2013-01-01"), date("20130505"))),
          "k": istimestamp(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": true, "f": true, "g": false, "h": true, "i": false, "j": false, "k": false }

 The function has three aliases: `isdatetime`, `is_timestamp`, and `istimestamp`.
 
### is_time ###
 * Syntax:

        is_time(expr)

 * Checks whether the given expression is evaluated to be a `time` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `time` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

         {
          "a": is_time(true),
          "b": is_time(false),
          "c": is_time(null),
          "d": is_time(missing),
          "e": is_time(time("08:00:00.000Z")),
          "f": istime(date("2013-01-01")),
          "g": istime(time("12:12:12.039Z")),
          "h": istime(datetime("2013-01-01T12:12:12.039Z")),
          "i": istime(duration("P100Y12MT12M")),
          "j": istime(interval(date("2013-01-01"), date("20130505"))),
          "k": istime(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": true, "f": false, "g": true, "h": false, "i": false, "j": false, "k": false }

 The function has an alias `istime`.
 
### is_duration ###
 * Syntax:

        is_duration(expr)

 * Checks whether the given expression is evaluated to be a duration value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `duration/year_month_duration/day_time_duration` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

         {
          "a": is_duration(true),
          "b": is_duration(false),
          "c": is_duration(null),
          "d": is_duration(missing),
          "e": is_duration(duration("-PT20.943S")),
          "f": isduration(date("2013-01-01")),
          "g": isduration(time("12:12:12.039Z")),
          "h": isduration(datetime("2013-01-01T12:12:12.039Z")),
          "i": isduration(duration("P100Y12MT12M")),
          "j": isduration(interval(date("2013-01-01"), date("20130505"))),
          "k": isduration(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": true, "f": false, "g": false, "h": false, "i": true, "j": false, "k": false }

 The function has an alias `isduration`.
 
### is_interval ###
 * Syntax:

        is_interval(expr)

 * Checks whether the given expression is evaluated to be a `interval` value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `interval` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

         {
          "a": is_interval(true),
          "b": is_interval(false),
          "c": is_interval(null),
          "d": is_interval(missing),
          "e": is_interval(interval(datetime("2013-01-01T00:01:01.000Z"), datetime("2013-05-05T13:39:01.049Z"))),
          "f": isinterval(date("2013-01-01")),
          "g": isinterval(time("12:12:12.039Z")),
          "h": isinterval(datetime("2013-01-01T12:12:12.039Z")),
          "i": isinterval(duration("P100Y12MT12M")),
          "j": isinterval(interval(date("2013-01-01"), date("20130505"))),
          "k": isinterval(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": true, "f": false, "g": false, "h": false, "i": false, "j": true, "k": false }

 The function has an alias `isinterval`.
 
### is_temporal ###
 * Syntax:

        is_temporal(expr)

 * Checks whether the given expression is evaluated to be a temporal value.
 * Arguments:
    * `expr` : an expression (any type is allowed).
 * Return Value:
    * a `boolean` on whether the argument is a `date/datetime/time/duration/year_month_duration/day_time_duration/interval` value or not,
    * a `missing` if the argument is a `missing` value,
    * a `null` if the argument is a `null` value.

 * Example:

         {
          "a": is_temporal(true),
          "b": is_temporal(false),
          "c": is_temporal(null),
          "d": is_temporal(missing),
          "e": is_temporal(duration("-PT20.943S")),
          "f": istemporal(date("2013-01-01")),
          "g": istemporal(time("12:12:12.039Z")),
          "h": istemporal(datetime("2013-01-01T12:12:12.039Z")),
          "i": istemporal(duration("P100Y12MT12M")),
          "j": istemporal(interval(date("2013-01-01"), date("20130505"))),
          "k": istemporal(3)
        };


 * The expected result is:

        { "a": false, "b": false, "c": null, "e": true, "f": true, "g": true, "h": true, "i": true, "j": true, "k": false }

 The function has an alias `istemporal`.

### get_type ###
 * Syntax:

        get_type(expr)

 * Returns a string describing the type of the given `expr`. This includes incomplete information types (i.e. `missing` and `null`).
 * Arguments:
    * `expr` : an expression (any type is allowed).

 * Example:

        {
          "a": get_type(true),
          "b": get_type(false),
          "c": get_type(null),
          "d": get_type(missing),
          "e": get_type("d"),
          "f": gettype(4.0),
          "g": gettype(5),
          "h": gettype(["1", 2]),
          "i": gettype({"a":1})
        };


 * The expected result is:

        { "a": "boolean", "b": "boolean", "c": "null", "d": "missing", "e": "string", "f": "double", "g": "bigint", "h": "array", "i": "object" }
        
 The function has an alias `gettype`.

