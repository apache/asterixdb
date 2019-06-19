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

## <a id="TemporalFunctions">Temporal Functions</a> ##

### get_year/get_month/get_day/get_hour/get_minute/get_second/get_millisecond ###
 * Syntax:

        get_year/get_month/get_day/get_hour/get_minute/get_second/get_millisecond(temporal_value)

 * Accessors for accessing fields in a temporal value
 * Arguments:
    * `temporal_value` : a temporal value represented as one of the following types: `date`, `datetime`, `time`, and `duration`.
 * Return Value:
    * an `bigint` value representing the field to be extracted,
    * `missing` if the argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-interval input value will cause a type error.

 * Example:

        {
          "year": get_year(date("2010-10-30")),
          "month": get_month(datetime("1987-11-19T23:49:23.938")),
          "day": get_day(date("2010-10-30")),
          "hour": get_hour(time("12:23:34.930+07:00")),
          "min": get_minute(duration("P3Y73M632DT49H743M3948.94S")),
          "second": get_second(datetime("1987-11-19T23:49:23.938")),
          "ms": get_millisecond(duration("P3Y73M632DT49H743M3948.94S"))
        };


 * The expected result is:

        { "year": 2010, "month": 11, "day": 30, "hour": 5, "min": 28, "second": 23, "ms": 94 }


### adjust_datetime_for_timezone ###
 * Syntax:

        adjust_datetime_for_timezone(datetime, string)

 * Adjusts the given datetime `datetime` by applying the timezone information `string`.
 * Arguments:
    * `datetime` : a `datetime` value to be adjusted.
    * `string` : a `string` representing the timezone information.
 * Return Value:
    * a `string` value representing the new datetime after being adjusted by the timezone information,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-datetime value,
        * or, the second argument is any other non-string value.

 * Example:

        adjust_datetime_for_timezone(datetime("2008-04-26T10:10:00"), "+08:00");


 * The expected result is:

        "2008-04-26T18:10:00.000+08:00"


### adjust_time_for_timezone ###
 * Syntax:

        adjust_time_for_timezone(time, string)

 * Adjusts the given time `time` by applying the timezone information `string`.
 * Arguments:
    * `time` : a `time` value to be adjusted.
    * `string` : a `string` representing the timezone information.
 * Return Value:
    * a `string` value representing the new time after being adjusted by the timezone information,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-time value,
        * or, the second argument is any other non-string value.

 * Example:

        adjust_time_for_timezone(get_time_from_datetime(datetime("2008-04-26T10:10:00")), "+08:00");


 * The expected result is:

        "18:10:00.000+08:00"


### calendar_duration_from_datetime ###
 * Syntax:

        calendar_duration_from_datetime(datetime, duration_value)

 * Gets a user_friendly representation of the duration `duration_value` based on the given datetime `datetime`.
 * Arguments:
    * `datetime` : a `datetime` value to be used as the reference time point.
    * `duration_value` : a `duration` value to be converted.
 * Return Value:
    * a `duration` value with the duration as `duration_value` but with a user_friendly representation,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-datetime value,
        * or, the second argument is any other non-duration input value.

 * Example:

        calendar_duration_from_datetime(
              datetime("2016-03-26T10:10:00"),
              datetime("2016-03-26T10:10:00") - datetime("2011-01-01T00:00:00")
        );

 * The expected result is:

        duration("P5Y2M24DT10H10M")


### get_year_month_duration/get_day_time_duration ###
 * Syntax:

        get_year_month_duration/get_day_time_duration(duration_value)

 * Extracts the correct `duration` subtype from `duration_value`.
 * Arguments:
    * `duration_value` : a `duration` value to be converted.
 * Return Value:
    * a `year_month_duration` value or a `day_time_duration` value,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-duration input value will cause a type error.

 * Example:

        get_year_month_duration(duration("P12M50DT10H"));


 * The expected result is:

        year_month_duration("P1Y")

### months_from_year_month_duration/ms_from_day_time_duration ###
* Syntax:

        months_from_year_month_duration/ms_from_day_time_duration(duration_value)

* Extracts the number of months or the number of milliseconds from the `duration` subtype.
* Arguments:
    * `duration_value` : a `duration` of the correct subtype.
* Return Value:
    * a `bigint` representing the number of months/milliseconds,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-duration input value will cause a type error.

* Example:

        {
            "months": months_from_year_month_duration(get_year_month_duration(duration("P5Y7MT50M"))),
            "milliseconds": ms_from_day_time_duration(get_day_time_duration(duration("P5Y7MT50M")))
        };

* The expected result is:

        {"months": 67, "milliseconds": 3000000}


### duration_from_months/duration_from_ms ###
* Syntax:

        duration_from_months/duration_from_ms(number_value)

* Creates a `duration` from `number_value`.
* Arguments:
    * `number_value` : a `bigint` representing the number of months/milliseconds
* Return Value:
    * a `duration` containing `number_value` value for months/milliseconds,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-duration input value will cause a type error.

* Example:

        duration_from_months(8);

* The expected result is:

        duration("P8M")


### duration_from_interval ###
* Syntax:

        duration_from_interval(interval_value)

* Creates a `duration` from `interval_value`.
* Arguments:
    * `interval_value` : an `interval` value
* Return Value:
    * a `duration` representing the time in the `interval_value`
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-duration input value will cause a type error.

* Example:

        {
          "dr1" : duration_from_interval(interval(date("2010-10-30"), date("2010-12-21"))),
          "dr2" : duration_from_interval(interval(datetime("2012-06-26T01:01:01.111"), datetime("2012-07-27T02:02:02.222"))),
          "dr3" : duration_from_interval(interval(time("12:32:38"), time("20:29:20"))),
          "dr4" : duration_from_interval(null)
        };

* The expected result is:

        {
          "dr1": day_time_duration("P52D"),
          "dr2": day_time_duration("P31DT1H1M1.111S"),
          "dr3": day_time_duration("PT7H56M42S"),
          "dr4": null
        }


### current_date ###
 * Syntax:

        current_date()

 * Gets the current date.
 * Arguments: None
 * Return Value:
    * a `date` value of the date when the function is called.

### current_time ###
 * Syntax:

        current_time()

 * Get the current time
 * Arguments: None
 * Return Value:
    * a `time` value of the time when the function is called.

### current_datetime ###
 * Syntax:

        current_datetime()

 * Get the current datetime
 * Arguments: None
 * Return Value:
    * a `datetime` value of the datetime when the function is called.


### get_date_from_datetime ###
 * Syntax:

        get_date_from_datetime(datetime)

 * Gets the date value from the given datetime value `datetime`.
 * Arguments:
    * `datetime`: a `datetime` value to be extracted from.
 * Return Value:
    * a `date` value from the datetime,
    * any other non-datetime input value will cause a type error.

### get_time_from_datetime ###
 * Syntax:

        get_time_from_datetime(datetime)

 * Get the time value from the given datetime value `datetime`
 * Arguments:
    * `datetime`: a `datetime` value to be extracted from.
 * Return Value:
    * a `time` value from the datetime.
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-datetime input value will cause a type error.

 * Example:

        get_time_from_datetime(datetime("2016-03-26T10:10:00"));

 * The expected result is:

        time("10:10:00.000Z")


### day_of_week ###
* Syntax:

        day_of_week(date)

* Finds the day of the week for a given date (1_7)
* Arguments:
    * `date`: a `date` value (Can also be a `datetime`)
* Return Value:
    * an `tinyint` representing the day of the week (1_7),
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-date input value will cause a type error.

* Example:

        day_of_week(datetime("2012-12-30T12:12:12.039Z"));


* The expected result is:

        7


### date_from_unix_time_in_days ###
 * Syntax:

        date_from_unix_time_in_days(numeric_value)

 * Gets a date representing the time after `numeric_value` days since 1970_01_01.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint` value representing the number of days.
 * Return Value:
    * a `date` value as the time after `numeric_value` days since 1970-01-01,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

### datetime_from_unix_time_in_ms ###
 * Syntax:

        datetime_from_unix_time_in_ms(numeric_value)

 * Gets a datetime representing the time after `numeric_value` milliseconds since 1970_01_01T00:00:00Z.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint` value representing the number of milliseconds.
 * Return Value:
    * a `datetime` value as the time after `numeric_value` milliseconds since 1970-01-01T00:00:00Z,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

### datetime_from_unix_time_in_secs ###
 * Syntax:

        datetime_from_unix_time_in_secs(numeric_value)

 * Gets a datetime representing the time after `numeric_value` seconds since 1970_01_01T00:00:00Z.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint` value representing the number of seconds.
 * Return Value:
    * a `datetime` value as the time after `numeric_value` seconds since 1970_01_01T00:00:00Z,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

### datetime_from_date_time ###
* Syntax:

datetime_from_date_time(date,time)

* Gets a datetime representing the combination of `date` and `time`
    * Arguments:
    * `date`: a `date` value
    * `time` a `time` value
* Return Value:
    * a `datetime` value by combining `date` and `time`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if
        * the first argument is any other non-date value,
        * or, the second argument is any other non-time value.

### time_from_unix_time_in_ms ###
 * Syntax:

        time_from_unix_time_in_ms(numeric_value)

 * Gets a time representing the time after `numeric_value` milliseconds since 00:00:00.000Z.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint` value representing the number of milliseconds.
 * Return Value:
    * a `time` value as the time after `numeric_value` milliseconds since 00:00:00.000Z,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        {
          "date": date_from_unix_time_in_days(15800),
          "datetime": datetime_from_unix_time_in_ms(1365139700000),
          "time": time_from_unix_time_in_ms(3748)
        };


 * The expected result is:

        { "date": date("2013-04-05"), "datetime": datetime("2013-04-05T05:28:20.000Z"), "time": time("00:00:03.748Z") }


### unix_time_from_date_in_days ###
 * Syntax:

        unix_time_from_date_in_days(date_value)

 * Gets an integer value representing the number of days since 1970_01_01 for `date_value`.
 * Arguments:
    * `date_value`: a `date` value.
 * Return Value:
    * a `bigint` value representing the number of days,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-date input value will cause a type error.


### unix_time_from_datetime_in_ms ###
 * Syntax:

        unix_time_from_datetime_in_ms(datetime_value)

 * Gets an integer value representing the time in milliseconds since 1970_01_01T00:00:00Z for `datetime_value`.
 * Arguments:
    * `datetime_value` : a `datetime` value.
 * Return Value:
    * a `bigint` value representing the number of milliseconds,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-datetime input value will cause a type error.


### unix_time_from_datetime_in_secs ###
 * Syntax:

        unix_time_from_datetime_in_secs(datetime_value)

 * Gets an integer value representing the time in seconds since 1970_01_01T00:00:00Z for `datetime_value`.
 * Arguments:
    * `datetime_value` : a `datetime` value.
 * Return Value:
    * a `bigint` value representing the number of seconds,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-datetime input value will cause a type error.


### unix_time_from_time_in_ms ###
 * Syntax:

        unix_time_from_time_in_ms(time_value)

 * Gets an integer value representing the time the milliseconds since 00:00:00.000Z for `time_value`.
 * Arguments:
    * `time_value` : a `time` value.
 * Return Value:
    * a `bigint` value representing the number of milliseconds,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-datetime input value will cause a type error.

 * Example:

        {
          "date": date_from_unix_time_in_days(15800),
          "datetime": datetime_from_unix_time_in_ms(1365139700000),
          "time": time_from_unix_time_in_ms(3748)
        }


 * The expected result is:

        { "date": date("2013-04-05"), "datetime": datetime("2013-04-05T05:28:20.000Z"), "time": time("00:00:03.748Z") }


### parse_date/parse_time/parse_datetime ###
* Syntax:

parse_date/parse_time/parse_datetime(date,formatting_expression)

* Creates a `date/time/date_time` value by treating `date` with formatting `formatting_expression`
* Arguments:
    * `date`: a `string` value representing the `date/time/datetime`.
    * `formatting_expression` a `string` value providing the formatting for `date_expression`.Characters used to create date expression:
       * `h` hours
       * `m` minutes
       * `s` seconds
       * `n` milliseconds
       * `a` am/pm
       * `z` timezone
       * `Y` year
       * `M` month
       * `D` day
       * `W` weekday
       * `_`, `'`, `/`, `.`, `,`, `T` seperators for both time and date
* Return Value:
    * a `date/time/date_time` value corresponding to `date`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
       * the first argument is any other non-date value,
       * the second argument is any other non-string value.

* Example:

        parse_time("30:30","m:s");

* The expected result is:

        time("00:30:30.000Z")


### print_date/print_time/print_datetime ###
* Syntax:

        print_date/print_time/print_datetime(date,formatting_expression)

* Creates a `string` representing a `date/time/date_time` value of the `date` using the formatting `formatting_expression`
* Arguments:
    * `date`: a `date/time/datetime` value.
    * `formatting_expression` a `string` value providing the formatting for `date_expression`. Characters used to create date expression:
       * `h` hours
       * `m` minutes
       * `s` seconds
       * `n` milliseconds
       * `a` am/pm
       * `z` timezone
       * `Y` year
       * `M` month
       * `D` day
       * `W` weekday
       * `_`, `'`, `/`, `.`, `,`, `T` seperators for both time and date
* Return Value:
    * a `string` value corresponding to `date`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
         * the first argument is any other non-date value,
         * the second argument is any other non-string value.

* Example:

        print_time(time("00:30:30.000Z"),"m:s");

* The expected result is:

        "30:30"


### get_interval_start, get_interval_end ###
 * Syntax:

        get_interval_start/get_interval_end(interval)

 * Gets the start/end of the given interval.
 * Arguments:
    * `interval`: the interval to be accessed.
 * Return Value:
    * a `time`, `date`, or `datetime` (depending on the time instances of the interval) representing the starting
     or ending time,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-interval value will cause a type error.

 * Example:

        {
          "start": get_interval_start(interval_start_from_date("1984-01-01", "P1Y")),
          "end": get_interval_end(interval_start_from_date("1984-01-01", "P1Y"))
        };


 * The expected result is:

        { "start": date("1984_01_01"), "end": date("1985_01_01") }


### get_interval_start_date/get_interval_start_datetimeget_interval_start_time, get_interval_end_date/get_interval_end_datetime/get_interval_end_time ###
 * Syntax:

        get_interval_start_date/get_interval_start_datetime/get_interval_start_time/get_interval_end_date/get_interval_end_datetime/get_interval_end_time(interval)

 * Gets the start/end of the given interval for the specific date/datetime/time type.
 * Arguments:
    * `interval`: the interval to be accessed.
 * Return Value:
    * a `time`, `date`, or `datetime` (depending on the function) representing the starting or ending time,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-interval value will cause a type error.

 * Example:

        {
          "start1": get_interval_start_date(interval_start_from_date("1984-01-01", "P1Y")),
          "end1": get_interval_end_date(interval_start_from_date("1984-01-01", "P1Y")),
          "start2": get_interval_start_datetime(interval_start_from_datetime("1984-01-01T08:30:00.000", "P1Y1H")),
          "end2": get_interval_end_datetime(interval_start_from_datetime("1984-01-01T08:30:00.000", "P1Y1H")),
          "start3": get_interval_start_time(interval_start_from_time("08:30:00.000", "P1H")),
          "end3": get_interval_end_time(interval_start_from_time("08:30:00.000", "P1H"))
        };


 * The expected result is:

        {
          "start1": date("1984-01-01"),
          "end1": date("1985-01-01"),
          "start2": datetime("1984-01-01T08:30:00.000Z"),
          "end2": datetime("1985-01-01T09:30:00.000Z"),
          "start3": time("08:30:00.000Z"),
          "end3": time("09:30:00.000Z")
        }


### get_overlapping_interval ###
 * Syntax:

        get_overlapping_interval(interval1, interval2)

 * Gets the start/end of the given interval for the specific date/datetime/time type.
 * Arguments:
    * `interval1`: an `interval` value
    * `interval2`: an `interval` value
 * Return Value:
    * an `interval` that is overlapping `interval1` and `interval2`.
      If `interval1` and `interval2` do not overlap `null` is returned. Note each interval must be of the same type.
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-interval input value will cause a type error.

 * Example:

        { "overlap1": get_overlapping_interval(interval(time("11:23:39"), time("18:27:19")), interval(time("12:23:39"), time("23:18:00"))),
          "overlap2": get_overlapping_interval(interval(time("12:23:39"), time("18:27:19")), interval(time("07:19:39"), time("09:18:00"))),
          "overlap3": get_overlapping_interval(interval(date("1980-11-30"), date("1999-09-09")), interval(date("2013-01-01"), date("2014-01-01"))),
          "overlap4": get_overlapping_interval(interval(date("1980-11-30"), date("2099-09-09")), interval(date("2013-01-01"), date("2014-01-01"))),
          "overlap5": get_overlapping_interval(interval(datetime("1844-03-03T11:19:39"), datetime("2000-10-30T18:27:19")), interval(datetime("1989-03-04T12:23:39"), datetime("2009-10-10T23:18:00"))),
          "overlap6": get_overlapping_interval(interval(datetime("1989-03-04T12:23:39"), datetime("2000-10-30T18:27:19")), interval(datetime("1844-03-03T11:19:39"), datetime("1888-10-10T23:18:00")))
        };

 * The expected result is:

        { "overlap1": interval(time("12:23:39.000Z"), time("18:27:19.000Z")),
          "overlap2": null,
          "overlap3": null,
          "overlap4": interval(date("2013-01-01"), date("2014_01_01")),
          "overlap5": interval(datetime("1989-03-04T12:23:39.000Z"), datetime("2000-10-30T18:27:19.000Z")),
          "overlap6": null
        }

### interval_bin ###
 * Syntax:

        interval_bin(time_to_bin, time_bin_anchor, duration_bin_size)

 * Returns the `interval` value representing the bin containing the `time_to_bin` value.
 * Arguments:
    * `time_to_bin`: a date/time/datetime value representing the time to be binned.
    * `time_bin_anchor`: a date/time/datetime value representing an anchor of a bin starts. The type of this argument should be the same as the first `time_to_bin` argument.
    * `duration_bin_size`: the duration value representing the size of the bin, in the type of year_month_duration or day_time_duration. The type of this duration should be compatible with the type of `time_to_bin`, so that the arithmetic operation between `time_to_bin` and `duration_bin_size` is well_defined. Currently AsterixDB supports the following arithmetic operations:
        * datetime +|_ year_month_duration
        * datetime +|_ day_time_duration
        * date +|_ year_month_duration
        * date +|_ day_time_duration
        * time +|_ day_time_duration
  * Return Value:
    * a `interval` value representing the bin containing the `time_to_bin` value. Note that the internal type of
      this interval value should be the same as the `time_to_bin` type,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument or the second argument is any other non-date/non-time/non-datetime value,
        * or, the second argument is any other non-year_month_duration/non-day_time_duration value.

  * Example:

        {
          "bin1": interval_bin(date("2010-10-30"), date("1990-01-01"), year_month_duration("P1Y")),
          "bin2": interval_bin(datetime("1987-11-19T23:49:23.938"), datetime("1990-01-01T00:00:00.000Z"), year_month_duration("P6M")),
          "bin3": interval_bin(time("12:23:34.930+07:00"), time("00:00:00"), day_time_duration("PT1M")),
          "bin4": interval_bin(datetime("1987-11-19T23:49:23.938"), datetime("2013-01-01T00:00:00.000"), day_time_duration("PT24H"))
        };

   * The expected result is:

        {
          "bin1": interval(date("2010-01-01"),date("2011-01-01")),
          "bin2": interval(datetime("1987-07-01T00:00:00.000Z"), datetime("1988-01-01T00:00:00.000Z")),
          "bin3": interval(time("05:23:00.000Z"), time("05:24:00.000Z")),
          "bin4": interval(datetime("1987-11-19T00:00:00.000Z"), datetime("1987-11-20T00:00:00.000Z"))
        }


### interval_start_from_date/time/datetime ###
 * Syntax:

        interval_start_from_date/time/datetime(date/time/datetime, duration)

 * Construct an `interval` value by the given starting `date`/`time`/`datetime` and the `duration` that the interval lasts.
 * Arguments:
    * `date/time/datetime`: a `string` representing a `date`, `time` or `datetime`, or a `date`/`time`/`datetime` value, representing the starting time point.
    * `duration`: a `string` or `duration` value representing the duration of the interval. Note that duration cannot be negative value.
 * Return Value:
    * an `interval` value representing the interval starting from the given time point with the length of duration,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
         * the first argument or the second argument is any other non-date/non-time/non-datetime value,
         * or, the second argument is any other non-duration value.

 * Example:

        {
          "interval1": interval_start_from_date("1984-01-01", "P1Y"),
          "interval2": interval_start_from_time(time("02:23:28.394"), "PT3H24M"),
          "interval3": interval_start_from_datetime("1999-09-09T09:09:09.999", duration("P2M30D"))
        };

 * The expectecd result is:

        {
          "interval1": interval(date("1984-01-01"), date("1985-01-01")),
          "interval2": interval(time("02:23:28.394Z"), time("05:47:28.394Z")),
          "interval3": interval(datetime("1999-09-09T09:09:09.999Z"), datetime("1999-12-09T09:09:09.999Z"))
        }


### overlap_bins ###
  * Return Value:
    * a `interval` value representing the bin containing the `time_to_bin` value. Note that the internal type of this interval value should be the same as the `time_to_bin` type.

 * Syntax:

        overlap_bins(interval, time_bin_anchor, duration_bin_size)

 * Returns an ordered list of `interval` values representing each bin that is overlapping the `interval`.
 * Arguments:
    * `interval`: an `interval` value
    * `time_bin_anchor`: a date/time/datetime value representing an anchor of a bin starts. The type of this argument should be the same as the first `time_to_bin` argument.
    * `duration_bin_size`: the duration value representing the size of the bin, in the type of year_month_duration or day_time_duration. The type of this duration should be compatible with the type of `time_to_bin`, so that the arithmetic operation between `time_to_bin` and `duration_bin_size` is well_defined. Currently AsterixDB supports the following arithmetic operations:
        * datetime +|_ year_month_duration
        * datetime +|_ day_time_duration
        * date +|_ year_month_duration
        * date +|_ day_time_duration
        * time +|_ day_time_duration
  * Return Value:
    * a ordered list of `interval` values representing each bin that is overlapping the `interval`.
      Note that the internal type as `time_to_bin` and `duration_bin_size`.
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
     * a type error will be raised if:
           * the first arugment is any other non-interval value,
           * or, the second argument is any other non-date/non-time/non-datetime value,
           * or, the second argument is any other non-year_month_duration/non-day_time_duration value.

  * Example:

        {
          "timebins": overlap_bins(interval(time("17:23:37"), time("18:30:21")), time("00:00:00"), day_time_duration("PT30M")),
          "datebins": overlap_bins(interval(date("1984-03-17"), date("2013-08-22")), date("1990-01-01"), year_month_duration("P10Y")),
          "datetimebins": overlap_bins(interval(datetime("1800-01-01T23:59:48.938"), datetime("2015-07-26T13:28:30.218")),
                                      datetime("1900-01-01T00:00:00.000"), year_month_duration("P100Y"))
        };

   * The expected result is:

        {
          "timebins": [
                        interval(time("17:00:00.000Z"), time("17:30:00.000Z")),
                        interval(time("17:30:00.000Z"), time("18:00:00.000Z")),
                        interval(time("18:00:00.000Z"), time("18:30:00.000Z")),
                        interval(time("18:30:00.000Z"), time("19:00:00.000Z"))
                      ],
          "datebins": [
                        interval(date("1980-01-01"), date("1990-01-01")),
                        interval(date("1990-01-01"), date("2000-01-01")),
                        interval(date("2000-01-01"), date("2010-01-01")),
                        interval(date("2010-01-01"), date("2020-01-01"))
                      ],
          "datetimebins": [
                            interval(datetime("1800-01-01T00:00:00.000Z"), datetime("1900-01-01T00:00:00.000Z")),
                            interval(datetime("1900-01-01T00:00:00.000Z"), datetime("2000-01-01T00:00:00.000Z")),
                            interval(datetime("2000-01-01T00:00:00.000Z"), datetime("2100-01-01T00:00:00.000Z"))
                           ]
        };

