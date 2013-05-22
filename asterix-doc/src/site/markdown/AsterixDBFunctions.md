<!--
# Asterix Data Model (ADM) #

# Basic data types #

An instance of Asterix data model (ADM) can be a _primitive type_ (`Int32`, `Int64`, `String`, `Float`, `Double`, `Date`, `Time`, `Datetime`, etc. or `NULL`) or a _derived type_.

## Primitive Types ##

### Boolean ###
`Boolean` data type can have one of the two values: _*true*_ or _*false*_.

 * Example:

        let $t := true
        let $f := false
        return { "true": $t, "false": $f }


 * The expected result is:

        { "true": true, "false": false }



### Int8 / Int16 / Int32 / Int64 ###
Integer types using 8, 16, 32, or 64 bits.

 * Example:

        let $v8 := int8("125")
        let $v16 := int16("32765")
        let $v32 := 294967295
        let $v64 := int64("1700000000000000000")
        return { "int8": $v8, "int16": $v16, "int32": $v32, "int64": $v64}


 * The expected result is:

        { "int8": 125i8, "int16": 32765i16, "int32": 294967295, "int64": 1700000000000000000i64 }


### Float ###
`Float` represents approximate numeric data values using 4 bytes.

 * Example:

        let $v1 := float("NaN")
        let $v2 := float("INF")
        let $v3 := float("-INF")
        let $v4 := float("-2013.5")
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4 }


 * The expected result is:

        { "v1": NaNf, "v2": Infinityf, "v3": -Infinityf, "v4": -2013.5f }


### Double ###
`Double` represents approximate numeric data values using 8 bytes.

 * Example:

        let $v1 := double("NaN")
        let $v2 := double("INF")
        let $v3 := double("-INF")
        let $v4 := double("-2013.593823748327284")
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4 }


 * The expected result is:

        { "v1": NaNd, "v2": Infinityd, "v3": -Infinityd, "v4": -2013.5938237483274d }


### String ###
`String` represents a sequence of characters.

 * Example:

        let $v1 := string("This is a string.")
        let $v2 := string("\"This is a quoted string\"")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": "This is a string.", "v2": "\"This is a quoted string\"" }


### Point ###
`Point` is the fundamental two-dimensional building block for spatial types. It consists of two `double` coordinates x and y.

 * Example:

        let $v1 := point("80.10d, -10E5")
        let $v2 := point("5.10E-10d, -10E5")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": point("80.1,-1000000.0"), "v2": point("5.1E-10,-1000000.0") }


### Line ###
`Line` consists of two points that represent the start and the end points of a line segment.

 * Example:

        let $v1 := line("10.1234,11.1e-1 +10.2E-2,-11.22")
        let $v2 := line("0.1234,-1.00e-10 +10.5E-2,-01.02")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": line("10.1234,1.11 0.102,-11.22"), "v2": line("0.1234,-1.0E-10 0.105,-1.02") }


### Rectangle ###
`Rectangle` consists of two points that represent the _*bottom left*_ and _*upper right*_ corners of a rectangle.

 * Example:

        let $v1 := rectangle("5.1,11.8 87.6,15.6548")
        let $v2 := rectangle("0.1234,-1.00e-10 5.5487,0.48765")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": rectangle("5.1,11.8 87.6,15.6548"), "v2": rectangle("0.1234,-1.0E-10 5.5487,0.48765") }


### Circle ###
`Circle` consists of one `point` that represents the center of the circle and a radius of type `double`.

 * Example:

        let $v1 := circle("10.1234,11.1e-1 +10.2E-2")
        let $v2 := circle("0.1234,-1.00e-10 +10.5E-2")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": circle("10.1234,1.11 0.102"), "v2": circle("0.1234,-1.0E-10 0.105") }


### Polygon ###
`Polygon` consists of _*n*_ points that represent the vertices of a _*simple closed*_ polygon.

 * Example:

        let $v1 := polygon("-1.2,+1.3e2 -2.14E+5,2.15 -3.5e+2,03.6 -4.6E-3,+4.81")
        let $v2 := polygon("-1.0,+10.5e2 -02.15E+50,2.5 -1.0,+3.3e3 -2.50E+05,20.15 +3.5e+2,03.6 -4.60E-3,+4.75 -2,+1.0e2 -2.00E+5,20.10 30.5,03.25 -4.33E-3,+4.75")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": polygon("-1.2,130.0 -214000.0,2.15 -350.0,3.6 -0.0046,4.81"), "v2": polygon("-1.0,1050.0 -2.15E50,2.5 -1.0,3300.0 -250000.0,20.15 350.0,3.6 -0.0046,4.75 -2.0,100.0 -200000.0,20.1 30.5,3.25 -0.00433,4.75") }


### Date ###
`Date` represents a time point along the Gregorian calendar system specified by the year, month and day. ASTERIX supports the date from `-9999-01-01` to `9999-12-31`.

A date value can be represented in two formats, extended format and basic format.

 * Extended format is represented as `[-]yyyy-mm-dd` for `year-month-day`. Each field should be padded if there are less digits than the format specified.
 * Basic format is in the format of `[-]yyyymmdd`.

 * Example:

        let $v1 := date("2013-01-01")
        let $v2 := date("-19700101")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": date("2013-01-01"), "v2": date("-1970-01-01") }


### Time ###
`Time` type describes the time within the range of a day. It is represented by three fields: hour, minute and second. Millisecond field is optional as the fraction of the second field. Its extended format is as `hh:mm:ss[.mmm]` and the basic format is `hhmmss[mmm]`. The value domain is from `00:00:00.000` to `23:59:59.999`.

Timezone field is optional for a time value. Timezone is represented as `[+|-]hh:mm` for extended format or `[+|-]hhmm` for basic format. Note that the sign designators cannot be omitted. `Z` can also be used to represent the UTC local time. If no timezone information is given, it is UTC by default.

 * Example:

        let $v1 := time("12:12:12.039Z")
        let $v2 := time("000000000-0800")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": time("12:12:12.039Z"), "v2": time("08:00:00.000Z") }


### Datetime ###
A `Datetime` value is a combination of an `Date` and `Time`, representing a fixed time point along the Gregorian calendar system. The value is among `-9999-01-01 00:00:00.000` and `9999-12-31 23:59:59.999`.

A `Datetime` value is represented as a combination of the representation of its `Date` part and `Time` part, separated by a separator `T`. Either extended or basic format can be used, and the two parts should be the same format.

Millisecond field and timezone field are optional, as specified in the `Time` type.

 * Example:

        let $v1 := datetime("2013-01-01T12:12:12.039Z")
        let $v2 := datetime("-19700101T000000000-0800")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": datetime("2013-01-01T12:12:12.039Z"), "v2": datetime("-1970-01-01T08:00:00.000Z") }


### Duration ###
`Duration` represents a duration of time. A duration value is specified by integers on at least one of the following fields: year, month, day, hour, minute, second, and millisecond.

A duration value is in the format of `[-]PnYnMnDTnHnMn.mmmS`. The millisecond part (as the fraction of the second field) is optional, and when no millisecond field is used, the decimal point should also be absent.

Negative durations are also supported for the arithmetic operations between time instance types (`Date`, `Time` and `Datetime`), and is used to roll the time back for the given duration. For example `date("2012-01-01") + duration("-P3D")` will return `date("2011-12-29")`.

Note that a canonical representation of the duration is always returned, regardless whether the duration is in the canonical representation or not from the user's input. More information about canonical representation can be found from [XPath dayTimeDuration Canonical Representation](http://www.w3.org/TR/xpath-functions/#canonical-dayTimeDuration) and [yearMonthDuration Canonical Representation](http://www.w3.org/TR/xpath-functions/#canonical-yearMonthDuration).

 * Example:

        let $v1 := duration("P100Y12MT12M")
        let $v2 := duration("-PT20.943S")
        return { "v1": $v1, "v2": $v2 }


 * The expected result is:

        { "v1": duration("P101YT12M"), "v2": duration("-PT20.943S") }


### Interval ###
`Interval` represents inclusive-exclusive ranges of time. It is defined by two time point values with the same temporal type(`Date`, `Time` or `Datetime`).

 * Example:

        let $v1 := interval-from-date(date("2013-01-01"), date("20130505"))
        let $v2 := interval-from-time(time("00:01:01"), time("213901049+0800"))
        let $v3 := interval-from-datetime(datetime("2013-01-01T00:01:01"), datetime("20130505T213901049+0800"))
        return { "v1": $v1, "v2": $v2, "v3": $v3 }


 * The expected result is:

        { "v1": interval-date("2013-01-01, 2013-05-05"), "v2": interval-time("00:01:01.000Z, 13:39:01.049Z"), "v3": interval-datetime("2013-01-01T00:01:01.000Z, 2013-05-05T13:39:01.049Z") }


## Derived Types ##

### Record ###
A `Record` contains a set of ﬁelds, where each ﬁeld is described by its name and type. A record type is either open or closed. Open records can contain ﬁelds that are not part of the type deﬁnition, while closed records cannot. Syntactically, record constructors are surrounded by curly braces "{...}".

An example would be


        { "id": 213508, "name": "Alice Bob" }


### OrderedList ###
An `OrderedList` is a sequence of values for which the order is determined by creation or insertion. OrderedList constructors are denoted by brackets: "[...]".

An example would be


        ["alice", 123, "bob", null]


### UnorderedList ###
An `UnorderedList` is an unordered sequence of values, similar to bags in SQL. UnorderedList constructors are denoted by two opening flower braces followed by data and two closing flower braces, like "{{...}}".

An example would be


        {{"hello", 9328, "world", [1, 2, null]}}

-->
# Asterix: Using Functions #
Asterix provides rich support of various classes of functions to support operations on string, spatial, and temporal data.  This document explains how to use these functions.

## String Functions ##
### string-to-codepoint ###
 * Syntax:

        string-to-codepoint(string_expression)

 * Converts the string `string_expression` to its code-based representation.
 * Arguments:
   * `string_expression` : A `String` that will be converted.
 * Return Value:
   * An `OrderedList` of the code points for the string `string_expression`.

### codepoint-to-string ###
 * Syntax:

        codepoint-to-string(list_expression)

 * Converts the ordered code-based representation `list_expression` to the corresponding string.
 * Arguments:
   * `list_expression` : An `OrderedList` of code-points.
 * Return Value:
   * A `String` representation of `list_expression`.

 * Example:

        use dataverse TinySocial;
        
        let $s := "Hello ASTERIX!"
        let $l := string-to-codepoint($s)
        let $ss := codepoint-to-string($l)
        return {"codes": $l, "string": $ss}


 * The expected result is:

        { "codes": [ 72, 101, 108, 108, 111, 32, 65, 83, 84, 69, 82, 73, 88, 33 ], "string": "Hello ASTERIX!" }


### contains ###
 * Syntax:

        contains(string_expression, string_pattern)

 * Checks whether the string `string_expression` contains the string `string_pattern`
 * Arguments:
   * `string_expression` : A `String` that might contain the pattern.
   * `string_pattern` : A target `String` that might be contained.
 * Return Value:
   * A `Boolean`, returns `true` if `string_expression` contains `string_pattern`, otherwise returns `false`.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        where contains($i.message, "phone")
        return {"mid": $i.message-id, "message": $i.message}


 * The expected result is:

        { "mid": 2, "message": " dislike iphone its touch-screen is horrible" }
        { "mid": 13, "message": " dislike iphone the voice-command is bad:(" }
        { "mid": 15, "message": " like iphone the voicemail-service is awesome" }


### len ###
 * Syntax:

        len(list_expression)

 * Returns the length of the list `list_expression`.
 * Arguments:
   * `list_expression` : An `OrderedList`, `UnorderedList` or `NULL`, represents the list need to be checked.
 * Return Value:
   * An `Int32` that represents the length of `list_expression`.

 * Example:

        use dataverse TinySocial;
        
        let $l := ["ASTERIX", "Hyracks"]
        return len($l)


 * The expected result is:

        2


### like ###
 * Syntax:

        like(string_expression, string_pattern)

 * Checks whether the string `string_expression` contains the string `string_pattern`.
 * Arguments:
   * `string_expression` : A `String` that might contain the pattern or `NULL`.
   * `string_pattern` : A pattern `String` that might be contained or `NULL`.
 * Return Value:
   * A `Boolean`, returns `true` if `string_expression` contains the pattern `string_pattern`, otherwise returns `false`.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        where like($i.message, "%at&t%")
        return $i.message


 * The expected result is:

        " can't stand at&t the network is horrible:("
        " can't stand at&t its plan is terrible"
        " love at&t its 3G is good:)"


### starts-with ###
 * Syntax:

        starts-with(string_expression, string_pattern)

 * Checks whether the string `string_expression` starts with the string `string_pattern`.
 * Arguments:
   * `string_expression` : A `String` that might start with the given string.
   * `string_pattern` : A `String` that might be contained as the starting substring.
 * Return Value:
   * A `Boolean`, returns `true` if `string_expression` starts with the string `string_pattern`, otherwise returns `false`.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        where starts-with($i.message, " like")
        return $i.message


 * The expected result is:

        " like samsung the plan is amazing"
        " like t-mobile its platform is mind-blowing"
        " like verizon the 3G is awesome:)"
        " like iphone the voicemail-service is awesome"


### ends-with ###
 * Syntax:

        ends-with(string_expression, string_pattern)

 * Checks whether the string `string_expression` ends with the string `string_pattern`.
 * Arguments:
   * `string_expression` : A `String` that might end with the given string.
   * `string_pattern` : A `String` that might be contained as the ending substring.
 * Return Value:
   * A `Boolean`, returns `true` if `string_expression` ends with the string `string_pattern`, otherwise returns `false`.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        where ends-with($i.message, ":)")
        return $i.message


 * The expected result is:

        " love sprint its shortcut-menu is awesome:)"
        " like verizon the 3G is awesome:)"
        " love at&t its 3G is good:)"


### string-concat ###
 * Syntax:

        string-concat(list_expression)

 * Concatenates a list of strings `list_expression` into a single string.
 * Arguments:
   * `list_expression` : An `OrderedList` or `UnorderedList` of `String`s (could be `NULL`) to be concatenated.
 * Return Value:
   * Returns the concatenated `String` value.

 * Example:

        let $i := "ASTERIX"
        let $j := " "
        let $k := "ROCKS!"
        return string-concat([$i, $j, $k])


 * The expected result is:

        "ASTERIX ROCKS!"


### string-equal ###
 * Syntax:

        string-equal(string_expression1, string_expression2)

 * Checks whether the strings `string_expression1` and `string_expression2` are equal.
 * Arguments:
   * `string_expression1` : A `String` to be compared.
   * `string_expression2` : A `String` to be compared with.
 * Return Value:
   * A `Boolean`, returns `true` if `string_expression1` and `string_expression2` are equal, otherwise returns `false`.

 * Example:

        use dataverse TinySocial;
        
        let $i := "Android"
        return {"Equal": string-equal($i, "Android"), "NotEqual": string-equal($i, "iphone")}


 * The expected result is:

        { "Equal": true, "NotEqual": false }


### string-join ###
 * Syntax:

        string-join(list_expression, string_expression)

 * Joins a list of strings `list_expression` with the given separator `string_expression` into a single string.
 * Arguments:
   * `list_expression` : An `OrderedList` or `UnorderedList` of `String`s (could be NULL) to be joined.
   * `string_expression` : A separator `String` value.
 * Return Value:
   * Returns the joined `String`.

 * Example:

        use dataverse TinySocial;
        
        let $i := ["ASTERIX", "ROCKS~"]
        return string-join($i, "!! ")


 * The expected result is:

        "ASTERIX!! ROCKS~"


### lowercase ###
 * Syntax:

        lowercase(string_expression)

 * Returns the lowercase of a given string `string_expression`.
 * Arguments:
   * `string_expression` : A `String` to be lowercased.
 * Return Value:
   * Returns the lowercased `String`.

 * Example:

        use dataverse TinySocial;
        
        let $i := "ASTERIX"
        return lowercase($i)


 * The expected result is:

        asterix


### matches ###
 * Syntax:

        matches(string_expression, string_pattern)

 * Checks whether the strings `string_expression` matches the given pattern `string_pattern`.
 * Arguments:
   * `string_expression` : A `String` that might contain the pattern.
   * `string_pattern` : A pattern `String` to be matched.
 * Return Value:
   * A `Boolean`, returns `true` if `string_expression` matches the pattern `string_pattern`, otherwise returns `false`.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        where matches($i.message, "dislike iphone")
        return $i.message


 * The expected result is:

        " dislike iphone its touch-screen is horrible"
        " dislike iphone the voice-command is bad:("


### replace ###
 * Syntax:

        replace(string_expression, string_pattern, string_replacement)

 * Checks whether the strings `string_expression` matches the given pattern `string_pattern`, and replace the matched pattern `string_pattern` with the new pattern `string_replacement`.
 * Arguments:
   * `string_expression` : A `String` that might contain the pattern.
   * `string_pattern` : A pattern `String` to be matched.
   * `string_replacement` : A pattern `String` to be used as the replacement.
 * Return Value:
   * Returns a `String` that is obtained after the replacements.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        where matches($i.message, " like iphone")
        return replace($i.message, " like iphone", "like android")


 * The expected result is:

        "like android the voicemail-service is awesome"


### string-length ###
 * Syntax:

        string-length(string_expression)

 * Returns the length of the string `string_expression`.
 * Arguments:
   * `string_expression` : A `String` or `NULL`, represents the string to be checked.
 * Return Value:
   * An `Int32` that represents the length of `string_expression`.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        return {"mid": $i.message-id, "message-len": string-length($i.message)}


 * The expected result is:

        { "mid": 1, "message-len": 43 }
        { "mid": 2, "message-len": 44 }
        { "mid": 3, "message-len": 33 }
        { "mid": 4, "message-len": 43 }
        { "mid": 5, "message-len": 46 }
        { "mid": 6, "message-len": 43 }
        { "mid": 7, "message-len": 37 }
        { "mid": 8, "message-len": 33 }
        { "mid": 9, "message-len": 34 }
        { "mid": 10, "message-len": 50 }
        { "mid": 11, "message-len": 38 }
        { "mid": 12, "message-len": 52 }
        { "mid": 13, "message-len": 42 }
        { "mid": 14, "message-len": 27 }
        { "mid": 15, "message-len": 45 }


### substring ###
 * Syntax:

        substring(string_expression, offset, length)

 * Returns the substring from the given string `string_expression` based on the given start offset `offset`.
 * Arguments:
   * `string_expression` : A `String` as the string to be extracted.
   * `offset` : An `Int32` as the starting offset of the substring in `string_expression`.
   * `length` : (Optional) An `Int32` as the length of the substring.
 * Return Value:
   * A `String` that represents the substring.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        where string-length($i.message) > 50
        return substring($i.message, 50)


 * The expected result is:

        "G:("


### substring-before ###
 * Syntax:

        substring-before(string_expression, string_pattern)

 * Returns the substring from the given string `string_expression` before the given pattern `string_pattern`.
 * Arguments:
   * `string_expression` : A `String` as the string to be extracted.
   * `string_pattern` : A `String` as the string pattern to be searched.
 * Return Value:
   * A `String` that represents the substring.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        where contains($i.message, "iphone")
        return substring-before($i.message, "iphone")


 * The expected result is:

        " dislike "
        " dislike "
        " like "


### substring-after ###
 * Syntax:

        substring-after(string_expression, string_pattern)

 * Returns the substring from the given string `string_expression` after the given pattern `string_pattern`.
 * Arguments:
   * `string_expression` : A `String` as the string to be extracted.
   * `string_pattern` : A `String` as the string pattern to be searched.
 * Return Value:
   * A `String` that represents the substring.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookMessages')
        where contains($i.message, "iphone")
        return substring-after($i.message, "iphone")


 * The expected result is:

        " its touch-screen is horrible"
        " the voice-command is bad:("
        " the voicemail-service is awesome"


## Spatial Functions ##
### create-point ###
 * Syntax:

        create-point(latitude, longitude)

 * Creates the primitive type `Point` using `latitude` and `longitude`.
 * Arguments:
   * `latitude` : A `Double` that represents the latitude.
   * `longitude` : A `Double` that represents the longitude.
 * Return Value:
   * A `Point`, represents a spatial point created using the latitude and longitude provided in `latitude` and `longitude`.

 * Example:

        use dataverse TinySocial;
        
        let $c :=  create-point(30.0,70.0)
        return {"point": $c}


 * The expected result is:

        { "point": point("30.0,70.0") }


### create-line ###
 * Syntax:

        create-line(point_expression1, point_expression2)

 * Creates the primitive type `Line` using `point_expression1` and `point_expression2`.
 * Arguments:
   * `point_expression1` : A `Point` that represents the start point of the line.
   * `point_expression2` : A `Point` that represents the end point of the line.
 * Return Value:
   * A `Line`, represents a spatial line created using the points provided in `point_expression1` and `point_expression2`.

 * Example:

        use dataverse TinySocial;
        
        let $c :=  create-line(create-point(30.0,70.0), create-point(50.0,90.0))
        return {"line": $c}


 * The expected result is:

        { "line": line("30.0,70.0 50.0,90.0") }


### create-rectangle ###
 * Syntax:

        create-rectangle(point_expression1, point_expression2)

 * Creates the primitive type Rectangle using `point_expression1` and `point_expression2`.
 * Arguments:
   * `point_expression1` : A `Point` that represents the lower-left point of the rectangle.
   * `point_expression2` : A `Point` that represents the upper-right point of the rectangle.
 * Return Value:
   * A `Rectangle`, represents a spatial rectangle created using the points provided in `point_expression1` and `point_expression2`.

 * Example:

        use dataverse TinySocial;
        
        let $c :=  create-rectangle(create-point(30.0,70.0), create-point(50.0,90.0))
        return {"rectangle": $c}


 * The expected result is:

        { "rectangle": rectangle("30.0,70.0 50.0,90.0") }


### create-circle ###
 * Syntax:

        create-circle(point_expression, radius)

 * Creates the primitive type `Circle` using `point_expression` and `radius`.
 * Arguments:
   * `point_expression` : A `Point` that represents the center of the circle.
   * `radius` : A `Double` that represents the radius of the circle.
 * Return Value:
   * A `Circle`, represents a spatial circle created using the center point and the radius provided in `point_expression` and `radius`.

 * Example:

        use dataverse TinySocial;
        
        let $c :=  create-circle(create-point(30.0,70.0), 5.0)
        return {"circle": $c}


 * The expected result is:

        { "circle": circle("30.0,70.0 5.0") }


### create-polygon ###
 * Syntax:

        create-polygon(point_expression1, point_expression2, ..., point_expressionn)

 * Creates the primitive type `Polygon` using unlimited number of arguments `point_expression1`, `point_expression2`, ..., `point_expressionn`.
 * Arguments:
   * `point_expression1`/.../`point_expressionn` : A `Point` that represents a vertex of the polygon.
 * Return Value:
   * A `Polygon`, represents a spatial simple polygon created using the points provided in `point_expression1`, `point_expression2`, ..., `point_expressionn`.

 * Example:

        use dataverse TinySocial;
        
        let $c :=  create-polygon(create-point(1.0,1.0), create-point(2.0,2.0), create-point(3.0,3.0), create-point(4.0,4.0))
        return {"polygon": $c}


 * The expected result is:

        { "polygon": polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0") }


### point ###
 * Syntax:

        point(string_expression)

 * Constructor function for `Point` type by parsing a point string `string_expression`
 * Arguments:
   * `string_expression` : The `String` value representing a point value.
 * Return Value:
   * A `Point` value represented by the given string.

 * Example:

        
        use dataverse TinySocial;
        
        let $c := point("55.05,-138.04")
        return {"point": $c}


 * The expected result is:

        { "point": point("55.05,-138.04") }


### line ###
 * Syntax:

        line(string_expression)

 * Constructor function for `Line` type by parsing a line string `string_expression`
 * Arguments:
   * `string_expression` : The `String` value representing a line value.
 * Return Value:
   * A `Line` value represented by the given string.

 * Example:

        
        use dataverse TinySocial;
        
        let $c := line("55.05,-138.04 13.54,-138.04")
        return {"line": $c}


 * The expected result is:

        { "line": line("55.05,-138.04 13.54,-138.04") }


### rectangle ###
 * Syntax:

        rectangle(string_expression)

 * Constructor function for `Rectangle` type by parsing a rectangle string `string_expression`
 * Arguments:
   * `string_expression` : The `String` value representing a rectangle value.
 * Return Value:
   * A `Rectangle` value represented by the given string.

 * Example:

        
        use dataverse TinySocial;
        
        let $c := rectangle("20.05,-125.0 40.67,-100.87")
        return {"rectangle": $c}


 * The expected result is:

        { "rectangle": rectangle("20.05,-125.0 40.67,-100.87") }


### circle ###
 * Syntax:

        circle(string_expression)

 * Constructor function for `Circle` type by parsing a circle string `string_expression`
 * Arguments:
   * `string_expression` : The `String` value representing a circle value.
 * Return Value:
   * A `Circle` value represented by the given string.

 * Example:

        
        use dataverse TinySocial;
        
        let $c := circle("55.05,-138.04 10.0")
        return {"circle": $c}


 * The expected result is:

        { "circle": circle("55.05,-138.04 10.0") }


### polygon ###
 * Syntax:

        polygon(string_expression)

 * Constructor function for `Polygon` type by parsing a polygon string `string_expression`
 * Arguments:
   * `string_expression` : The `String` value representing a polygon value.
 * Return Value:
   * A `Polygon` value represented by the given string.

 * Example:

        
        use dataverse TinySocial;
        
        let $c := polygon("55.05,-138.04 13.54,-138.04 13.54,-53.31 55.05,-53.31")
        return {"polygon": $c}


 * The expected result is:

        { "polygon": polygon("55.05,-138.04 13.54,-138.04 13.54,-53.31 55.05,-53.31") }


### get-x/get-y ###
 * Syntax:

        get-x(point_expression) or get-y(point_expression)

 * Returns the x or y coordinates of a point `point_expression`.
 * Arguments:
   * `point_expression` : A `Point`.
 * Return Value:
   * A `Double`, represents the x or y coordinates of the point `point_expression`.

 * Example:

        use dataverse TinySocial;
        
        let $point := create-point(2.3,5.0)
        return {"x-coordinate": get-x($point), "y-coordinate": get-y($point)}


 * The expected result is:

        { "x-coordinate": 2.3d, "y-coordinate": 5.0d }


### get-points ###
 * Syntax:

        get-points(spatial_expression)

 * Returns an ordered list of the points forming the spatial object `spatial_expression`.
 * Arguments:
   * `spatial_expression` : A `Point`, `Line`, `Rectangle`, `Circle`, or `Polygon`.
 * Return Value:
   * An `OrderedList` of the points forming the spatial object `spatial_expression`.

 * Example:

        use dataverse TinySocial;
        
        let $line := create-line(create-point(100.6,99.4), create-point(-72.0,-76.9))
        let $rectangle := create-rectangle(create-point(9.2,49.0), create-point(77.8,111.1))
        let $polygon := create-polygon(create-point(1.0,1.0), create-point(2.0,2.0), create-point(3.0,3.0), create-point(4.0,4.0))
        let $line_list := get-points($line)
        let $rectangle_list := get-points($rectangle)
        let $polygon_list := get-points($polygon)
        return {"line-first-point": $line_list[0], "line-second-point": $line_list[1], "rectangle-left-bottom-point": $rectangle_list[0], "rectangle-top-upper-point": $rectangle_list[1], "polygon-first-point": $polygon_list[0], "polygon-second-point": $polygon_list[1], "polygon-third-point": $polygon_list[2], "polygon-forth-point": $polygon_list[3]}


 * The expected result is:

        { "line-first-point": point("100.6,99.4"), "line-second-point": point("-72.0,-76.9"), "rectangle-left-bottom-point": point("9.2,49.0"), "rectangle-top-upper-point": point("77.8,111.1"), "polygon-first-point": point("1.0,1.0"), "polygon-second-point": point("2.0,2.0"), "polygon-third-point": point("3.0,3.0"), "polygon-forth-point": point("4.0,4.0") }


### get-center/get-radius ###
 * Syntax:

        get-center(circle_expression) or get-radius(circle_expression)

 * Returns the center and the radius of a circle `circle_expression`.
 * Arguments:
   * `circle_expression` : A `Circle`.
 * Return Value:
   * A `Point` or `Double`, represent the center or radius of the circle `circle_expression`.

 * Example:

        use dataverse TinySocial;
        
        let $circle := create-circle(create-point(6.0,3.0), 1.0)
        return {"circle-radius": get-radius($circle), "circle-center": get-center($circle)}
        


 * The expected result is:

        { "circle-radius": 1.0d, "circle-center": point("6.0,3.0") }



### spatial-distance ###
 * Syntax:

        spatial-distance(point_expression1, point_expression2)

 * Returns the euclidean distance between `point_expression1` and `point_expression2`.
 * Arguments:
   * `point_expression1` : A `Point`.
   * `point_expression2` : A `Point`.
 * Return Value:
   * A `Double`, represents the euclidean distance between `point_expression1` and `point_expression2`.

 * Example:

        use dataverse TinySocial;
        
        for $t in dataset('TweetMessages')
        let $d :=  spatial-distance($t.sender-location, create-point(30.0,70.0))
        return {"point": $t.sender-location, "distance": $d}
        


 * The expected result is:

        { "point": point("47.44,80.65"), "distance": 20.434678857275934d }
        { "point": point("29.15,76.53"), "distance": 6.585089217315132d }
        { "point": point("37.59,68.42"), "distance": 7.752709203884797d }
        { "point": point("24.82,94.63"), "distance": 25.168816023007512d }
        { "point": point("32.84,67.14"), "distance": 4.030533463451212d }
        { "point": point("29.72,75.8"), "distance": 5.806754687430835d }
        { "point": point("39.28,70.48"), "distance": 9.292405501268227d }
        { "point": point("40.09,92.69"), "distance": 24.832321679617472d }
        { "point": point("47.51,83.99"), "distance": 22.41250097601782d }
        { "point": point("36.21,72.6"), "distance": 6.73231758015024d }
        { "point": point("46.05,93.34"), "distance": 28.325926286707734d }
        { "point": point("36.86,74.62"), "distance": 8.270671073135482d }


### spatial-area ###
 * Syntax:

        spatial-distance(spatial_2d_expression)

 * Returns the spatial area of `spatial_2d_expression`.
 * Arguments:
   * `spatial_2d_expression` : A `Rectangle`, `Circle`, or `Polygon`.
 * Return Value:
   * A `Double`, represents the area of `spatial_2d_expression`.

 * Example:

        use dataverse TinySocial;
        
        let $circleArea := spatial-area(create-circle(create-point(0.0,0.0), 5.0))
        return {"Area":$circleArea}
        


 * The expected result is:

        { "Area": 78.53981625d }


### spatial-intersect ###
 * Syntax:

        spatial-intersect(spatial_expression1, spatial_expression2)

 * Checks whether `@arg1` and `@arg2` spatially intersect each other.
 * Arguments:
   * `spatial_expression1` : A `Point`, `Line`, `Rectangle`, `Circle`, or `Polygon`.
   * `spatial_expression2` : A `Point`, `Line`, `Rectangle`, `Circle`, or `Polygon`.
 * Return Value:
   * A `Boolean`, represents whether `spatial_expression1` and `spatial_expression2` spatially intersect each other.

 * Example:

        use dataverse TinySocial;
        
        for $t in dataset('TweetMessages')
        where spatial-intersect($t.sender-location, create-rectangle(create-point(30.0,70.0), create-point(40.0,80.0)))
        return $t


 * The expected result is:

        { "tweetid": "4", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("39.28,70.48"), "send-time": datetime("2011-12-26T10:10:00.000Z"), "referred-topics": {{ "sprint", "voice-command" }}, "message-text": " like sprint the voice-command is mind-blowing:)" }
        { "tweetid": "7", "user": { "screen-name": "ChangEwing_573", "lang": "en", "friends_count": 182, "statuses_count": 394, "name": "Chang Ewing", "followers_count": 32136 }, "sender-location": point("36.21,72.6"), "send-time": datetime("2011-08-25T10:10:00.000Z"), "referred-topics": {{ "samsung", "platform" }}, "message-text": " like samsung the platform is good" }
        { "tweetid": "9", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("36.86,74.62"), "send-time": datetime("2012-07-21T10:10:00.000Z"), "referred-topics": {{ "verizon", "voicemail-service" }}, "message-text": " love verizon its voicemail-service is awesome" }


### spatial-cell ###
 * Syntax:

        spatial-cell(point_expression1, point_expression2, x_increment, y_increment)

 * Returns the grid cell that `point_expression1` belongs to.
 * Arguments:
   * `point_expression1` : A `Point`, represents the point of interest that its grid cell will be returned.
   * `point_expression2` : A `Point`, represents the origin of the grid.
   * `x_increment` : A `Double`, represents X increments.
   * `y_increment` : A `Double`, represents Y increments.
 * Return Value:
   * A `Rectangle`, represents the grid cell that `point_expression1` belongs to.

 * Example:

        use dataverse TinySocial;
        
        for $t in dataset('TweetMessages')
        group by $c :=  spatial-cell($t.sender-location, create-point(20.0,50.0), 5.5, 6.0) with $t
        let $num :=  count($t)
        return { "cell": $c, "count": $num}


 * The expected result is:

        { "cell": rectangle("20.0,92.0 25.5,98.0"), "count": 1 }
        { "cell": rectangle("25.5,74.0 31.0,80.0"), "count": 2 }
        { "cell": rectangle("31.0,62.0 36.5,68.0"), "count": 1 }
        { "cell": rectangle("31.0,68.0 36.5,74.0"), "count": 1 }
        { "cell": rectangle("36.5,68.0 42.0,74.0"), "count": 2 }
        { "cell": rectangle("36.5,74.0 42.0,80.0"), "count": 1 }
        { "cell": rectangle("36.5,92.0 42.0,98.0"), "count": 1 }
        { "cell": rectangle("42.0,80.0 47.5,86.0"), "count": 1 }
        { "cell": rectangle("42.0,92.0 47.5,98.0"), "count": 1 }
        { "cell": rectangle("47.5,80.0 53.0,86.0"), "count": 1 }




## Similarity Functions ##

AsterixDB supports queries with different similarity functions, including edit distance and Jaccard.

### edit-distance ###
 * Syntax:

        edit-distance(expression1, expression2)

 * Returns the [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) of `expression1` and `expression2`.
 * Arguments:
   * `expression1` : A `String` or a homogeneous `OrderedList` of a comparable item type.
   * `expression2` : The same type as `expression1`.
 * Return Value:
   * An `Int32` that represents the edit-distance similarity of `expression1` and `expression2`.

 * Example:

        use dataverse TinySocial;
        
        for $user in dataset('FacebookUsers')
        let $ed := edit-distance($user.name, "Suzanna Tilson")
        where $ed <= 2
        return $user


 * The expected result is:

        {
        "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "user-since": datetime("2012-08-07T10:10:00.000Z"), "friend-ids": {{ 6 }},
        "employment": [ { "organization-name": "Labzatron", "start-date": date("2011-04-19"), "end-date": null } ]
        }


### edit-distance-check ###
 * Syntax:

        edit-distance-check(expression1, expression2, threshold)

 * Checks whether `expression1` and `expression2` have a [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) `<= threshold`.  The “check” version of edit distance is faster than the "non-check" version because the former can detect whether two items satisfy a given similarity threshold using early-termination techniques, as opposed to computing their real distance. Although possible, it is not necessary for the user to write queries using the “check” versions explicitly, since a rewrite rule can perform an appropriate transformation from a “non-check” version to a “check” version.

 * Arguments:
   * `expression1` : A `String` or a homogeneous `OrderedList` of a comparable item type.
   * `expression2` : The same type as `expression1`.
   * `threshold` : An `Int32` that represents the distance threshold.
 * Return Value:
   * An `OrderedList` with two items:
     * The first item contains a `Boolean` value representing whether `expression1` and `expression2` are similar.
     * The second item contains an `Int32` that represents the edit distance of `expression1` and `expression2` if it is `<= `threshold`, or 0 otherwise.

 * Example:

        use dataverse TinySocial;
        
        for $user in dataset('FacebookUsers')
        let $ed := edit-distance-check($user.name, "Suzanna Tilson", 2)
        where $ed[0]
        return $ed[1]


 * The expected result is:

        2


### similarity-jaccard ###
 * Syntax:

        similarity-jaccard(list_expression1, list_expression2)

 * Returns the [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index) of `list_expression1` and `list_expression2`.
 * Arguments:
   * `list_expression1` : An `UnorderedList` or `OrderedList`.
   * `list_expression2` : An `UnorderedList` or `OrderedList`.
 * Return Value:
   * A `Float` that represents the Jaccard similarity of `list_expression1` and `list_expression2`.

 * Example:

        use dataverse TinySocial;
        
        for $user in dataset('FacebookUsers')
        let $sim := similarity-jaccard($user.friend-ids, [1,5,9])
        where $sim >= 0.6f
        return $user


 * The expected result is:

        {
        "id": 3, "alias": "Emory", "name": "EmoryUnk", "user-since": datetime("2012-07-10T10:10:00.000Z"), "friend-ids": {{ 1, 5, 8, 9 }},
        "employment": [ { "organization-name": "geomedia", "start-date": date("2010-06-17"), "end-date": date("2010-01-26") } ]
        }
        {
        "id": 10, "alias": "Bram", "name": "BramHatch", "user-since": datetime("2010-10-16T10:10:00.000Z"), "friend-ids": {{ 1, 5, 9 }},
        "employment": [ { "organization-name": "physcane", "start-date": date("2007-06-05"), "end-date": date("2011-11-05") } ]
        }


### similarity-jaccard-check ###
 * Syntax:

        similarity-jaccard-check(list_expression1, list_expression2, threshold)

 * Checks whether `list_expression1` and `list_expression2` have a [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index) `>= threshold`.  Again, the “check” version of Jaccard is faster than the "non-check" version.

 * Arguments:
   * `list_expression1` : An `UnorderedList` or `OrderedList`.
   * `list_expression2` : An `UnorderedList` or `OrderedList`.
   * `threshold` : A Float that represents the similarity threshold.
 * Return Value:
   * An `OrderedList` with two items:
     * The first item contains a `Boolean` value representing whether `list_expression1` and `list_expression2` are similar.
     * The second item contains a `Float` that represents the Jaccard similarity of `list_expression1` and `list_expression2` if it is >`= `threshold`, or 0 otherwise.

 * Example:

        use dataverse TinySocial;
        
        for $user in dataset('FacebookUsers')
        let $sim := similarity-jaccard-check($user.friend-ids, [1,5,9], 0.6f)
        where $sim[0]
        return $sim[1]


 * The expected result is:

        0.75f
        1.0f


### Similarity Operator ~# ###
 * "`~=`" is syntactic sugar for expressing a similarity condition with a given similarity threshold.
 * The similarity function and threshold for "`~=`" are controlled via "set" directives.
 * The "`~=`" operator returns a Boolean that represents whether the operands are similar.

 * Example for Jaccard similarity:

        use dataverse TinySocial;
        
        set simfunction "jaccard";
        set simthreshold "0.6f";
        
        for $user in dataset('FacebookUsers')
        where $user.friend-ids ~= [1,5,9]
        return $user


 * The expected result is:

        {
        "id": 3, "alias": "Emory", "name": "EmoryUnk", "user-since": datetime("2012-07-10T10:10:00.000Z"), "friend-ids": {{ 1, 5, 8, 9 }},
        "employment": [ { "organization-name": "geomedia", "start-date": date("2010-06-17"), "end-date": date("2010-01-26") } ]
        }
        {
        "id": 10, "alias": "Bram", "name": "BramHatch", "user-since": datetime("2010-10-16T10:10:00.000Z"), "friend-ids": {{ 1, 5, 9 }},
        "employment": [ { "organization-name": "physcane", "start-date": date("2007-06-05"), "end-date": date("2011-11-05") } ]
        }


 * Example for edit-distance similarity:

        use dataverse TinySocial;
        
        set simfunction "edit-distance";
        set simthreshold "2";
        
        for $user in dataset('FacebookUsers')
        where $user.name ~= "Suzanna Tilson"
        return $user


 * The expected output is:

        {
        "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "user-since": datetime("2012-08-07T10:10:00.000Z"), "friend-ids": {{ 6 }},
        "employment": [ { "organization-name": "Labzatron", "start-date": date("2011-04-19"), "end-date": null } ]
        }


## Tokenizing Functions ##
### word-tokens ###
 * Syntax:

        word-tokens(string_expression)

 * Returns a list of word tokens of `string_expression`.
 * Arguments:
   * `string_expression` : A `String` that will be tokenized.
 * Return Value:
   * An `OrderedList` of `String` word tokens.

 * Example:

        use dataverse TinySocial;
        
        for $t in dataset('TweetMessages')
        let $tokens := word-tokens($t.message-text)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "word-tokens": $tokens
        }


 * The expected result is:

        { "tweetid": "9", "word-tokens": [ "love", "verizon", "its", "voicemail", "service", "is", "awesome" ] }


### hashed-word-tokens ###
 * Syntax:

        hashed-word-tokens(string_expression)

 * Returns a list of hashed word tokens of `string_expression`.
 * Arguments:
   * `string_expression` : A `String` that will be tokenized.
 * Return Value:
   * An `OrderedList` of Int32 hashed tokens.

 * Example:

        use dataverse TinySocial;
        
        for $t in dataset('TweetMessages')
        let $tokens := hashed-word-tokens($t.message-text)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "hashed-word-tokens": $tokens
        }


 * The expected result is:

        { "tweetid": "9", "hashed-word-tokens": [ -1217719622, -447857469, -1884722688, -325178649, 210976949, 285049676, 1916743959 ] }


### counthashed-word-tokens ###
 * Syntax:

        counthashed-word-tokens(string_expression)

 * Returns a list of hashed word tokens of `string_expression`. The hashing mechanism gives duplicate tokens different hash values, based on the occurrence count of that token.
 * Arguments:
   * `string_expression` : A `String` that will be tokenized.
 * Return Value:
   * An `OrderedList` of `Int32` hashed tokens.
 * Example:

        use dataverse TinySocial;
        
        for $t in dataset('TweetMessages')
        let $tokens := counthashed-word-tokens($t.message-text)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "counthashed-word-tokens": $tokens
        }


 * The expected result is:

        { "tweetid": "9", "counthashed-word-tokens": [ -1217719622, -447857469, -1884722688, -325178649, 210976949, 285049676, 1916743959 ] }


### gram-tokens ###
 * Syntax:

        gram-tokens(string_expression, gram_length, boolean_expression)

 * Returns a list of gram tokens of `string_expression`, which can be obtained by scanning the characters using a sliding window of a fixed length.
 * Arguments:
   * `string_expression` : A `String` that will be tokenized.
   * `gram_length` : An `Int32` as the length of grams.
   * `boolean_expression` : A `Boolean` value to indicate whether to generate additional grams by pre- and postfixing `string_expression` with special characters.
 * Return Value:
   * An `OrderedList` of String gram tokens.

 * Example:

        use dataverse TinySocial;
        
        for $t in dataset('TweetMessages')
        let $tokens := gram-tokens($t.message-text, 3, true)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "gram-tokens": $tokens
        }


 * The expected result is:

        {
        "tweetid": "9",
        "gram-tokens": [ "## ", "# l", " lo", "lov", "ove", "ve ", "e v", " ve", "ver", "eri", "riz", "izo", "zon", "on ", "n i", " it", "its", "ts ", "s v", " vo", "voi", "oic", "ice",
        "cem", "ema", "mai", "ail", "il-", "l-s", "-se", "ser", "erv", "rvi", "vic", "ice", "ce ", "e i", " is", "is ", "s a", " aw", "awe", "wes", "eso", "som", "ome", "me$", "e$$" ]
        }


### hashed-gram-tokens ###
 * Syntax:

        hashed-gram-tokens(string_expression, gram_length, boolean_expression)

 * Returns a list of hashed gram tokens of `string_expression`.
 * Arguments:
   * `string_expression` : A `String` that will be tokenized.
   * `gram_length` : An `Int32` as the length of grams.
   * `boolean_expression` : A `Boolean` to indicate whether to generate additional grams by pre- and postfixing `string_expression` with special characters.
 * Return Value:
   * An `OrderedList` of `Int32` hashed gram tokens.

 * Example:

        use dataverse TinySocial;
        
        for $t in dataset('TweetMessages')
        let $tokens := hashed-gram-tokens($t.message-text, 3, true)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "hashed-gram-tokens": $tokens
        }


 * The expected result is:

        {
        "tweetid": "9",
        "hashed-gram-tokens": [ 40557178, -2002241593, 161665899, -856104603, -500544946, 693410611, 395674299, -1015235909, 1115608337, 1187999872, -31006095, -219180466, -1676061637,
        1040194153, -1339307841, -1527110163, -1884722688, -179148713, -431014627, -1789789823, -1209719926, 684519765, -486734513, 1734740619, -1971673751, -932421915, -2064668066,
        -937135958, -790946468, -69070309, 1561601454, 26169001, -160734571, 1330043462, -486734513, -18796768, -470303314, 113421364, 1615760212, 1688217556, 1223719184, 536568131,
        1682609873, 2935161, -414769471, -1027490137, 1602276102, 1050490461 ]
        }


### counthashed-gram-tokens ###
 * Syntax:

        counthashed-gram-tokens(string_expression, gram_length, boolean_expression)

 * Returns a list of hashed gram tokens of `string_expression`. The hashing mechanism gives duplicate tokens different hash values, based on the occurrence count of that token.
 * Arguments:
   * `string_expression` : A `String` that will be tokenized.
   * `gram_length` : An `Int32`, length of grams to generate.
   * `boolean_expression` : A `Boolean`, whether to generate additional grams by pre- and postfixing `string_expression` with special characters.
 * Return Value:
   * An `OrderedList` of `Int32` hashed gram tokens.

 * Example:

        use dataverse TinySocial;
        
        for $t in dataset('TweetMessages')
        let $tokens := counthashed-gram-tokens($t.message-text, 3, true)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "counthashed-gram-tokens": $tokens
        }


 * The expected result is:

        {
        "tweetid": "9",
        "counthashed-gram-tokens": [ 40557178, -2002241593, 161665899, -856104603, -500544946, 693410611, 395674299, -1015235909, 1115608337, 1187999872, -31006095, -219180466, -1676061637,
        1040194153, -1339307841, -1527110163, -1884722688, -179148713, -431014627, -1789789823, -1209719926, 684519765, -486734513, 1734740619, -1971673751, -932421915, -2064668066, -937135958,
        -790946468, -69070309, 1561601454, 26169001, -160734571, 1330043462, -486734512, -18796768, -470303314, 113421364, 1615760212, 1688217556, 1223719184, 536568131, 1682609873, 2935161,
        -414769471, -1027490137, 1602276102, 1050490461 ]
        }


## Temporal Functions ##

### date ###
 * Syntax:

        date(string_expression)

 * Constructor function for `Date` type by parsing a date string `string_expression`
 * Arguments:
   * `string_expression` : The `String` value representing a date value.
 * Return Value:
   * A `Date` value represented by the given string.

 * Example:

        {
        "date-extended": date("2013-04-01"),
        "date-basic": date("20130401")
        }


 * The expected result is:

        {
        "date-extended": date("2013-04-01"),
        "date-basic": date("2013-04-01")
        }


### time ###
 * Syntax:

        time(string_expression)

 * Constructor function for `Time` type by parsing a time string `string_expression`
 * Arguments:
   * `string_expression` : The `String` value representing a time value.
 * Return Value:
   * A `Time` value represented by the given string.

 * Example:

        {
        "time-extended": time("12:30:45.678+08:00"),
        "time-basic": time("123045678+0800")
        }


 * The expected result is:

        {
        "time-extended": time("04:30:45.678Z"),
        "time-basic": time("04:30:45.678Z")
        }


### datetime ###
 * Syntax:

        datetime(string_expression)

 * Constructor function for `Datetime` type by parsing a datetime string `string_expression`
 * Arguments:
   * `string_expression` : The `String` value representing a datetime value.
 * Return Value:
   * A `Datetime` value represented by the given string.

 * Example:

        {
        "datetime-extended": datetime("2013-04-01T12:30:45.678+08:00"),
        "datetime-basic": datetime("20130401T123045678+0800")
        }


 * The expected result is:

        {
        "datetime-extended": datetime("2013-04-01T04:30:45.678Z"),
        "datetime-basic": datetime("2013-04-01T04:30:45.678Z")
        }


### interval-from-date ###
 * Syntax:

        interval-from-date(string_expression1, string_expression2)

 * Constructor function for `Interval` type by parsing two date strings.
 * Arguments:
   * `string_expression1` : The `String` value representing the starting date.
   * `string_expression2` : The `String` value representing the ending date.
 * Return Value:
   * An `Interval` value between the two dates.

 * Example:

        {"date-interval": interval-from-date("2012-01-01", "2013-04-01")}


 * The expected result is:

        { "date-interval": interval-date("2012-01-01, 2013-04-01") }


### interval-from-time ###
 * Syntax:

        interval-from-time(string_expression1, string_expression2)

 * Constructor function for `Interval` type by parsing two time strings.
 * Arguments:
   * `string_expression1` : The `String` value representing the starting time.
   * `string_expression2` : The `String` value representing the ending time.
 * Return Value:
   * An `Interval` value between the two times.

 * Example:

        {"time-interval": interval-from-time("12:23:34.456Z", "233445567+0800")}


 * The expected result is:

        { "time-interval": interval-time("12:23:34.456Z, 15:34:45.567Z") }


### interval-from-datetime ###
 * Syntax:

        interval-from-datetime(string_expression1, string_expression2)

 * Constructor function for `Interval` type by parsing two datetime strings.
 * Arguments:
   * `string_expression1` : The `String` value representing the starting datetime.
   * `string_expression2` : The `String` value representing the ending datetime.
 * Return Value:
   * An `Interval` value between the two datetimes.

 * Example:

        {"datetime-interval": interval-from-datetime("2012-01-01T12:23:34.456+08:00", "20130401T153445567Z")}


 * The expected result is:

        { "datetime-interval": interval-datetime("2012-01-01T04:23:34.456Z, 2013-04-01T15:34:45.567Z") }


### year/month/day/hour/minute/second/millisecond ###
 * Syntax:

        year/month/day/hour/minute/second/millisecond(temporal_expression)

 * Accessors for accessing fields in a temporal value
 * Arguments:
   * `temporal_expression` : a temporal value represented as one of the following types: `Date`, `Datetime`, `Time`, `Duration`.
 * Return Value:
   * An `Int32` value representing the field to be extracted.

 * Example:

        let $c1 := date("2010-10-30")
        let $c2 := datetime("1987-11-19T23:49:23.938")
        let $c3 := time("12:23:34.930+07:00")
        let $c4 := duration("P3Y73M632DT49H743M3948.94S")
        
        return {"year": year($c1), "month": month($c2), "day": day($c1), "hour": hour($c3), "min": minute($c4), "second": second($c2), "ms": millisecond($c4)}


 * The expected result is:

        { "year": 2010, "month": 11, "day": 30, "hour": 5, "min": 28, "second": 23, "ms": 94 }



### add-date-duration ###
 * Syntax:

        add-date-duration(date_expression, duration_expression)

 * Create a new date by adding the duration `duration_expression` to the given date `date_expression`.
 * Arguments:
   * `date_expression` : The `Date` value to be added onto.
   * `duration_expression` : The `Duration` to be added.
 * Return Value:
   * A `Date` value represents the new date after being adjusted by the duration.

 * Example:

        use dataverse TinySocial;
        
        let $startdate := date('2011-03-01')
        for $i in dataset('TweetMessage')
        where date-from-datetime($i.send-time) > $startdate
        and date-from-datetime($i.send-time) < add-date-duration($startdate, duration('P2Y'))
        return {"send-time": $i.send-time, "message": $i.message-text}


 * The expected result is:

        { "send-time": datetime("2011-12-26T10:10:00.000Z"), "message": " like sprint the voice-command is mind-blowing:)" }
        { "send-time": datetime("2011-08-25T10:10:00.000Z"), "message": " like samsung the platform is good" }
        { "send-time": datetime("2012-07-21T10:10:00.000Z"), "message": " love verizon its voicemail-service is awesome" }


### add-datetime-duration ###
 * Syntax:

        add-date-duration(datetime_expression, duration_expression)

 * Create a new datetime by adding the duration `duration_expression` to the given datetime `datetime_expression`.
 * Arguments:
   * `datetime_expression` : The `Datetime` value to be added onto.
   * `duration_expression` : The `Duration` to be added.
 * Return Value:
   * A `Datetime` value represents the new datetime after being adjusted by the duration.

 * Example:

        use dataverse TinySocial;
        
        let $startdt := datetime('2011-03-01T00:00:00')
        for $i in dataset('TweetMessage')
        where $i.send-time > $startdt and $i.send-time < add-datetime-duration($startdt, duration('P2Y'))
        return {"send-time": $i.send-time, "message": $i.message-text}


 * The expected result is:

        { "send-time": datetime("2011-12-26T10:10:00.000Z"), "message": " like sprint the voice-command is mind-blowing:)" }
        { "send-time": datetime("2011-08-25T10:10:00.000Z"), "message": " like samsung the platform is good" }
        { "send-time": datetime("2012-07-21T10:10:00.000Z"), "message": " love verizon its voicemail-service is awesome" }


### add-time-duration ###
 * Syntax:

        add-time-duration(time_expression, duration_expression)

 * Create a new time by adding the duration `duration_expression` to the given time `time_expression`.
 * Arguments:
   * `time_expression` : The `Time` value to be added onto.
   * `duration_expression` : The `Duration` to be added.
 * Return Value:
   * A `Time` value represents the new time after being adjusted by the duration.

 * Example:

        use dataverse TinySocial;
        
        let $starttime := time('08:00:00')
        for $i in dataset('TweetMessage')
        where time-from-datetime($i.send-time) > $starttime and time-from-datetime($i.send-time) < add-time-duration($starttime, duration('PT5H'))
        return {"send-time": $i.send-time, "message": $i.message-text}


 * The expected result is:

        { "send-time": datetime("2008-04-26T10:10:00.000Z"), "message": " love t-mobile its customization is good:)" }
        { "send-time": datetime("2010-05-13T10:10:00.000Z"), "message": " like verizon its shortcut-menu is awesome:)" }
        { "send-time": datetime("2006-11-04T10:10:00.000Z"), "message": " like motorola the speed is good:)" }
        { "send-time": datetime("2011-12-26T10:10:00.000Z"), "message": " like sprint the voice-command is mind-blowing:)" }
        { "send-time": datetime("2006-08-04T10:10:00.000Z"), "message": " can't stand motorola its speed is terrible:(" }
        { "send-time": datetime("2010-05-07T10:10:00.000Z"), "message": " like iphone the voice-clarity is good:)" }
        { "send-time": datetime("2011-08-25T10:10:00.000Z"), "message": " like samsung the platform is good" }
        { "send-time": datetime("2005-10-14T10:10:00.000Z"), "message": " like t-mobile the shortcut-menu is awesome:)" }
        { "send-time": datetime("2012-07-21T10:10:00.000Z"), "message": " love verizon its voicemail-service is awesome" }
        { "send-time": datetime("2008-01-26T10:10:00.000Z"), "message": " hate verizon its voice-clarity is OMG:(" }
        { "send-time": datetime("2008-03-09T10:10:00.000Z"), "message": " can't stand iphone its platform is terrible" }
        { "send-time": datetime("2010-02-13T10:10:00.000Z"), "message": " like samsung the voice-command is amazing:)" }


### adjust-datetime-for-timezone ###
 * Syntax:

        adjust-datetime-for-timezone(datetime_expression, string_expression)

 * Adjust the given datetime `datetime_expression` by applying the timezone information `string_expression`
 * Arguments:
   * `datetime_expression` : A `Datetime` value to be adjusted.
   * `string_expression` : A `String` representing the timezone information.
 * Return Value:
   * A `String` value represents the new datetime after being adjusted by the timezone information.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('TweetMessage')
        return {"adjusted-send-time": adjust-datetime-for-timezone($i.send-time, "+08:00"), "message": $i.message-text}


 * The expected result is:

        { "adjusted-send-time": "2008-04-26T18:10:00.000+08:00", "message": " love t-mobile its customization is good:)" }
        { "adjusted-send-time": "2010-05-13T18:10:00.000+08:00", "message": " like verizon its shortcut-menu is awesome:)" }
        { "adjusted-send-time": "2006-11-04T18:10:00.000+08:00", "message": " like motorola the speed is good:)" }
        { "adjusted-send-time": "2011-12-26T18:10:00.000+08:00", "message": " like sprint the voice-command is mind-blowing:)" }
        { "adjusted-send-time": "2006-08-04T18:10:00.000+08:00", "message": " can't stand motorola its speed is terrible:(" }
        { "adjusted-send-time": "2010-05-07T18:10:00.000+08:00", "message": " like iphone the voice-clarity is good:)" }
        { "adjusted-send-time": "2011-08-25T18:10:00.000+08:00", "message": " like samsung the platform is good" }
        { "adjusted-send-time": "2005-10-14T18:10:00.000+08:00", "message": " like t-mobile the shortcut-menu is awesome:)" }
        { "adjusted-send-time": "2012-07-21T18:10:00.000+08:00", "message": " love verizon its voicemail-service is awesome" }
        { "adjusted-send-time": "2008-01-26T18:10:00.000+08:00", "message": " hate verizon its voice-clarity is OMG:(" }
        { "adjusted-send-time": "2008-03-09T18:10:00.000+08:00", "message": " can't stand iphone its platform is terrible" }
        { "adjusted-send-time": "2010-02-13T18:10:00.000+08:00", "message": " like samsung the voice-command is amazing:)" }


### adjust-time-for-timezone ###
 * Syntax:

        adjust-time-for-timezone(time_expression, string_expression)

 * Adjust the given time `time_expression` by applying the timezone information `string_expression`
 * Arguments:
   * `time_expression` : A `Time` value to be adjusted.
   * `string_expression` : A `String` representing the timezone information.
 * Return Value:
   * A `String` value represents the new time after being adjusted by the timezone information.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('TweetMessage')
        return {"adjusted-send-time": adjust-time-for-timezone(time-from-datetime($i.send-time), "+08:00"), "message": $i.message-text}


 * The expected result is:

        { "adjusted-send-time": "18:10:00.000+08:00", "message": " love t-mobile its customization is good:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like verizon its shortcut-menu is awesome:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like motorola the speed is good:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like sprint the voice-command is mind-blowing:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " can't stand motorola its speed is terrible:(" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like iphone the voice-clarity is good:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like samsung the platform is good" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like t-mobile the shortcut-menu is awesome:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " love verizon its voicemail-service is awesome" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " hate verizon its voice-clarity is OMG:(" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " can't stand iphone its platform is terrible" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like samsung the voice-command is amazing:)" }


### calendar-duration-from-datetime ###
 * Syntax:

        calendar-duration-from-datetime(datetime_expression, duration_expression)

 * Get a user-friendly representation of the duration `duration_expression` based on the given datetime `datetime_expression`
 * Arguments:
   * `datetime_expression` : A `Datetime` value to be used as the reference time point.
   * `duration_expression` : A `Duration` value to be converted
 * Return Value:
   * A `Duration` value with the duration as `duration_expression` but with a user-friendly representation.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('TweetMessage')
        where $i.send-time > datetime("2011-01-01T00:00:00")
        return {"since-2011": subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")), "since-2011-user-friendly": calendar-duration-from-datetime($i.send-time, subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")))}


 * The expected result is:

        { "since-2011": duration("P359DT10H10M"), "since-2011-user-friendly": duration("P11M23DT10H10M") }
        { "since-2011": duration("P236DT10H10M"), "since-2011-user-friendly": duration("P7M23DT10H10M") }
        { "since-2011": duration("P567DT10H10M"), "since-2011-user-friendly": duration("P1Y6M18DT10H10M") }


### calendar-duration-from-date ###
 * Syntax:

        calendar-duration-from-date(date_expression, duration_expression)

 * Get a user-friendly representation of the duration `duration_expression` based on the given date `date_expression`
 * Arguments:
   * `date_expression` : A `Date` value to be used as the reference time point.
   * `duration_expression` : A `Duration` value to be converted
 * Return Value:
   * A `Duration` value with the duration as `duration_expression` but with a user-friendly representation.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('TweetMessage')
        where $i.send-time > datetime("2011-01-01T00:00:00")
        return {"since-2011": subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")),
        "since-2011-user-friendly": calendar-duration-from-date(date-from-datetime($i.send-time), subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")))}


 * The expected result is:

        { "since-2011": duration("P359DT10H10M"), "since-2011-user-friendly": duration("P11M23DT10H10M") }
        { "since-2011": duration("P236DT10H10M"), "since-2011-user-friendly": duration("P7M23DT10H10M") }
        { "since-2011": duration("P567DT10H10M"), "since-2011-user-friendly": duration("P1Y6M18DT10H10M") }


### current-date ###
 * Syntax:

        current-date()

 * Get the current date
 * Arguments:None
 * Return Value:
   * A `Date` value of the date when the function is called.

### current-time ###
 * Syntax:

        current-time()

 * Get the current time
 * Arguments:None
 * Return Value:
   * A `Time` value of the time when the function is called.

### current-datetime ###
 * Syntax:

        current-datetime()

 * Get the current datetime
 * Arguments:None
 * Return Value:
   * A `Datetime` value of the datetime when the function is called.

 * Example:

        use dataverse TinySocial;
        
        {"current-date": current-date(),
        "current-time": current-time(),
        "current-datetime": current-datetime()}


 * The expected result is:

        { "current-date": date("2013-04-06"),
        "current-time": time("00:48:44.093Z"),
        "current-datetime": datetime("2013-04-06T00:48:44.093Z") }


### date-from-datetime ###
 * Syntax:

        date-from-datetime(datetime_expression)

 * Get the date value from the given datetime value `datetime_expression`
 * Arguments:
   * `datetime_expression`: A `Datetime` value to be extracted from
 * Return Value:
   * A `Date` value from the datetime.

### time-from-datetime ###
 * Syntax:

        time-from-datetime(datetime_expression)

 * Get the time value from the given datetime value `datetime_expression`
 * Arguments:
   * `datetime_expression`: A `Datetime` value to be extracted from
 * Return Value:
   * A `Time` value from the datetime.

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('TweetMessage')
        where $i.send-time > datetime("2011-01-01T00:00:00")
        return {"send-date": date-from-datetime($i.send-time), "send-time": time-from-datetime($i.send-time)}


 * The expected result is:

        { "send-date": date("2011-12-26"), "send-time": time("10:10:00.000Z") }
        { "send-date": date("2011-08-25"), "send-time": time("10:10:00.000Z") }
        { "send-date": date("2012-07-21"), "send-time": time("10:10:00.000Z") }


### date-from-unix-time-in-days ###
 * Syntax:

        date-from-unix-time-in-days(numeric_expression)

 * Get date representing the time after `numeric_expression` days since 1970-01-01
 * Arguments:
   * `numeric_expression`: A `Int8`/`Int16`/`Int32` value representing the number of days
 * Return Value:
   * A `Date` value as the time after `numeric_expression` days since 1970-01-01

### datetime-from-unix-time-in-ms ###
 * Syntax:

        datetime-from-unix-time-in-ms(numeric_expression)

 * Get datetime representing the time after `numeric_expression` milliseconds since 1970-01-01T00:00:00Z
 * Arguments:
   * `numeric_expression`: A `Int8`/`Int16`/`Int32`/`Int64` value representing the number of milliseconds
 * Return Value:
   * A `Datetime` value as the time after `numeric_expression` milliseconds since 1970-01-01T00:00:00Z

### time-from-unix-time-in-ms ###
 * Syntax:

        time-from-unix-time-in-ms(numeric_expression)

 * Get time representing the time after `numeric_expression` milliseconds since 00:00:00.000Z
 * Arguments:
   * `numeric_expression`: A `Int8`/`Int16`/`Int32` value representing the number of milliseconds
 * Return Value:
   * A `Time` value as the time after `numeric_expression` milliseconds since 00:00:00.000Z

 * Example:

        use dataverse TinySocial;
        
        let $d := date-from-unix-time-in-days(15800)
        let $dt := datetime-from-unix-time-in-ms(1365139700000)
        let $t := time-from-unix-time-in-ms(3748)
        return {"date": $d, "datetime": $dt, "time": $t}


 * The expected result is:

        { "date": date("2013-04-05"), "datetime": datetime("2013-04-05T05:28:20.000Z"), "time": time("00:00:03.748Z") }


### subtract-date ###
 * Syntax:

        subtract-date(date_start, date_end)

 * Get the duration between two dates `date_start` and `date_end`
 * Arguments:
   * `date_start`: the starting `Date`
   * `date_end`: the ending `Date`
 * Return Value:
   * A `Duration` value between `date_start` and `date_end`

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookUser')
        for $j in dataset('FacebookUser')
        where $i.user-since < $j.user-since and $i.user-since > datetime("2012-01-01T00:00:00")
        return {"id1": $i.id, "id2": $j.id, "diff": subtract-date(date-from-datetime($j.user-since), date-from-datetime($i.user-since))}


 * The expected result is:

        { "id1": 3, "id2": 1, "diff": duration("P41D") }
        { "id1": 3, "id2": 7, "diff": duration("P28D") }
        { "id1": 7, "id2": 1, "diff": duration("P13D") }


### subtract-time ###
 * Syntax:

        subtract-time(time_start, time_end)

 * Get the duration between two times `time_start` and `time_end`
 * Arguments:
   * `time_start`: the starting `Time`
   * `time_end`: the ending `Time`
 * Return Value:
   * A `Duration` value between `time_start` and `time_end`

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookUser')
        for $j in dataset('FacebookUser')
        where $i.user-since < $j.user-since and $i.user-since > datetime("2012-01-01T00:00:00")
        return {"id1": $i.id, "id2": $j.id, "diff": subtract-time(time-from-datetime($j.user-since), time("02:50:48.938"))}


 * The expected result is:

        { "id1": 3, "id2": 1, "diff": duration("PT7H19M11.62S") }
        { "id1": 3, "id2": 7, "diff": duration("PT7H19M11.62S") }
        { "id1": 7, "id2": 1, "diff": duration("PT7H19M11.62S") }


### subtract-datetime ###
 * Syntax:

        subtract-datetime(datetime_start, datetime_end)

 * Get the duration between two datetimes `datetime_start` and `datetime_end`
 * Arguments:
   * `datetime_start`: the starting `Datetime`
   * `datetime_end`: the ending `Datetime`
 * Return Value:
   * A `Duration` value between `datetime_start` and `datetime_end`

 * Example:

        use dataverse TinySocial;
        
        for $i in dataset('FacebookUser')
        for $j in dataset('FacebookUser')
        where $i.user-since < $j.user-since and $i.user-since > datetime("2011-01-01T00:00:00")
        return {"id1": $i.id, "id2": $j.id, "diff": subtract-datetime($j.user-since, $i.user-since)}


 * The expected result is:

        { "id1": 2, "id2": 1, "diff": duration("P576D") }
        { "id1": 2, "id2": 3, "diff": duration("P535D") }
        { "id1": 2, "id2": 7, "diff": duration("P563D") }
        { "id1": 3, "id2": 1, "diff": duration("P41D") }
        { "id1": 3, "id2": 7, "diff": duration("P28D") }
        { "id1": 7, "id2": 1, "diff": duration("P13D") }

