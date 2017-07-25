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

## <a id="PrimitiveTypes">Primitive Types</a>##

### <a id="PrimitiveTypesBoolean">Boolean</a> ###
`boolean` data type can have one of the two values: _*true*_ or _*false*_.

 * Example:

        { "true": true, "false": false };


 * The expected result is:

        { "true": true, "false": false }


### <a id="PrimitiveTypesString">String</a> ###
`string` represents a sequence of characters. The total length of the sequence can be up to 2,147,483,648.

 * Example:

        { "v1": string("This is a string."), "v2": string("\"This is a quoted string\"") };


 * The expected result is:

        { "v1": "This is a string.", "v2": "\"This is a quoted string\"" }


### <a id="PrimitiveTypesInt">Tinyint / Smallint / Integer (Int) / Bigint</a> ###
Integer types using 8, 16, 32, or 64 bits. The ranges of these types are:

- `tinyint`: -128 to 127
- `smallint`: -32768 to 32767
- `integer`: -2147483648 to 2147483647
- `bigint`: -9223372036854775808 to 9223372036854775807

`int` is an abbreviated alias for integer.

 * Example:

        { "tinyint": tiny("125"), "smallint": smallint("32765"), "integer": 294967295, "bigint": bigint("1700000000000000000")};


 * The expected result is:

        { "tinyint": 125, "smallint": 32765, "integer": 294967295, "bigint": 17000000 }

### <a id="PrimitiveTypesFloat">Float</a> ###
`float` represents approximate numeric data values using 4 bytes. The range of a float value can be
from 2^(-149) to (2-2^(-23)·2^(127) for both positive and negative. Beyond these ranges will get `INF` or `-INF`.

 * Example:

        { "v1": float("NaN"), "v2": float("INF"), "v3": float("-INF"), "v4": float("-2013.5") };


 * The expected result is:

        { "v1": "NaN", "v2": "Infinity", "v3": "-Infinity", "v4": -2013.5 }


### <a id="PrimitiveTypesDouble">Double (double precision)</a> ###
`double` represents approximate numeric data values using 8 bytes. The range of a double value can be from (2^(-1022)) to (2-2^(-52))·2^(1023)
for both positive and negative. Beyond these ranges will get `INF` or `-INF`.

 * Example:

        { "v1": double("NaN"), "v2": double("INF"), "v3": double("-INF"), "v4": "-2013.593823748327284" };


 * The expected result is:

        { "v1": "NaN", "v2": "Infinity", "v3": "-Infinity", "v4": -2013.5938237483274 }

`Double precision` is an alias of `double`.

