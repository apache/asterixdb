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

## <a id="PrimitiveTypes">Primitive Types</a> ##

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




