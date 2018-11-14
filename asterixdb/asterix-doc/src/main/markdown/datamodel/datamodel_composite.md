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


## <a id="DerivedTypes">Derived Types</a> ##

### <a id="DerivedTypesObject">Object</a> ###
An `object` contains a set of ﬁelds, where each ﬁeld is described by its name and type. An object type may be defined as either open or closed. Open objects (instances of open object types) are permitted to contain ﬁelds that are not part of the type deﬁnition, while closed objects do not permit their instances to carry extra fields. An example type definition for an object is:

        create type SoldierType as open {
            name: string?,
            rank: string,
            serialno: int
        };

Syntactically, object constructors are surrounded by curly braces "{...}".
Some examples of legitimate instances of the above type include:

        { "name": "Joe Blow", "rank": "Sergeant", "serialno": 1234567 }
        { "rank": "Private", "serialno": 9876543 }
        { "name": "Sally Forth", "rank": "Major", "serialno": 2345678, "gender": "F" }

The first instance has all of the type's prescribed content. The second instance is missing the name field, which is fine because it is optional (due to the ?). The third instance has an extra field; that is fine because the type definition specifies that it is open (which is also true by default, if open is not specified). To more tightly control object content, specifying closed instead of open in the type definition for SoldierType would have made the third example instance an invalid instance of the type.

### <a id="DerivedTypesArray">Array</a> ###
An `array` is a container that holds a fixed number of values. Array constructors are denoted by brackets: "[...]".

An example would be


        ["alice", 123, "bob", null]


### <a id="DerivedTypesMultiset">Multiset</a> ###
A `multiset` is a generalization of the concept of a set that, unlike a set, allows multiple instances of the multiset's elements.
 Multiset constructors are denoted by two opening curly braces followed by data and two closing curly braces, like "{{...}}".

An example would be


        {{"hello", 9328, "world", [1, 2, null]}}
