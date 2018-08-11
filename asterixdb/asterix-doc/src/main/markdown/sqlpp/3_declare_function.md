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

When writing a complex query, it can sometimes be helpful to define one or more auxilliary functions
that each address a sub-piece of the overall query.
The declare function statement supports the creation of such helper functions.
In general, the function body (expression) can be any legal query expression.

    FunctionDeclaration  ::= "DECLARE" "FUNCTION" Identifier ParameterList "{" Expression "}"
    ParameterList        ::= "(" ( <VARIABLE> ( "," <VARIABLE> )* )? ")"

The following is a simple example of a temporary function definition and its use.

##### Example

    DECLARE FUNCTION friendInfo(userId) {
        (SELECT u.id, u.name, len(u.friendIds) AS friendCount
         FROM GleambookUsers u
         WHERE u.id = userId)[0]
     };

    SELECT VALUE friendInfo(2);

For our sample data set, this returns:

    [
      { "id": 2, "name": "IsbelDull", "friendCount": 2 }
    ]

