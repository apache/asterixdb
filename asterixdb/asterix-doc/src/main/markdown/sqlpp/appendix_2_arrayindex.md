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

## <a id="ArrayIndexFlag">Controlling Array-Index Access Method Plan Parameter</a>
By default, the system attempts to utilize array indexes as an access method if an array index is present and is applicable.
If you believe that your query will not benefit from an array index, toggle the parameter below.

*  **compiler.arrayindex**: if this is set to true, array indexes will be considered as an access method for applicable queries; the default value is true.


#### Example

    set `compiler.arrayindex` "false";

    SELECT o.orderno
    FROM orders o
    WHERE SOME i IN o.items
    SATISFIES i.price = 19.91;
