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


The following example creates an open btree index called gbReadTimeIdx on the (non-predeclared) readTime
field of the GleambookMessages dataset having datetime type.
This index can be useful for accelerating exact-match queries, range search queries,
and joins involving the `readTime` field.
The index is not enforced so that records that do not have the `readTime` field or have a mismatched type on the field
can still be inserted into the dataset.

#### Example

    CREATE INDEX gbReadTimeIdx ON GleambookMessages(readTime: datetime?);

