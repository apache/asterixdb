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

## <a id="IncompleteInformationTypes">Incomplete Information Types</a> ##

### <a id="IncompleteInformationTypesNull">Null</a> ###
`null` is a special value that is often used to represent an unknown value.
For example, a user might not be able to know the value of a field and let it be `null`.

 * Example:

        { "field": null };


 * The expected result is:

        { "field": null }


### <a id="IncompleteInformationTypesMissing">Missing</a> ###
`missing` indicates that a name-value pair is missing from an object.
If a missing name-value pair is accessed, an empty result value is returned by the query.

As neither the data model nor the system enforces homogeneity for datasets or collections,
items in a dataset or collection can be of heterogeneous types and
so a field can be present in one object and `missing` in another.

 * Example:

        { "field": missing };


 * The expected result is:

        {  }

Since a field with value `missing` means the field is absent, we get an empty object.

