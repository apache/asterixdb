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

# The Asterix Data Model (ADM) #

## <a id="toc">Table of Contents</a> ##

* [Primitive Types](#PrimitiveTypes)
    * [Boolean](#PrimitiveTypesBoolean)
    * [String](#PrimitiveTypesString)
    * [Tinyint / Smallint / Integer (Int) / Bigint](#PrimitiveTypesInt)
    * [Float](#PrimitiveTypesFloat)
    * [Double (Double Precision)](#PrimitiveTypesDouble)
    * [Binary](#PrimitiveTypesBinary)
    * [Point](#PrimitiveTypesPoint)
    * [Line](#PrimitiveTypesLine)
    * [Rectangle](#PrimitiveTypesRectangle)
    * [Circle](#PrimitiveTypesCircle)
    * [Polygon](#PrimitiveTypesPolygon)
    * [Date](#PrimitiveTypesDate)
    * [Time](#PrimitiveTypesTime)
    * [Datetime (Timestamp)](#PrimitiveTypesDateTime)
    * [Duration/Year_month_duration/Day_time_duration](#PrimitiveTypesDuration)
    * [Interval](#PrimitiveTypesInterval)
    * [UUID](#PrimitiveTypesUUID)
* [Incomplete Information Types](#IncompleteInformationTypes)
    * [Null](#IncompleteInformationTypesNull)
    * [Missing](#IncompleteInformationTypesMissing)
* [Derived Types](#DerivedTypes)
    * [Object](#DerivedTypesObject)
    * [Array](#DerivedTypesArray)
    * [Multiset](#DerivedTypesMultiset)

An instance of Asterix data model (ADM) can be a _*primitive type*_ (`boolean`,
`tinyint`, `smallint`, `integer`, `bigint`, `string`, `float`, `double`, `date`,
`time`, `datetime`, etc.), a _*special type*_ (`null` or `missing`), or a _*derived type*_.

The type names are case-insensitive, e.g., both `BIGINT` and `bigint` are acceptable.

