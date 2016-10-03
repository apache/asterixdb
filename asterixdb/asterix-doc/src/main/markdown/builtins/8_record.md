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

## <a id="RecordFunctions">Record Functions</a> ##

### get_record_fields ###
 * Syntax:

        get_record_fields(input_record)

 * Access the record field names, type and open status for a given record.
 * Arguments:
    * `input_record` : a record value.
 * Return Value:
    * an array of `record` values that include the field_name `string`,
      field_type `string`, is_open `boolean` (used for debug purposes only: `true` if field is open and `false` otherwise),
      and optional nested `orderedList` for the values of a nested record,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-record input value will cause a type error.

 * Example:

        get_record_fields(
                          {
                            "id": 1,
                            "project": "AsterixDB",
                            "address": {"city": "Irvine", "state": "CA"},
                            "related": ["Hivestrix", "Preglix", "Apache VXQuery"]
                          }
                         );

 * The expected result is:

        [
          { "field-name": "id", "field-type": "INT64", "is-open": false },
          { "field-name": "project", "field-type": "STRING", "is-open": false },
          { "field-name": "address", "field-type": "RECORD", "is-open": false,
            "nested": [
                        { "field-name": "city", "field-type": "STRING", "is-open": false },
                        { "field-name": "state", "field-type": "STRING", "is-open": false }
                      ]
          },
          { "field-name":
                "related",
                "field-type": "ORDEREDLIST",
                "is-open": false,
                "list": [
                          { "field-type": "STRING" },
                          { "field-type": "STRING" },
                          { "field-type": "STRING" }
                        ]
          }
        ]

 ]
### get_record_field_value ###
 * Syntax:

        get_record_field_value(input_record, string)

 * Access the field name given in the `string_expression` from the `record_expression`.
 * Arguments:
    * `input_record` : a `record` value.
    * `string` : a `string` representing the top level field name.
 * Return Value:
    * an `any` value saved in the designated field of the record,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-record value,
        * or, the second argument is any other non-string value.

 * Example:

        get_record_field_value({
                                 "id": 1,
                                 "project": "AsterixDB",
                                 "address": {"city": "Irvine", "state": "CA"},
                                 "related": ["Hivestrix", "Preglix", "Apache VXQuery"]
                                },
                                "project"
                               );

 * The expected result is:

        "AsterixDB"

### record_remove_fields ###
 * Syntax:

        record_remove_fields(input_record, field_names)

 * Remove indicated fields from a record given a list of field names.
 * Arguments:
    * `input_record`:  a record value.
    * `field_names`: an array of strings and/or array of array of strings.

 * Return Value:
    * a new record value without the fields listed in the second argument,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-record value,
        * or, the second argument is any other non-array value or recursively contains non-string items.


 * Example:

        record_remove_fields(
                               {
                                 "id":1,
                                 "project":"AsterixDB",
                                 "address":{"city":"Irvine", "state":"CA"},
                                 "related":["Hivestrix", "Preglix", "Apache VXQuery"]
                               },
                               [["address", "city"], "related"]
                             );

 * The expected result is:

        {
          "id":1,
          "project":"AsterixDB",
          "address":{ "state": "CA" }
        }

### record_add_fields ###
 * Syntax:

        record_add_fields(input_record, fields)

 * Add fields to a record given a list of field names.
 * Arguments:
    * `input_record` : a record value.
    * `fields`: an array of field descriptor records where each record has field_name and  field_value.
 * Return Value:
    * a new record value with the new fields included,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-record value,
        * the second argument is any other non-array value, or contains non-record items.


 * Example:

        record_add_fields(
                           {
                             "id":1,
                             "project":"AsterixDB",
                             "address":{"city":"Irvine", "state":"CA"},
                             "related":["Hivestrix", "Preglix", "Apache VXQuery"]
                            },
                            [{"field-name":"employment_location", "field-value":create_point(30.0,70.0)}]
                          );

 * The expected result is:

        {
           "id":1,
           "project":"AsterixDB",
           "address":{"city":"Irvine", "state":"CA"},
           "related":["Hivestrix", "Preglix", "Apache VXQuery"]
           "employment_location": point("30.0,70.0")
         }

### record_merge ###
 * Syntax:

        record_merge(record1, record2)

 * Merge two different records into a new record.
 * Arguments:
    * `record1` : a record value.
    * `record2` : a record value.
 * Return Value:
    * a new record value with fields from both input records. If a field’s names in both records are the same,
      an exception is issued,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-record input value will cause a type error.


 * Example:

        record_merge(
                      {
                        "id":1,
                        "project":"AsterixDB",
                        "address":{"city":"Irvine", "state":"CA"},
                        "related":["Hivestrix", "Preglix", "Apache VXQuery"]
                      },
                      {
                        "user_id": 22,
                        "employer": "UC Irvine",
                        "employment_type": "visitor"
                      }
                    );

 * The expected result is:

        {
          "employment_type": "visitor",
          "address": {
            "city": "Irvine",
            "state": "CA"
          },
          "related": [
            "Hivestrix",
            "Preglix",
            "Apache VXQuery"
          ],
          "user_id": 22,
          "project": "AsterixDB",
          "employer": "UC Irvine",
          "id": 1
        }

