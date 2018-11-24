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

## <a id="ObjectFunctions">Object Functions</a> ##

### get_object_fields ###
 * Syntax:

        get_object_fields(input_object)

 * Access the object field names, type and open status for a given object.
 * Arguments:
    * `input_object` : a object value.
 * Return Value:
    * an array of `object` values that include the field_name `string`,
      field_type `string`, is_open `boolean` (used for debug purposes only: `true` if field is open and `false` otherwise),
      and optional nested `orderedList` for the values of a nested object,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-object input value will cause a type error.

 * Example:

        get_object_fields(
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
### get_object_field_value ###
 * Syntax:

        get_object_field_value(input_object, string)

 * Access the field name given in the `string_expression` from the `object_expression`.
 * Arguments:
    * `input_object` : a `object` value.
    * `string` : a `string` representing the top level field name.
 * Return Value:
    * an `any` value saved in the designated field of the object,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-object value,
        * or, the second argument is any other non-string value.

 * Example:

        get_object_field_value({
                                 "id": 1,
                                 "project": "AsterixDB",
                                 "address": {"city": "Irvine", "state": "CA"},
                                 "related": ["Hivestrix", "Preglix", "Apache VXQuery"]
                                },
                                "project"
                               );

 * The expected result is:

        "AsterixDB"

### object_remove_fields ###
 * Syntax:

        object_remove_fields(input_object, field_names)

 * Remove indicated fields from a object given a list of field names.
 * Arguments:
    * `input_object`:  a object value.
    * `field_names`: an array of strings and/or array of array of strings.

 * Return Value:
    * a new object value without the fields listed in the second argument,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-object value,
        * or, the second argument is any other non-array value or recursively contains non-string items.


 * Example:

        object_remove_fields(
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

### object_add_fields ###
 * Syntax:

        object_add_fields(input_object, fields)

 * Add fields to a object given a list of field names.
 * Arguments:
    * `input_object` : a object value.
    * `fields`: an array of field descriptor objects where each object has field_name and  field_value.
 * Return Value:
    * a new object value with the new fields included,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-object value,
        * the second argument is any other non-array value, or contains non-object items.


 * Example:

        object_add_fields(
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

### object_merge ###
 * Syntax:

        object_merge(object1, object2)

 * Merge two different objects into a new object.
 * Arguments:
    * `object1` : a object value.
    * `object2` : a object value.
 * Return Value:
    * a new object value with fields from both input objects. If a field’s names in both objects are the same,
      an exception is issued,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-object input value will cause a type error.


 * Example:

        object_merge(
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

### object_length ###
 * Syntax:

        object_length(input_object)

 * Returns number of top-level fields in the given object
 * Arguments:
    * `input_object` : an object value.
 * Return Value:
    * an integer that represents the number of top-level fields in the given object,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value or any other non-object value

 * Example:

        object_length(
                       {
                         "id": 1,
                         "project": "AsterixDB",
                         "address": {"city": "Irvine", "state": "CA"},
                       }
                     );

 * The expected result is:

        3

### object_names ###
 * Syntax:

        object_names(input_object)

 * Returns names of top-level fields in the given object
 * Arguments:
    * `input_object` : an object value.
 * Return Value:
    * an array with top-level field names of the given object,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value or any other non-object value

 * Example:

        object_names(
                       {
                         "id": 1,
                         "project": "AsterixDB",
                         "address": {"city": "Irvine", "state": "CA"},
                       }
                     );

 * The expected result is:

        [ "id", "project", "address" ]

### object_remove ###
 * Syntax:

        object_remove(input_object, field_name)

 * Returns a new object that has the same fields as the input object except the field to be removed
 * Arguments:
    * `input_object` : an object value.
    * `field_name` : a string field name.
 * Return Value:
    * A new object that has the same fields as `input_object` except the field `field_name`,
    * `missing` if the argument `input_object` or `field_name` is missing,
    * `null` if the argument `input_object` is `null` or any other non-object value, or the argument `field_name`
       is `null` or any other non-string value.

 * Example:

        object_remove(
                       {
                         "id": 1,
                         "project": "AsterixDB",
                         "address": {"city": "Irvine", "state": "CA"}
                       }
                       , "address"
                     );

 * The expected result is:

        {
          "id": 1,
          "project": "AsterixDB",
        }

### object_rename ###
 * Syntax:

        object_rename(input_object, old_field, new_field)

 * Returns a new object that has the same fields as `input_object` with field `old_field` replaced by `new_field`
 * Arguments:
    * `input_object` : an object value.
    * `old_field` : a string representing the old (original) field name inside the object `input_object`.
    * `new_field` : a string representing the new field name to replace `old_field` inside the object `input_object`.
 * Return Value:
    * A new object that has the same fields as `input_object` with field `old_field` replaced by `new_field`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is `null` or `input_object` is non-object value, or `old_field` is non-string value, or
      `new_field` is any non-string value.

 * Example:

        object_rename(
                       {
                         "id": 1,
                         "project": "AsterixDB",
                         "address": {"city": "Irvine", "state": "CA"}
                       }
                       , "address"
                       , "location"
                     );

 * The expected result is:

        {
          "id": 1,
          "project": "AsterixDB",
          "location": {"city": "Irvine", "state": "CA"}
        }

### object_unwrap ###
 * Syntax:

        object_unwrap(input_object)

 * Returns the value of the single name-value pair that appears in `input_object`.
 * Arguments:
    * `input_object` : an object value that consists of exactly one name-value pair.
 * Return Value:
    * The value of the single name-value pair that appears in `input_object`,
    * `missing` if `input_object` is `missing`,
    * `null` if `input_object` is null, or an empty object, or there is more than one name-value pair in `input_object`,
      or any non-object value.

 * Example:

        object_unwrap(
                     {
                       "id": 1
                     }
                   );

 * The expected result is:

        {
          1
        }

### object_replace ###
 * Syntax:

        object_replace(input_object, old_value, new_value)

 * Returns a new object that has the same fields as `input_object` with all occurrences of value `old_value` replaced by
   `new_value`
 * Arguments:
    * `input_object` : an object value.
    * `old_value` : a primitive type value to be replaced by `new_value`.
    * `new_value` : a value to replace `old_value`.
 * Return Value:
    * A new object that has the same fields as `input_object` with all occurrences of value `old_value` replaced by
      `new_value`,
    * `missing` if any argument is a `missing` value,
    * `null` if `input_object`  or `old_value` is null,
    * a type error will be raised if:
        * `old_value` is not a primitive type value.

 * Example:

        object_replace(
                       {
                         "id": 1,
                         "project": "AsterixDB",
                         "address": {"city": "Irvine", "state": "CA"}
                       }
                       , "AsterixDB"
                       , "Apache AsterixDB"
                     );

 * The expected result is:

        {
          "id": 1,
          "project": "Apache AsterixDB",
          "location": {"city": "Irvine", "state": "CA"}
        }

### object_add ###
 * Syntax:

        object_add(input_object, field_name, field_value)

 * Returns a new object that has the same fields as `input_object` as well as the new field `field_name`.
 * Arguments:
    * `input_object` : an object value.
    * `field_name` : a string representing a field name to be added.
    * `field_value` : a value to be assigned to the new field `field_name`.
 * Return Value:
    * A new object that has the same fields as `input_object` as well as the new field `field_name`,
    * `missing` if `input_object` or `field_name` is `missing`,
    * `null` if `input_object` or `field_name` is `null`, or `input_object` is not an object, or `field_name` is not
      a string,
    * `input_object` if `field_name`already exists in `input_object` or `field_value` is missing.

 * Example:

        object_add(
                       {
                         "id": 1,
                         "project": "AsterixDB",
                         "address": {"city": "Irvine", "state": "CA"}
                       }
                       , "company"
                       , "Apache"
                     );

 * The expected result is:

        {
          "id": 1,
          "project": "AsterixDB",
          "location": {"city": "Irvine", "state": "CA"},
          "company": "Apache"
        }

### object_put ###
 * Syntax:

        object_put(input_object, field_name, field_value)

 * Adds, modifies, or removes a field of an object.
 * Arguments:
    * `input_object` : an object value.
    * `field_name` : a string representing a field name to be added.
    * `field_value` : a value to be assigned to the new field `field_name`.
 * Return Value:
    * a new object that has the same fields as `input_object` as well as the new field `field_name`, or with updated
      `field_name` value to `field_value` if `field_name` already exists in `input_object`, or with `field_name`removed
      if `field_name` already exists in `input_object` and `field_value` is `missing`,
    * `missing` if `input_object` or `field_name` is `missing`,
    * `null` if `input_object` or `field_name` is `null`, or `input_object` is not an object, or `field_name` is not
      not a string.

 * Example:

        object_put(
                       {
                         "id": 1,
                         "project": "AsterixDB",
                         "address": {"city": "Irvine", "state": "CA"}
                       }
                       , "project"
                       , "Apache AsterixDB"
                     );

 * The expected result is:

        {
          "id": 1,
          "project": "Apache AsterixDB",
          "location": {"city": "Irvine", "state": "CA"}
        }

### object_values ###
 * Syntax:

        object_values(input_object)

 * Returns an array of the values of the fields in `input_object`.
 * Arguments:
    * `input_object` : an object value.
 * Return Value:
    * An array of the values of the fields in `input_object`,
    * `missing` if `input_object` is `missing`,
    * `null` if `input_object` is null or any non-object value.

 * Example:

        object_values(
                       {
                         "id": 1,
                         "project": "AsterixDB",
                         "address": {"city": "Irvine", "state": "CA"}
                       }
                     );

 * The expected result is:

        [
          1,
          "AsterixDB",
          {"city": "Irvine", "state": "CA"}
        ]

### object_pairs ###
 * Syntax:

        object_pairs(input_object)

 * Returns an array of objects describing fields of `input_object`.
   For each field of the `input_object` the returned array contains an object with two fields `name` and `value`
   which are set to the `input_object`'s field name and value.

 * Arguments:
    * `input_object` : an object value.
 * Return Value:
    * An array of the `name`/`value` pairs of the fields in `input_object`,
    * `missing` if `input_object` is `missing`,
    * `null` if `input_object` is null or any non-object value.

 * Example:

        object_pairs(
                      {
                        "id": 1,
                        "project": "AsterixDB",
                        "address": {"city": "Irvine", "state": "CA"}
                      }
                    );

 * The expected result is:

        [
          { "name": "id", "value": 1 },
          { "name": "project", "value": "AsterixDB" },
          { "name": "address", "value": {"city": "Irvine", "state": "CA"} }
        ]

### pairs ###
 * Syntax:

        pairs(input_object)

 * Returns an array of arrays describing fields of `input_object`, including nested fields.
   For each field of the `input_object` the returned array contains an array with two elements.
   The first element is the name and the second one is the value of the `input_object`'s field.
   The input object is introspected recursively, so all fields of its nested objects are returned.
   Nested objects contained in arrays and multisets are also processed by this function.

 * Arguments:
    * `input_object` : an object value (or an array or a multiset)
 * Return Value:
    * An array of arrays with name, value pairs of the fields in `input_object`, including nested fields.
      Each inner array has exactly two items: name and value of the `input_object`'s field.
    * `missing` if `input_object` is `missing`,
    * `null` if `input_object` is null or a value of a primitive data type.

 * Example:

        pairs(
               {
                 "id": 1,
                 "project": "AsterixDB",
                 "address": {"city": "Irvine", "state": "CA"}
               }
             );

 * The expected result is:

        [
          [ "id", 1 ],
          [ "project", "AsterixDB" ],
          [ "address", { "city": "Irvine", "state": "CA" } ],
          [ "city", "Irvine" ],
          [ "state", "CA" ]
        ]

