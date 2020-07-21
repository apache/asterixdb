/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.lang.common.util;

import java.util.HashMap;
import java.util.Map;

public class CommonFunctionMapUtil {

    // Maps from a function name to an another internal function name (i.e., AsterixDB internal name).
    private static final Map<String, String> FUNCTION_NAME_MAP = new HashMap<>();

    static {
        addFunctionMapping("ceil", "ceiling"); //ceil,  internal: ceiling
        addFunctionMapping("length", "string-length"); // length,  internal: string-length
        addFunctionMapping("lower", "lowercase"); // lower, internal: lowercase
        addFunctionMapping("substr", "substring"); // substr,  internal: substring
        addFunctionMapping("upper", "uppercase"); // upper, internal: uppercase
        addFunctionMapping("title", "initcap"); // title, internal: initcap
        addFunctionMapping("int", "integer"); // int, internal: integer

        // The "mapped-to" names are to be deprecated.
        addFunctionMapping("tinyint", "int8"); // tinyint, internal: int8
        addFunctionMapping("smallint", "int16"); // smallint, internal: int16
        addFunctionMapping("integer", "int32"); // integer, internal: int32
        addFunctionMapping("bigint", "int64"); // bigint, internal: int64

        // String functions
        addFunctionMapping("pos", "position");
        addFunctionMapping("pos0", "position");
        addFunctionMapping("position0", "position");
        addFunctionMapping("pos1", "position1");
        addFunctionMapping("substr", "substring");
        addFunctionMapping("substr0", "substring");
        addFunctionMapping("substring0", "substring");
        addFunctionMapping("substr1", "substring1");
        addFunctionMapping("regex_contains", "matches");
        addFunctionMapping("contains_regex", "matches");
        addFunctionMapping("regexp_contains", "matches");
        addFunctionMapping("contains_regexp", "matches");
        addFunctionMapping("regex_like", "regexp-like");
        addFunctionMapping("regex_pos", "regexp-position");
        addFunctionMapping("regex_position", "regexp-position");
        addFunctionMapping("regex_pos0", "regexp-position");
        addFunctionMapping("regex_position0", "regexp-position");
        addFunctionMapping("regexp_pos", "regexp-position");
        addFunctionMapping("regexp_pos0", "regexp-position");
        addFunctionMapping("regexp_position0", "regexp-position");
        addFunctionMapping("regex_pos1", "regexp-position1");
        addFunctionMapping("regex_position1", "regexp-position1");
        addFunctionMapping("regexp_pos1", "regexp-position1");
        addFunctionMapping("regex_replace", "regexp-replace");
        addFunctionMapping("regex_matches", "regexp-matches");
        addFunctionMapping("regex_split", "regexp-split");

        // Type functions.
        addFunctionMapping("gettype", "get-type"); // istype, internal: is-type
        addFunctionMapping("isnull", "is-null"); // isnull, internal: is-null
        addFunctionMapping("ismissing", "is-missing"); // ismissing, internal: is-missing
        addFunctionMapping("isunknown", "is-unknown"); // isunknown, internal: is-unknown
        addFunctionMapping("isatomic", "is-atomic"); // isatomic, internal: is-atomic
        addFunctionMapping("isatom", "is-atomic"); // isatom, internal: is-atomic
        addFunctionMapping("is_atom", "is-atomic"); // is_atom, internal: is-atomic
        addFunctionMapping("isboolean", "is-boolean"); // isboolean, internal: is-boolean
        addFunctionMapping("isbool", "is-boolean"); // isbool, internal: is-boolean
        addFunctionMapping("is_bool", "is-boolean"); // is_bool, internal: is-boolean
        addFunctionMapping("isbinary", "is-binary"); // isbinary, internal: is-binary
        addFunctionMapping("isbin", "is-binary"); // isbin, internal: is-binary
        addFunctionMapping("is_bin", "is-binary"); // is_bin, internal: is-binary
        addFunctionMapping("ispoint", "is-point"); // ispoint, internal: is-point
        addFunctionMapping("isline", "is-line"); // isline, internal: is-line
        addFunctionMapping("isrectangle", "is-rectangle"); // isrectangle, internal: is-rectangle
        addFunctionMapping("iscircle", "is-circle"); // iscircle, internal: is-circle
        addFunctionMapping("ispolygon", "is-polygon"); // ispolygon, internal: is-polygon
        addFunctionMapping("isspatial", "is-spatial"); // isspatial, internal: is-spatial
        addFunctionMapping("isdate", "is-date"); // isdate, internal: is-date
        addFunctionMapping("isdatetime", "is-datetime"); // isdatetime, internal: is-datetime
        addFunctionMapping("istimestamp", "is-datetime"); // istimestamp, internal: is-datetime
        addFunctionMapping("is_timestamp", "is-datetime"); // is_timestamp, internal: is-datetime
        addFunctionMapping("istime", "is-time"); // istime, internal: is-time
        addFunctionMapping("isduration", "is-duration"); // isduration, internal: is-duration
        addFunctionMapping("isinterval", "is-interval"); // isinterval, internal: is-interval
        addFunctionMapping("istemporal", "is-temporal"); // istemporal, internal: is-temporal
        addFunctionMapping("isuuid", "is-uuid"); // isuuid, internal: is-uuid
        addFunctionMapping("isnumber", "is-number"); // isnumber, internal: is-number
        addFunctionMapping("isnum", "is-number"); // isnum, internal: is-number
        addFunctionMapping("is_num", "is-number"); // is_num, internal: is-number
        addFunctionMapping("isstring", "is-string"); // isstring, internal: is-string
        addFunctionMapping("isstr", "is-string"); // isstr, internal: is-string
        addFunctionMapping("is_str", "is-string"); // is_str, internal: is-string
        addFunctionMapping("isarray", "is-array"); // isarray, internal: is-array
        addFunctionMapping("ismultiset", "is-multiset"); // ismultiset, internal: is-multiset
        addFunctionMapping("isobject", "is-object"); // isobject, internal: is-object
        addFunctionMapping("isobj", "is-object"); // isobj, internal: is-object
        addFunctionMapping("is_obj", "is-object"); // is_obj, internal: is-object
        addFunctionMapping("ifmissing", "if-missing"); // ifmissing, internal: if-missing
        addFunctionMapping("ifnull", "if-null"); // ifnull, internal: if-null
        addFunctionMapping("ifmissingornull", "if-missing-or-null"); // ifmissingornull, internal: if-missing-or-null
        addFunctionMapping("ifinf", "if-inf"); // ifinf, internal: if-inf
        addFunctionMapping("ifnan", "if-nan"); // ifnan, internal: if-nan
        addFunctionMapping("ifnanorinf", "if-nan-or-inf"); // ifnanorinf, internal: if-nan-or-inf
        addFunctionMapping("coalesce", "if-missing-or-null"); // coalesce, internal: if-missing-or-null
        addFunctionMapping("missingif", "missing-if"); // missingif, internal: missing-if
        addFunctionMapping("nanif", "nan-if"); // nanif, internal: nan-if
        addFunctionMapping("neginfif", "neginf-if"); // neginfif, internal: neginf-if
        addFunctionMapping("nullif", "null-if"); // nullif, internal: null-if
        addFunctionMapping("posinfif", "posinf-if"); // posinfif, internal: posinf-if
        addFunctionMapping("toarray", "to-array"); // toarray, internal: to-array
        addFunctionMapping("toatomic", "to-atomic"); // toatomic, internal: to-atomic
        addFunctionMapping("toatom", "to-atomic"); // toatom, internal: to-atomic
        addFunctionMapping("to_atom", "to-atomic"); // to_atom, internal: to-atomic
        addFunctionMapping("toboolean", "to-boolean"); // toboolean, internal: to-boolean
        addFunctionMapping("tobool", "to-boolean"); // tobool, internal: to-boolean
        addFunctionMapping("to_bool", "to-boolean"); // to_bool, internal: to-boolean
        addFunctionMapping("tobigint", "to-bigint"); // tobigint, internal: to-bigint
        addFunctionMapping("todouble", "to-double"); // todouble, internal: to-double
        addFunctionMapping("tostring", "to-string"); // tostring, internal: to-string
        addFunctionMapping("tostr", "to-string"); // tostr, internal: to-string
        addFunctionMapping("to_str", "to-string"); // to_str, internal: to-string
        addFunctionMapping("tonumber", "to-number"); // tonumber, internal: to-number
        addFunctionMapping("tonum", "to-number"); // tonum, internal: to-number
        addFunctionMapping("to_num", "to-number"); // to_num, internal: to-number
        addFunctionMapping("toobject", "to-object"); // toobject, internal: to-object
        addFunctionMapping("toobj", "to-object"); // toobj, internal: to-object
        addFunctionMapping("to_obj", "to-object"); // to_obj, internal: to-object

        // Object functions
        // record-merge, internal: object-merge
        addFunctionMapping("record-merge", "object-merge");
        // record-concat, internal: object-concat
        addFunctionMapping("record-concat", "object-concat");
        // record-get-fields, internal: object-get-fields
        addFunctionMapping("record-get-fields", "object-get-fields");
        // record-get-field-value, internal: object-get-field-value
        addFunctionMapping("record-get-field-value", "object-get-field-value");
        // record-add-fields, internal: object-add-fields
        addFunctionMapping("record-add-fields", "object-add-fields");
        // record-remove-fields, internal: object-remove-fields
        addFunctionMapping("record-remove-fields", "object-remove-fields");

        // Array/Mutliset functions
        addFunctionMapping("array_agg", "arrayagg");
        addFunctionMapping("array_agg-distinct", "arrayagg-distinct");
        addFunctionMapping("array_length", "len");

        // Aggregate functions
        addFunctionMapping("stddev", "stddev_samp");
        addFunctionMapping("variance", "var_samp");
        addFunctionMapping("variance_samp", "var_samp");
        addFunctionMapping("variance_pop", "var_pop");
    }

    private CommonFunctionMapUtil() {

    }

    public static String getFunctionMapping(String alias) {
        return FUNCTION_NAME_MAP.get(alias);
    }

    public static void addFunctionMapping(String alias, String functionName) {
        FUNCTION_NAME_MAP.put(alias, functionName);
    }
}
