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

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.om.functions.BuiltinFunctions;

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
        addFunctionMapping("regexp_contains", "matches"); // regexp_contains, internal: matches
        addFunctionMapping("int", "integer"); // int, internal: integer

        // The "mapped-to" names are to be deprecated.
        addFunctionMapping("tinyint", "int8"); // tinyint, internal: int8
        addFunctionMapping("smallint", "int16"); // smallint, internal: int16
        addFunctionMapping("integer", "int32"); // integer, internal: int32
        addFunctionMapping("bigint", "int64"); // bigint, internal: int64

        // Type functions.
        addFunctionMapping("isnull", "is-null"); // isnull, internal: is-null
        addFunctionMapping("ismissing", "is-missing"); // ismissing, internal: is-missing
        addFunctionMapping("isunknown", "is-unknown"); // isunknown, internal: is-unknown
        addFunctionMapping("isatomic", "is-atomic"); // isatomic, internal: is-atomic
        addFunctionMapping("isatom", "is-atomic"); // isatom, internal: is-atomic
        addFunctionMapping("is_atom", "is-atomic"); // is_atom, internal: is-atomic
        addFunctionMapping("isboolean", "is-boolean"); // isboolean, internal: is-boolean
        addFunctionMapping("isbool", "is-boolean"); // isbool, internal: is-boolean
        addFunctionMapping("is_bool", "is-boolean"); // is_bool, internal: is-boolean
        addFunctionMapping("isnumber", "is-number"); // isnumber, internal: is-number
        addFunctionMapping("isnum", "is-number"); // isnum, internal: is-number
        addFunctionMapping("is_num", "is-number"); // is_num, internal: is-number
        addFunctionMapping("isstring", "is-string"); // isstring, internal: is-string
        addFunctionMapping("isstr", "is-string"); // isstr, internal: is-string
        addFunctionMapping("is_str", "is-string"); // is_str, internal: is-string
        addFunctionMapping("isarray", "is-array"); // isarray, internal: is-array
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

    /**
     * Maps a user invoked function signature to a builtin internal function signature if possible.
     *
     * @param fs,
     *            the signature of an user typed function.
     * @return the corresponding system internal function signature if it exists, otherwise
     *         the input function signature.
     */
    public static FunctionSignature normalizeBuiltinFunctionSignature(FunctionSignature fs) {
        String name = fs.getName();
        String lowerCaseName = name.toLowerCase();
        String mappedName = getFunctionMapping(lowerCaseName);
        if (mappedName != null) {
            return new FunctionSignature(fs.getNamespace(), mappedName, fs.getArity());
        }
        String understoreName = lowerCaseName.replace('_', '-');
        FunctionSignature newFs = new FunctionSignature(fs.getNamespace(), understoreName, fs.getArity());
        return BuiltinFunctions.isBuiltinCompilerFunction(newFs, true) ? newFs : fs;
    }

    public static String getFunctionMapping(String alias) {
        return FUNCTION_NAME_MAP.get(alias);
    }

    public static void addFunctionMapping(String alias, String functionName) {
        FUNCTION_NAME_MAP.put(alias, functionName);
    }
}
