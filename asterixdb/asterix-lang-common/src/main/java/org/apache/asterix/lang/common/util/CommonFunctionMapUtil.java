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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;

public class CommonFunctionMapUtil {

    // Maps from a function name to an another internal function name (i.e., AsterixDB internal name).
    private static final Map<String, String> FUNCTION_NAME_MAP = new HashMap<>();

    static {
        FUNCTION_NAME_MAP.put("ceil", "ceiling"); //ceil,  internal: ceiling
        FUNCTION_NAME_MAP.put("length", "string-length"); // length,  internal: string-length
        FUNCTION_NAME_MAP.put("lower", "lowercase"); // lower, internal: lowercase
        FUNCTION_NAME_MAP.put("substr", "substring"); // substr,  internal: substring
        FUNCTION_NAME_MAP.put("upper", "uppercase"); // upper, internal: uppercase
        FUNCTION_NAME_MAP.put("title", "initcap"); // title, internal: initcap
        FUNCTION_NAME_MAP.put("regexp_contains", "matches"); // regexp_contains, internal: matches
        FUNCTION_NAME_MAP.put("regexp_replace", "replace"); //regexp_replace, internal: replace
        FUNCTION_NAME_MAP.put("power", "caret"); //pow, internal: caret
        FUNCTION_NAME_MAP.put("int", "integer"); // int, internal: integer

        // The "mapped-to" names are to be deprecated.
        FUNCTION_NAME_MAP.put("tinyint", "int8"); // tinyint, internal: int8
        FUNCTION_NAME_MAP.put("smallint", "int16"); // smallint, internal: int16
        FUNCTION_NAME_MAP.put("integer", "int32"); // integer, internal: int32
        FUNCTION_NAME_MAP.put("bigint", "int64"); // bigint, internal: int64

        // Type functions.
        FUNCTION_NAME_MAP.put("isnull", "is-null"); // isnull, internal: is-null
        FUNCTION_NAME_MAP.put("ismissing", "is-missing"); // ismissing, internal: is-missing
        FUNCTION_NAME_MAP.put("isunknown", "is-unknown"); // isunknown, internal: is-unknown
        FUNCTION_NAME_MAP.put("isboolean", "is-boolean"); // isboolean, internal: is-boolean
        FUNCTION_NAME_MAP.put("isbool", "is-boolean"); // isbool, internal: is-boolean
        FUNCTION_NAME_MAP.put("isnumber", "is-number"); // isnumber, internal: is-number
        FUNCTION_NAME_MAP.put("isnum", "is-number"); // isnum, internal: is-number
        FUNCTION_NAME_MAP.put("isstring", "is-string"); // isstring, internal: is-string
        FUNCTION_NAME_MAP.put("isstr", "is-string"); // isstr, internal: is-string
        FUNCTION_NAME_MAP.put("isarray", "is-array"); // isarray, internal: is-array
        FUNCTION_NAME_MAP.put("isobject", "is-object"); // isobject, internal: is-object
        FUNCTION_NAME_MAP.put("isobj", "is-object"); // isobj, internal: is-object

        // Object functions
        FUNCTION_NAME_MAP.put("record-merge", "object-merge"); // record-merge, internal: object-merge
        // record-get-fields, internal: object-get-fields
        FUNCTION_NAME_MAP.put("record-get-fields", "object-get-fields");
        // record-get-field-value, internal: object-get-field-value
        FUNCTION_NAME_MAP.put("record-get-field-value", "object-get-field-value");
        // record-add-fields, internal: object-add-fields
        FUNCTION_NAME_MAP.put("record-add-fields", "object-add-fields");
        // record-remove-fields, internal: object-remove-fields
        FUNCTION_NAME_MAP.put("record-remove-fields", "object-remove-fields");
    }

    private CommonFunctionMapUtil() {

    }

    /**
     * Maps a user invoked function signature to a builtin internal function signature if possible.
     *
     * @param fs,
     *            the signature of an user typed function.
     * @return the corresponding system internal function signature if it exists, otherwise
     *         the input function synature.
     */
    public static FunctionSignature normalizeBuiltinFunctionSignature(FunctionSignature fs) throws AsterixException {
        String name = fs.getName();
        String lowerCaseName = name.toLowerCase();
        String mappedName = FUNCTION_NAME_MAP.get(lowerCaseName);
        if (mappedName != null) {
            return new FunctionSignature(fs.getNamespace(), mappedName, fs.getArity());
        }
        String understoreName = lowerCaseName.replace('_', '-');
        FunctionSignature newFs = new FunctionSignature(fs.getNamespace(), understoreName, fs.getArity());
        return AsterixBuiltinFunctions.isBuiltinCompilerFunction(newFs, true) ? newFs : fs;
    }
}
