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
package org.apache.asterix.common.exceptions;

import java.util.HashMap;
import java.util.Map;

// Error code:
// 0 --- 999:  runtime errors
// 1000 ---- 1999: compilation errors
// 2000 ---- 2999: storage errors
// 3000 ---- 3999: feed errors
// 4000 ---- 4999: lifecycle management errors
public class ErrorCode {
    public static final String ASTERIX = "ASX";

    // Extension errors
    public static final int ERROR_EXTENSION_ID_CONFLICT = 4001;
    public static final int ERROR_EXTENSION_COMPONENT_CONFLICT = 4002;

    // Runtime errors
    public static final int ERROR_CASTING_FIELD = 1;
    public static final int ERROR_TYPE_MISMATCH = 2;
    public static final int ERROR_TYPE_INCOMPATIBLE = 3;
    public static final int ERROR_TYPE_UNSUPPORTED = 4;
    public static final int ERROR_TYPE_ITEM = 5;
    public static final int ERROR_INVALID_FORMAT = 6;
    public static final int ERROR_OVERFLOW = 7;
    public static final int ERROR_UNDERFLOW = 8;
    public static final int ERROR_INJECTED_FAILURE = 9;
    public static final int ERROR_NEGATIVE_VALUE = 10;
    public static final int ERROR_OUT_OF_BOUND = 11;
    public static final int ERROR_COERCION = 12;
    public static final int ERROR_DUPLICATE_FIELD_NAME = 13;

    // Compilation errors
    public static final int ERROR_PARSE_ERROR = 1001;
    public static final int ERROR_COMPILATION_TYPE_MISMATCH = 1002;
    public static final int ERROR_COMPILATION_TYPE_INCOMPATIBLE = 1003;
    public static final int ERROR_COMPILATION_TYPE_UNSUPPORTED = 1004;
    public static final int ERROR_COMPILATION_TYPE_ITEM = 1005;
    public static final int ERROR_COMPILATION_INVALID_EXPRESSION = 1006;
    public static final int ERROR_COMPILATION_INVALID_PARAMETER_NUMBER = 1007;
    public static final int ERROR_COMPILATION_DUPLICATE_FIELD_NAME = 1008;

    private static final String ERROR_MESSAGE_ID_CONFLICT = "Two Extensions share the same Id: %1$s";
    private static final String ERROR_MESSAGE_COMPONENT_CONFLICT = "Extension Conflict between %1$s and %2$s both "
            + "extensions extend %3$s";
    private static final String ERROR_MESSAGE_TYPE_MISMATCH = "Type mismatch: function %1$s expects"
            + " its %2$s input parameter to be type %3$s, but the actual input type is %4$s";
    private static final String ERROR_MESSAGE_TYPE_INCOMPATIBLE = "Type incompatibility: function %1$s gets"
            + " incompatible input values: %2$s and %3$s";
    private static final String ERROR_MESSAGE_TYPE_UNSUPPORTED = "Unsupported type: %1$s"
            + " cannot process input type %2$s";
    private static final String ERROR_MESSAGE_TYPE_ITEM = "Invalid item type: function %1$s"
            + " cannot process item type %2$s in an input array (or multiset)";
    private static final String ERROR_MESSAGE_INVALID_FORMAT = "Invalid format for %1$s in %2$s";
    private static final String ERROR_MESSAGE_OVERFLOW = "Overflow happend in %1$s";
    private static final String ERROR_MESSAGE_UNDERFLOW = "Underflow happend in %1$s";
    private static final String ERROR_MESSAGE_INJECTED_FAILURE = "Injected failure in %1$s";
    private static final String ERROR_MESSAGE_NEGATIVE_VALUE = "Invalid value: function %1$s expects"
            + " its %2$s input parameter to be a non-negative value, but gets %3$s";
    private static final String ERROR_MESSAGE_OUT_OF_BOUND = "Index out of bound in %1$s: %2$s";
    private static final String ERROR_MESSAGE_COERCION = "Invalid implicit scalar to collection coercion in %1$s";
    private static final String ERROR_MESSAGE_DUPLICATE_FIELD = "Duplicate field name \"%1$s\"";
    private static final String ERROR_MESSAGE_INVALID_EXPRESSION = "Invalid expression: function %1$s expects"
            + " its %2$s input parameter to be a %3$s expression, but the actual expression is %4$s";
    private static final String ERROR_MESSAGE_INVALID_PARAMETER_NUMBER = "Invalid parameter number: function %1$s "
            + "cannot take %2$s parameters";

    private static Map<Integer, String> errorMessageMap = new HashMap<>();

    static {
        // runtime errors
        errorMessageMap.put(ERROR_TYPE_MISMATCH, ERROR_MESSAGE_TYPE_MISMATCH);
        errorMessageMap.put(ERROR_TYPE_INCOMPATIBLE, ERROR_MESSAGE_TYPE_INCOMPATIBLE);
        errorMessageMap.put(ERROR_TYPE_ITEM, ERROR_MESSAGE_TYPE_ITEM);
        errorMessageMap.put(ERROR_TYPE_UNSUPPORTED, ERROR_MESSAGE_TYPE_UNSUPPORTED);
        errorMessageMap.put(ERROR_INVALID_FORMAT, ERROR_MESSAGE_INVALID_FORMAT);
        errorMessageMap.put(ERROR_OVERFLOW, ERROR_MESSAGE_OVERFLOW);
        errorMessageMap.put(ERROR_UNDERFLOW, ERROR_MESSAGE_UNDERFLOW);
        errorMessageMap.put(ERROR_INJECTED_FAILURE, ERROR_MESSAGE_INJECTED_FAILURE);
        errorMessageMap.put(ERROR_NEGATIVE_VALUE, ERROR_MESSAGE_NEGATIVE_VALUE);
        errorMessageMap.put(ERROR_OUT_OF_BOUND, ERROR_MESSAGE_OUT_OF_BOUND);
        errorMessageMap.put(ERROR_COERCION, ERROR_MESSAGE_COERCION);
        errorMessageMap.put(ERROR_DUPLICATE_FIELD_NAME, ERROR_MESSAGE_DUPLICATE_FIELD);

        // compilation errors
        errorMessageMap.put(ERROR_COMPILATION_TYPE_MISMATCH, ERROR_MESSAGE_TYPE_MISMATCH);
        errorMessageMap.put(ERROR_COMPILATION_TYPE_INCOMPATIBLE, ERROR_MESSAGE_TYPE_INCOMPATIBLE);
        errorMessageMap.put(ERROR_COMPILATION_TYPE_ITEM, ERROR_MESSAGE_TYPE_ITEM);
        errorMessageMap.put(ERROR_COMPILATION_TYPE_UNSUPPORTED, ERROR_MESSAGE_TYPE_UNSUPPORTED);
        errorMessageMap.put(ERROR_COMPILATION_INVALID_EXPRESSION, ERROR_MESSAGE_INVALID_EXPRESSION);
        errorMessageMap.put(ERROR_COMPILATION_INVALID_PARAMETER_NUMBER, ERROR_MESSAGE_INVALID_PARAMETER_NUMBER);
        errorMessageMap.put(ERROR_COMPILATION_DUPLICATE_FIELD_NAME, ERROR_MESSAGE_DUPLICATE_FIELD);

        // lifecycle management errors
        errorMessageMap.put(ERROR_EXTENSION_ID_CONFLICT, ERROR_MESSAGE_ID_CONFLICT);
        errorMessageMap.put(ERROR_EXTENSION_COMPONENT_CONFLICT, ERROR_MESSAGE_COMPONENT_CONFLICT);
    }

    private ErrorCode() {
    }

    public static String getErrorMessage(int errorCode) {
        String msg = errorMessageMap.get(errorCode);
        if (msg == null) {
            throw new IllegalStateException("Undefined error code: " + errorCode);
        }
        return msg;
    }
}
