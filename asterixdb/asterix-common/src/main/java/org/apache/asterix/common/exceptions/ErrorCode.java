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

import java.io.File;
import java.io.InputStream;
import java.util.Map;

import org.apache.hyracks.api.util.ErrorMessageUtil;

// Error code:
// 0 --- 999:  runtime errors
// 1000 ---- 1999: compilation errors
// 2000 ---- 2999: storage errors
// 3000 ---- 3999: feed errors
// 4000 ---- 4999: lifecycle management errors
public class ErrorCode {
    private static final String RESOURCE_PATH = "asx_errormsg" + File.separator + "en.properties";
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
    public static final int ERROR_COMPILATION_DUPLICATE_FIELD_NAME = 1006;
    public static final int ERROR_COMPILATION_INVALID_EXPRESSION = 1007;
    public static final int ERROR_COMPILATION_INVALID_PARAMETER_NUMBER = 1008;
    public static final int ERROR_COMPILATION_INVALID_RETURNING_EXPRESSION = 1009;

    // Loads the map that maps error codes to error message templates.
    private static Map<Integer, String> errorMessageMap = null;

    private ErrorCode() {
    }

    public static String getErrorMessage(int errorCode) {
        if (errorMessageMap == null) {
            try (InputStream resourceStream = ErrorCode.class.getClassLoader().getResourceAsStream(RESOURCE_PATH)) {
                errorMessageMap = ErrorMessageUtil.loadErrorMap(resourceStream);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        String msg = errorMessageMap.get(errorCode);
        if (msg == null) {
            throw new IllegalStateException("Undefined error code: " + errorCode);
        }
        return msg;
    }
}
