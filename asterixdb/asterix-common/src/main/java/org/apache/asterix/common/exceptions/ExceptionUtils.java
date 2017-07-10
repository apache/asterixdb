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

public class ExceptionUtils {
    public static final String INCORRECT_PARAMETER = "Incorrect parameter.\n";
    public static final String PARAMETER_NAME = "Parameter name: ";
    public static final String EXPECTED_VALUE = "Expected value: ";
    public static final String PASSED_VALUE = "Passed value: ";

    private ExceptionUtils() {
    }

    public static String incorrectParameterMessage(String parameterName, String expectedValue, String passedValue) {
        return INCORRECT_PARAMETER + PARAMETER_NAME + parameterName + System.lineSeparator() + EXPECTED_VALUE
                + expectedValue + System.lineSeparator() + PASSED_VALUE + passedValue;
    }

    // Gets the error message for the root cause of a given Throwable instance.
    public static String getErrorMessage(Throwable th) {
        Throwable cause = getRootCause(th);
        return cause.getMessage();
    }

    // Finds the root cause of a given Throwable instance.
    public static Throwable getRootCause(Throwable e) {
        Throwable current = e;
        Throwable cause = e.getCause();
        while (cause != null && cause != current) {
            Throwable nextCause = current.getCause();
            current = cause;
            cause = nextCause;
        }
        return current;
    }
}
