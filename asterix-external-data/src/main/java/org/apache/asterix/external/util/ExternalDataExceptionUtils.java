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
package org.apache.asterix.external.util;

public class ExternalDataExceptionUtils {
    public static final String INCORRECT_PARAMETER = "Incorrect parameter.\n";
    public static final String MISSING_PARAMETER = "Missing parameter.\n";
    public static final String PARAMETER_NAME = "Parameter name: ";
    public static final String EXPECTED_VALUE = "Expected value: ";
    public static final String PASSED_VALUE = "Passed value: ";

    public static String incorrectParameterMessage(String parameterName, String expectedValue, String passedValue) {
        return INCORRECT_PARAMETER + PARAMETER_NAME + parameterName + ExternalDataConstants.LF + EXPECTED_VALUE
                + expectedValue + ExternalDataConstants.LF + PASSED_VALUE + passedValue;
    }
}
