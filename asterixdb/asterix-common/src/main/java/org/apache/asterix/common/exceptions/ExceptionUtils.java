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

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ExceptionUtils {
    public static final String INCORRECT_PARAMETER = "Incorrect parameter.\n";
    public static final String MISSING_PARAMETER = "Missing parameter.\n";
    public static final String PARAMETER_NAME = "Parameter name: ";
    public static final String EXPECTED_VALUE = "Expected value: ";
    public static final String PASSED_VALUE = "Passed value: ";

    private ExceptionUtils() {
    }

    public static String incorrectParameterMessage(String parameterName, String expectedValue, String passedValue) {
        return INCORRECT_PARAMETER + PARAMETER_NAME + parameterName + System.lineSeparator() + EXPECTED_VALUE
                + expectedValue + System.lineSeparator() + PASSED_VALUE + passedValue;
    }

    public static HyracksDataException suppressIntoHyracksDataException(HyracksDataException hde, Throwable th) {
        if (hde == null) {
            return new HyracksDataException(th);
        } else {
            hde.addSuppressed(th);
            return hde;
        }
    }

    public static Throwable suppress(Throwable suppressor, Throwable suppressed) {
        if (suppressor == null) {
            return suppressed;
        } else if (suppressed != null) {
            suppressor.addSuppressed(suppressed);
        }
        return suppressor;
    }

    public static HyracksDataException convertToHyracksDataException(Throwable throwable) {
        if (throwable == null || throwable instanceof HyracksDataException) {
            return (HyracksDataException) throwable;
        }
        return new HyracksDataException(throwable);
    }
}
