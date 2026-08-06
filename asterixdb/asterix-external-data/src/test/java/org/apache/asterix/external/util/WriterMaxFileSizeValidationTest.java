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

import static org.apache.asterix.external.util.ExternalDataConstants.KEY_WRITER_MAX_FILE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.junit.Test;

public class WriterMaxFileSizeValidationTest {

    private static final String ADAPTER = ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3;

    @Test
    public void testAbsentIsAccepted() throws CompilationException {
        validate(null);
    }

    @Test
    public void testValidSizesAreAccepted() throws CompilationException {
        validate("5MB");
        validate("128MB");
        validate("1GB");
        validate("4.5GB");
        validate("5GB");
        validate("10485760");
    }

    @Test
    public void testUnparsableSizeIsRejected() {
        assertRejected("random", ErrorCode.ILLEGAL_SIZE_PROVIDED);
        assertRejected("", ErrorCode.ILLEGAL_SIZE_PROVIDED);
        assertRejected("10XB", ErrorCode.ILLEGAL_SIZE_PROVIDED);
    }

    @Test
    public void testSizeBelowMinimumIsRejected() {
        assertRejected("1MB", ErrorCode.MINIMUM_VALUE_ALLOWED_FOR_PARAM);
        assertRejected("1024", ErrorCode.MINIMUM_VALUE_ALLOWED_FOR_PARAM);
        assertRejected("0", ErrorCode.MINIMUM_VALUE_ALLOWED_FOR_PARAM);
    }

    @Test
    public void testNegativeSizeIsRejected() {
        // the size parser accepts a leading minus, so the minimum check is what rules these out
        assertRejected("-1", ErrorCode.MINIMUM_VALUE_ALLOWED_FOR_PARAM);
        assertRejected("-10MB", ErrorCode.MINIMUM_VALUE_ALLOWED_FOR_PARAM);
    }

    @Test
    public void testSizeAboveMaximumIsRejected() {
        assertRejected("6TB", ErrorCode.MAXIMUM_VALUE_ALLOWED_FOR_PARAM);
        assertRejected("1PB", ErrorCode.MAXIMUM_VALUE_ALLOWED_FOR_PARAM);
    }

    @Test
    public void testRejectionNamesTheOption() {
        try {
            validate("random");
            fail("expected the invalid size to be rejected");
        } catch (CompilationException e) {
            assertTrue("error message does not name the option: " + e.getMessage(),
                    e.getMessage().contains(KEY_WRITER_MAX_FILE_SIZE));
        }
    }

    private static void assertRejected(String maxFileSize, ErrorCode expected) {
        try {
            validate(maxFileSize);
            fail("expected '" + maxFileSize + "' to be rejected");
        } catch (CompilationException e) {
            assertEquals("unexpected error for '" + maxFileSize + "'", expected.intValue(), e.getErrorCode());
        }
    }

    private static void validate(String maxFileSize) throws CompilationException {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ExternalDataConstants.KEY_FORMAT, ExternalDataConstants.FORMAT_JSON_LOWER_CASE);
        if (maxFileSize != null) {
            configuration.put(KEY_WRITER_MAX_FILE_SIZE, maxFileSize);
        }
        WriterValidationUtil.validateWriterConfiguration(ADAPTER, ExternalDataConstants.WRITER_SUPPORTED_ADAPTERS,
                configuration, null);
    }
}
