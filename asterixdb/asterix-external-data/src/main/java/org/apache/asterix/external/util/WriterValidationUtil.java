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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class WriterValidationUtil {

    private WriterValidationUtil() {
    }

    public static void validateWriterConfiguration(String adapter, Set<String> supportedAdapters,
            Map<String, String> configuration, SourceLocation sourceLocation) throws CompilationException {
        validateAdapter(adapter, supportedAdapters, sourceLocation);
        validateFormat(configuration, sourceLocation);
        validateCompression(configuration, sourceLocation);
        validateMaxResult(configuration, sourceLocation);
    }

    private static void validateAdapter(String adapter, Set<String> supportedAdapters, SourceLocation sourceLocation)
            throws CompilationException {
        checkSupported(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE, adapter, supportedAdapters,
                ErrorCode.UNSUPPORTED_WRITING_ADAPTER, sourceLocation, false);
    }

    private static void validateFormat(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        checkSupported(ExternalDataConstants.KEY_FORMAT, format, ExternalDataConstants.WRITER_SUPPORTED_FORMATS,
                ErrorCode.UNSUPPORTED_WRITING_FORMAT, sourceLocation, false);
    }

    private static void validateCompression(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String compression = configuration.get(ExternalDataConstants.KEY_WRITER_COMPRESSION);
        checkSupported(ExternalDataConstants.KEY_WRITER_COMPRESSION, compression,
                ExternalDataConstants.WRITER_SUPPORTED_COMPRESSION, ErrorCode.UNKNOWN_COMPRESSION_SCHEME,
                sourceLocation, true);
    }

    private static void validateMaxResult(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String maxResult = configuration.get(ExternalDataConstants.KEY_WRITER_MAX_RESULT);
        if (maxResult == null) {
            return;
        }

        try {
            Integer.parseInt(maxResult);
        } catch (NumberFormatException e) {
            throw CompilationException.create(ErrorCode.INTEGER_VALUE_EXPECTED, sourceLocation, maxResult);
        }
    }

    private static void checkSupported(String paramKey, String value, Set<String> supportedSet, ErrorCode errorCode,
            SourceLocation sourceLocation, boolean optional) throws CompilationException {
        if (optional && value == null) {
            return;
        }

        if (value == null) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, sourceLocation, paramKey);
        }

        String normalizedValue = value.toLowerCase();
        if (!supportedSet.contains(normalizedValue)) {
            List<String> sorted = supportedSet.stream().sorted().collect(Collectors.toList());
            throw CompilationException.create(errorCode, sourceLocation, value, sorted.toString());
        }
    }

}
