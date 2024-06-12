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

import static org.apache.asterix.common.exceptions.ErrorCode.INVALID_REQ_PARAM_VAL;
import static org.apache.asterix.common.exceptions.ErrorCode.MAXIMUM_VALUE_ALLOWED_FOR_PARAM;
import static org.apache.asterix.common.exceptions.ErrorCode.MINIMUM_VALUE_ALLOWED_FOR_PARAM;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAMETERS_REQUIRED;
import static org.apache.asterix.external.util.ExternalDataConstants.FORMAT_CSV;
import static org.apache.asterix.external.util.ExternalDataConstants.FORMAT_JSON_LOWER_CASE;
import static org.apache.asterix.external.util.ExternalDataConstants.FORMAT_PARQUET;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_PARQUET_PAGE_SIZE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_PARQUET_ROW_GROUP_SIZE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_WRITER_MAX_RESULT;
import static org.apache.asterix.external.util.ExternalDataConstants.PARQUET_MAX_SCHEMAS_KEY;
import static org.apache.asterix.external.util.ExternalDataConstants.PARQUET_MAX_SCHEMAS_MAX_VALUE;
import static org.apache.asterix.external.util.ExternalDataConstants.PARQUET_WRITER_VERSION_KEY;
import static org.apache.asterix.external.util.ExternalDataConstants.WRITER_MAX_RESULT_MINIMUM;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.StorageUtil;

public class WriterValidationUtil {

    private WriterValidationUtil() {
    }

    public static void validateWriterConfiguration(String adapter, Set<String> supportedAdapters,
            Map<String, String> configuration, SourceLocation sourceLocation) throws CompilationException {
        validateAdapter(adapter, supportedAdapters, sourceLocation);
        validateFormat(configuration, sourceLocation);
        validateMaxResult(configuration, sourceLocation);
    }

    private static void validateQuote(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String quote = configuration.get(ExternalDataConstants.KEY_QUOTE);
        if (quote != null && !ExternalDataConstants.WRITER_SUPPORTED_QUOTES.contains(quote.toLowerCase())) {
            throw CompilationException.create(ErrorCode.INVALID_QUOTE, sourceLocation, quote,
                    ExternalDataConstants.WRITER_SUPPORTED_QUOTES.toString());
        }
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
        switch (format.toLowerCase()) {
            case FORMAT_JSON_LOWER_CASE:
                validateJSON(configuration, sourceLocation);
                break;
            case FORMAT_PARQUET:
                validateParquet(configuration, sourceLocation);
                break;
            case FORMAT_CSV:
                validateCSV(configuration, sourceLocation);
                break;
        }
    }

    private static void validateParquet(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        validateParquetCompression(configuration, sourceLocation);
        validateParquetRowGroupSize(configuration);
        validateParquetPageSize(configuration);
        validateVersion(configuration, sourceLocation);
        validateMaxParquetSchemas(configuration, sourceLocation);
    }

    private static void validateVersion(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String version = configuration.get(PARQUET_WRITER_VERSION_KEY);
        checkSupported(PARQUET_WRITER_VERSION_KEY, version, ExternalDataConstants.PARQUET_WRITER_SUPPORTED_VERSION,
                ErrorCode.INVALID_PARQUET_WRITER_VERSION, sourceLocation, true);
    }

    private static void validateParquetRowGroupSize(Map<String, String> configuration) throws CompilationException {
        String rowGroupSize = configuration.get(KEY_PARQUET_ROW_GROUP_SIZE);
        if (rowGroupSize == null)
            return;
        try {
            StorageUtil.getByteValue(rowGroupSize);
        } catch (IllegalArgumentException e) {
            throw CompilationException.create(ErrorCode.ILLEGAL_SIZE_PROVIDED, KEY_PARQUET_ROW_GROUP_SIZE,
                    rowGroupSize);
        }
    }

    private static void validateParquetPageSize(Map<String, String> configuration) throws CompilationException {
        String pageSize = configuration.get(KEY_PARQUET_PAGE_SIZE);
        if (pageSize == null)
            return;
        try {
            StorageUtil.getByteValue(pageSize);
        } catch (IllegalArgumentException e) {
            throw CompilationException.create(ErrorCode.ILLEGAL_SIZE_PROVIDED, KEY_PARQUET_PAGE_SIZE, pageSize);
        }
    }

    private static void validateJSON(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        validateTextualCompression(configuration, sourceLocation);
    }

    private static void validateCSV(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        validateTextualCompression(configuration, sourceLocation);
        validateDelimiter(configuration, sourceLocation);
        validateRecordDelimiter(configuration, sourceLocation);
        validateQuote(configuration, sourceLocation);
        validateEscape(configuration, sourceLocation);
    }

    private static void validateParquetCompression(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String compression = configuration.get(ExternalDataConstants.KEY_WRITER_COMPRESSION);
        checkCompressionSupported(ExternalDataConstants.KEY_WRITER_COMPRESSION, compression,
                ExternalDataConstants.PARQUET_WRITER_SUPPORTED_COMPRESSION,
                ErrorCode.UNSUPPORTED_WRITER_COMPRESSION_SCHEME, sourceLocation, FORMAT_PARQUET, true);
    }

    private static void validateTextualCompression(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String compression = configuration.get(ExternalDataConstants.KEY_WRITER_COMPRESSION);
        checkCompressionSupported(ExternalDataConstants.KEY_WRITER_COMPRESSION, compression,
                ExternalDataConstants.TEXTUAL_WRITER_SUPPORTED_COMPRESSION,
                ErrorCode.UNSUPPORTED_WRITER_COMPRESSION_SCHEME, sourceLocation,
                configuration.get(ExternalDataConstants.KEY_FORMAT), true);
        if (ExternalDataUtils.isGzipCompression(compression)) {
            validateGzipCompressionLevel(configuration, sourceLocation);
        }
    }

    private static void validateMaxResult(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String maxResult = configuration.get(KEY_WRITER_MAX_RESULT);
        if (maxResult == null) {
            return;
        }

        try {
            int value = Integer.parseInt(maxResult);
            if (value < WRITER_MAX_RESULT_MINIMUM) {
                throw new CompilationException(MINIMUM_VALUE_ALLOWED_FOR_PARAM, KEY_WRITER_MAX_RESULT,
                        WRITER_MAX_RESULT_MINIMUM, value);
            }
        } catch (NumberFormatException e) {
            throw CompilationException.create(ErrorCode.INTEGER_VALUE_EXPECTED, sourceLocation, maxResult);
        }
    }

    private static void validateMaxParquetSchemas(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String maxResult = configuration.get(PARQUET_MAX_SCHEMAS_KEY);
        if (maxResult == null) {
            return;
        }

        try {
            int value = Integer.parseInt(maxResult);
            if (value > PARQUET_MAX_SCHEMAS_MAX_VALUE) {
                throw new CompilationException(MAXIMUM_VALUE_ALLOWED_FOR_PARAM, PARQUET_MAX_SCHEMAS_KEY,
                        PARQUET_MAX_SCHEMAS_MAX_VALUE, value);
            }
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
            throw new CompilationException(PARAMETERS_REQUIRED, sourceLocation, paramKey);
        }

        String normalizedValue = value.toLowerCase();
        if (!supportedSet.contains(normalizedValue)) {
            List<String> sorted = supportedSet.stream().sorted().collect(Collectors.toList());
            throw CompilationException.create(errorCode, sourceLocation, value, sorted.toString());
        }
    }

    private static void validateGzipCompressionLevel(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String compressionLevelStr = configuration.get(ExternalDataConstants.KEY_COMPRESSION_GZIP_COMPRESSION_LEVEL);
        if (compressionLevelStr == null) {
            return;
        }
        try {
            int compressionLevel = Integer.parseInt(compressionLevelStr);
            if (compressionLevel < 1 || compressionLevel > 9) {
                throw new CompilationException(INVALID_REQ_PARAM_VAL, sourceLocation,
                        ExternalDataConstants.KEY_COMPRESSION_GZIP_COMPRESSION_LEVEL, compressionLevelStr);
            }
        } catch (NumberFormatException e) {
            throw CompilationException.create(ErrorCode.INTEGER_VALUE_EXPECTED, sourceLocation, compressionLevelStr);
        }
    }

    private static void checkCompressionSupported(String paramKey, String value, Set<String> supportedSet,
            ErrorCode errorCode, SourceLocation sourceLocation, String format, boolean optional)
            throws CompilationException {
        if (optional && value == null) {
            return;
        }

        if (value == null) {
            throw new CompilationException(PARAMETERS_REQUIRED, sourceLocation, paramKey);
        }

        String normalizedValue = value.toLowerCase();
        if (!supportedSet.contains(normalizedValue)) {
            List<String> sorted = supportedSet.stream().sorted().collect(Collectors.toList());
            throw CompilationException.create(errorCode, sourceLocation, value, format, sorted.toString());
        }
    }

    private static void validateDelimiter(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        // Will this affect backward compatibility
        String delimiter = configuration.get(ExternalDataConstants.KEY_DELIMITER);
        unitByteCondition(delimiter, sourceLocation, ErrorCode.INVALID_DELIMITER);
    }

    private static void validateEscape(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        // Will this affect backward compatibility?
        String escape = configuration.get(ExternalDataConstants.KEY_ESCAPE);
        unitByteCondition(escape, sourceLocation, ErrorCode.INVALID_ESCAPE);
    }

    private static void validateRecordDelimiter(Map<String, String> configuration, SourceLocation sourceLocation)
            throws CompilationException {
        String recordDel = configuration.get(ExternalDataConstants.KEY_RECORD_DELIMITER);
        unitByteCondition(recordDel, sourceLocation, ErrorCode.INVALID_FORCE_QUOTE);
    }

    private static void unitByteCondition(String param, SourceLocation sourceLocation, ErrorCode errorCode)
            throws CompilationException {
        if (param != null && param.length() > 1 && param.getBytes().length != 1) {
            throw CompilationException.create(errorCode, sourceLocation, param);
        }
    }

}
