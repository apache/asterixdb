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
package org.apache.asterix.dataflow.data.nontagged.printers.csv;

import java.io.IOException;
import java.io.PrintStream;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.nontagged.printers.PrintTools;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class CSVUtils {

    // Constants for the supported CSV parameters
    public static final String NONE = "none";
    public static final String KEY_NULL = "null";
    public static final String KEY_ESCAPE = "escape";
    public static final String KEY_HEADER = "header";
    public static final String KEY_DELIMITER = "delimiter";
    public static final String KEY_RECORD_DELIMITER = "recordDelimiter";
    public static final String KEY_FORCE_QUOTE = "forceQuote";
    public static final String KEY_QUOTE = "quote";
    public static final String KEY_EMPTY_FIELD_AS_NULL = "empty_field_as_null";
    public static final char DEFAULT_QUOTE = '"';
    private static final String DEFAULT_DELIMITER_VALUE = ",";
    private static final String DEFAULT_NULL_VALUE = "";
    private static final String DOUBLE_QUOTES = "\"";
    public static final char NULL_CHAR = '\0';
    private static final String FALSE = "false";
    private static final String DEFAULT_RECORD_DELIMITER = "\n";

    // Default values for each parameter
    public static final Map<String, String> DEFAULT_VALUES;

    static {
        DEFAULT_VALUES = new HashMap<>();
        DEFAULT_VALUES.put(KEY_NULL, DEFAULT_NULL_VALUE);
        DEFAULT_VALUES.put(KEY_ESCAPE, DOUBLE_QUOTES);
        DEFAULT_VALUES.put(KEY_HEADER, FALSE);
        DEFAULT_VALUES.put(KEY_DELIMITER, DEFAULT_DELIMITER_VALUE);
        DEFAULT_VALUES.put(KEY_RECORD_DELIMITER, DEFAULT_RECORD_DELIMITER);
        DEFAULT_VALUES.put(KEY_FORCE_QUOTE, FALSE);
        DEFAULT_VALUES.put(KEY_QUOTE, DOUBLE_QUOTES);
        DEFAULT_VALUES.put(KEY_EMPTY_FIELD_AS_NULL, FALSE);
    }

    public static boolean isEmptyString(byte[] b, int s, int l) {
        return b == null || l <= 2 || s < 0 || s + l > b.length;
    }

    public static boolean getHeader(Map<String, String> configuration) {
        return configuration.get(KEY_HEADER) != null && Boolean.parseBoolean(configuration.get(KEY_HEADER));
    }

    public static String getDelimiter(Map<String, String> configuration) {
        return configuration.get(KEY_DELIMITER) != null ? configuration.get(KEY_DELIMITER) : DEFAULT_DELIMITER_VALUE;
    }

    public static String getRecordDelimiter(Map<String, String> configuration, boolean itemTypeProvided) {
        return configuration.get(KEY_RECORD_DELIMITER) != null ? configuration.get(KEY_RECORD_DELIMITER)
                : (itemTypeProvided ? DEFAULT_RECORD_DELIMITER : "");
    }

    public static void validateSchema(ARecordType schema, EnumSet<ATypeTag> supportedTypes)
            throws CompilationException {
        for (IAType iaType : schema.getFieldTypes()) {
            ATypeTag typeTag = iaType.getTypeTag();
            if (iaType.getTypeTag().equals(ATypeTag.UNION)) {
                AUnionType unionType = (AUnionType) iaType;
                typeTag = unionType.getActualType().getTypeTag();
            }

            if (!supportedTypes.contains(typeTag)) {
                throw new CompilationException(ErrorCode.TYPE_UNSUPPORTED_CSV_WRITE, typeTag.toString());
            }
        }
    }

    public static void printString(byte[] b, int s, int l, PrintStream ps, char quote, boolean forceQuote, char escape,
            char delimiter) throws HyracksDataException {
        try {
            PrintTools.writeUTF8StringAsCSV(b, s + 1, l - 1, ps, quote, forceQuote, escape, delimiter);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void printNull(PrintStream ps, String nullString) {
        ps.print(nullString);
    }

    public static char getCharOrDefault(String value, String defaultValue) {
        return value != null ? extractSingleChar(value) : extractSingleChar(defaultValue);
    }

    private static char extractSingleChar(String input) {
        if (CSVUtils.NONE.equalsIgnoreCase(input)) {
            return CSVUtils.NULL_CHAR;
        } else {
            return input.charAt(0);
        }
    }
}
