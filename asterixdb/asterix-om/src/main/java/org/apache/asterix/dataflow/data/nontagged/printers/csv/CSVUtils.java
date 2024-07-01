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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.types.ARecordType;

public class CSVUtils {

    // Constants for the supported CSV parameters
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

    // List of supported parameters
    public static final List<String> CSV_PARAMETERS = Arrays.asList(KEY_NULL, KEY_ESCAPE, KEY_HEADER, KEY_DELIMITER,
            KEY_RECORD_DELIMITER, KEY_FORCE_QUOTE, KEY_QUOTE, KEY_EMPTY_FIELD_AS_NULL);

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

    // Generate a key based on configuration for ARecordType and parameters
    public static String generateKey(ARecordType itemType, Map<String, String> configuration) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(itemType == null ? KEY_NULL : itemType.toString()).append(" | ");
        // Iterate through supported CSV parameters and append their values from configuration
        for (String param : CSV_PARAMETERS) {
            String value = configuration.getOrDefault(param, DEFAULT_VALUES.get(param));
            keyBuilder.append(param).append(" : ").append(value).append(" | ");
        }
        // Remove the trailing " | "
        if (keyBuilder.length() > 0 && keyBuilder.charAt(keyBuilder.length() - 2) == '|') {
            keyBuilder.setLength(keyBuilder.length() - 3);
        }
        return keyBuilder.toString();
    }

    public static String generateKey(String quote, String forceQuoteStr, String escape, String delimiter) {
        // Use default values when no values are specified (null)
        return KEY_QUOTE + " : " + (quote != null ? quote : DEFAULT_VALUES.get(KEY_QUOTE)) + " | " + KEY_FORCE_QUOTE
                + " : " + (forceQuoteStr != null ? forceQuoteStr : DEFAULT_VALUES.get(KEY_FORCE_QUOTE)) + " | "
                + KEY_ESCAPE + " : " + (escape != null ? escape : DEFAULT_VALUES.get(KEY_ESCAPE)) + " | "
                + KEY_DELIMITER + " : " + (delimiter != null ? delimiter : DEFAULT_VALUES.get(KEY_DELIMITER));
    }

    public static String generateKey(String nullString) {
        // Use the default value when nullString is not specified (null)
        return KEY_NULL + " : " + (nullString != null ? nullString : DEFAULT_VALUES.get(KEY_NULL));
    }

    public static boolean isEmptyString(byte[] b, int s, int l) {
        return b == null || l <= 2 || s < 0 || s + l > b.length;
    }

    public static String getDelimiter(Map<String, String> configuration) {
        return configuration.get(KEY_DELIMITER) == null ? DEFAULT_DELIMITER_VALUE : configuration.get(KEY_DELIMITER);
    }
}
