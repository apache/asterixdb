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
package org.apache.hyracks.control.common.config;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.util.StorageUtil;
import org.apache.logging.log4j.Level;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OptionTypes {

    public static final IOptionType<Integer> INTEGER_BYTE_UNIT = new IOptionType<Integer>() {
        @Override
        public Integer parse(String s) {
            if (s == null) {
                return null;
            }
            long result1 = StorageUtil.getByteValue(s);
            if (result1 > Integer.MAX_VALUE || result1 < Integer.MIN_VALUE) {
                throw new IllegalArgumentException("The given value: " + result1 + " is not within the int range.");
            }
            return (int) result1;
        }

        @Override
        public Integer parse(JsonNode node) {
            return node.isNull() ? null : parse(node.asText());
        }

        @Override
        public Class<Integer> targetType() {
            return Integer.class;
        }

        @Override
        public String serializeToHumanReadable(Object value) {
            return value + " (" + StorageUtil.toHumanReadableSize((int) value) + ")";
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (int) value);
        }
    };

    public static final IOptionType<Long> LONG_BYTE_UNIT = new IOptionType<Long>() {
        @Override
        public Long parse(String s) {
            return s == null ? null : StorageUtil.getByteValue(s);
        }

        @Override
        public Long parse(JsonNode node) {
            return node.isNull() ? null : parse(node.asText());
        }

        @Override
        public Class<Long> targetType() {
            return Long.class;
        }

        @Override
        public String serializeToHumanReadable(Object value) {
            return value + " (" + StorageUtil.toHumanReadableSize((long) value) + ")";
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (long) value);
        }
    };

    public static final IOptionType<Short> SHORT = new IOptionType<Short>() {
        @Override
        public Short parse(String s) {
            int value = Integer.decode(s);
            return validateShort(value);
        }

        private Short validateShort(int value) {
            if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
                throw new IllegalArgumentException("The given value " + value + " does not fit in a short");
            }
            return (short) value;
        }

        @Override
        public Short parse(JsonNode node) {
            return node.isNull() ? null : validateShort(node.asInt());
        }

        @Override
        public Class<Short> targetType() {
            return Short.class;
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (short) value);
        }
    };

    public static final IOptionType<Integer> INTEGER = new IOptionType<Integer>() {
        @Override
        public Integer parse(String s) {
            return Integer.parseInt(s);
        }

        @Override
        public Integer parse(JsonNode node) {
            return node.isNull() ? null : node.asInt();
        }

        @Override
        public Class<Integer> targetType() {
            return Integer.class;
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (int) value);
        }
    };

    public static final IOptionType<Double> DOUBLE = new IOptionType<Double>() {
        @Override
        public Double parse(String s) {
            return Double.parseDouble(s);
        }

        @Override
        public Double parse(JsonNode node) {
            return node.isNull() ? null : node.asDouble();
        }

        @Override
        public Class<Double> targetType() {
            return Double.class;
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (double) value);
        }
    };

    public static final IOptionType<String> STRING = new IOptionType<String>() {
        @Override
        public String parse(String s) {
            return s;
        }

        @Override
        public String parse(JsonNode node) {
            return node.isNull() ? null : node.asText();
        }

        @Override
        public Class<String> targetType() {
            return String.class;
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (String) value);
        }
    };

    public static final IOptionType<Long> LONG = new IOptionType<Long>() {
        @Override
        public Long parse(String s) {
            return Long.parseLong(s);
        }

        @Override
        public Long parse(JsonNode node) {
            return node.isNull() ? null : node.asLong();
        }

        @Override
        public Class<Long> targetType() {
            return Long.class;
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (long) value);
        }
    };

    public static final IOptionType<Boolean> BOOLEAN = new IOptionType<Boolean>() {
        @Override
        public Boolean parse(String s) {
            return Boolean.parseBoolean(s);
        }

        @Override
        public Boolean parse(JsonNode node) {
            return node.isNull() ? null : node.asBoolean();
        }

        @Override
        public Class<Boolean> targetType() {
            return Boolean.class;
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (boolean) value);
        }
    };

    public static final IOptionType<Level> LEVEL = new IOptionType<Level>() {
        @Override
        public Level parse(String s) {
            if (s == null) {
                throw new IllegalArgumentException("Logging level cannot be null");
            }
            final Level level = Level.getLevel(s);
            if (level == null) {
                throw new IllegalArgumentException("Unrecognized logging level: " + s);
            }
            return level;
        }

        @Override
        public Level parse(JsonNode node) {
            return node.isNull() ? null : parse(node.asText());
        }

        @Override
        public Class<Level> targetType() {
            return Level.class;
        }

        @Override
        public String serializeToJSON(Object value) {
            return value == null ? null : ((Level) value).name();
        }

        @Override
        public String serializeToIni(Object value) {
            return ((Level) value).name();
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, serializeToJSON(value));
        }
    };

    public static final IOptionType<String[]> STRING_ARRAY = new IOptionType<String[]>() {
        @Override
        public String[] parse(String s) {
            return s == null ? null : s.split("\\s*,\\s*");
        }

        @Override
        public String[] parse(JsonNode node) {
            if (node.isNull()) {
                return null;
            }
            List<String> strings = new ArrayList<>();
            if (node instanceof ArrayNode) {
                node.elements().forEachRemaining(n -> strings.add(n.asText()));
                return strings.toArray(new String[strings.size()]);
            } else {
                return parse(node.asText());
            }
        }

        @Override
        public Class<String[]> targetType() {
            return String[].class;
        }

        @Override
        public String serializeToIni(Object value) {
            return String.join(",", (String[]) value);
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            if (value == null) {
                node.putNull(fieldName);
            } else {
                ArrayNode array = node.putArray(fieldName);
                Stream.of((String[]) value).forEachOrdered(array::add);
            }
        }
    };

    public static final IOptionType<java.net.URL> URL = new IOptionType<java.net.URL>() {
        @Override
        public java.net.URL parse(String s) {
            try {
                return s == null ? null : new java.net.URL(s);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public java.net.URL parse(JsonNode node) {
            return node.isNull() ? null : parse(node.asText());
        }

        @Override
        public Class<java.net.URL> targetType() {
            return java.net.URL.class;
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, value == null ? null : String.valueOf(value));
        }
    };

    public static final IOptionType<Integer> UNSIGNED_INTEGER = new IOptionType<Integer>() {
        @Override
        public Integer parse(String s) {
            return Integer.parseUnsignedInt(s);
        }

        @Override
        public Integer parse(JsonNode node) {
            return node.isNull() ? null : parse(node.asText());
        }

        @Override
        public Class<Integer> targetType() {
            return Integer.class;
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (int) value);
        }
    };

    public static final IOptionType<Integer> POSITIVE_INTEGER = new IOptionType<Integer>() {
        @Override
        public Integer parse(String s) {
            final int value = Integer.parseUnsignedInt(s);
            if (value == 0) {
                throw new IllegalArgumentException("Value must be greater than zero");
            }
            return value;
        }

        @Override
        public Integer parse(JsonNode node) {
            return node.isNull() ? null : parse(node.asText());
        }

        @Override
        public Class<Integer> targetType() {
            return Integer.class;
        }

        @Override
        public void serializeJSONField(String fieldName, Object value, ObjectNode node) {
            node.put(fieldName, (int) value);
        }
    };

    private OptionTypes() {
    }
}
