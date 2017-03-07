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
import java.util.logging.Level;

import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.util.StorageUtil;

public class OptionTypes {

    public static final IOptionType<Integer> INTEGER_BYTE_UNIT = new IOptionType<Integer>() {
        @Override
        public Integer parse(String s) {
            long result1 = StorageUtil.getByteValue(s);
            if (result1 > Integer.MAX_VALUE || result1 < Integer.MIN_VALUE) {
                throw new IllegalArgumentException(
                        "The given value: " + result1 + " is not within the int range.");
            }
            return (int) result1;
        }

        @Override
        public Class<Integer> targetType() {
            return Integer.class;
        }
    };

    public static final IOptionType<Long> LONG_BYTE_UNIT = new IOptionType<Long>() {
        @Override
        public Long parse(String s) {
            return StorageUtil.getByteValue(s);
        }

        @Override
        public Class<Long> targetType() {
            return Long.class;
        }
    };

    public static final IOptionType<Integer> INTEGER = new IOptionType<Integer>() {
        @Override
        public Integer parse(String s) {
            return Integer.parseInt(s);
        }

        @Override
        public Class<Integer> targetType() {
            return Integer.class;
        }
    };

    public static final IOptionType<Double> DOUBLE = new IOptionType<Double>() {
        @Override
        public Double parse(String s) {
            return Double.parseDouble(s);
        }

        @Override
        public Class<Double> targetType() {
            return Double.class;
        }
    };

    public static final IOptionType<String> STRING = new IOptionType<String>() {
        @Override
        public String parse(String s) {
            return s;
        }

        @Override
        public Class<String> targetType() {
            return String.class;
        }
    };

    public static final IOptionType<Long> LONG = new IOptionType<Long>() {
        @Override
        public Long parse(String s) {
            return Long.parseLong(s);
        }

        @Override
        public Class<Long> targetType() {
            return Long.class;
        }
    };

    public static final IOptionType<Boolean> BOOLEAN = new IOptionType<Boolean>() {
        @Override
        public Boolean parse(String s) {
            return Boolean.parseBoolean(s);
        }

        @Override
        public Class<Boolean> targetType() {
            return Boolean.class;
        }
    };

    public static final IOptionType<Level> LEVEL = new IOptionType<Level>() {
        @Override
        public Level parse(String s) {
            return Level.parse(s);
        }

        @Override
        public Class<Level> targetType() {
            return Level.class;
        }

        @Override
        public Object serializeToJSON(Object value) {
            return ((Level)value).getName();
        }

        @Override
        public String serializeToIni(Object value) {
            return ((Level)value).getName();
        }
    };

    public static final IOptionType<String []> STRING_ARRAY = new IOptionType<String []>() {
        @Override
        public String [] parse(String s) {
            return s.split("\\s*,\\s*");
        }

        @Override
        public Class<String []> targetType() {
            return String [].class;
        }

        @Override
        public String serializeToIni(Object value) {
            return String.join(",", (String [])value);
        }
    };

    public static final IOptionType<java.net.URL> URL = new IOptionType<java.net.URL>() {
        @Override
        public java.net.URL parse(String s) {
            try {
                return new java.net.URL(s);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public Class<java.net.URL> targetType() {
            return java.net.URL.class;
        }
    };


    private OptionTypes() {
    }
}
