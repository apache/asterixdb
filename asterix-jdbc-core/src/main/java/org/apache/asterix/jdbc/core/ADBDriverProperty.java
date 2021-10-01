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

package org.apache.asterix.jdbc.core;

import java.util.Objects;
import java.util.function.Function;

interface ADBDriverProperty {

    String getPropertyName();

    Function<String, ?> getValueParser();

    Object getDefaultValue();

    enum Common implements ADBDriverProperty {

        USER("user", Function.identity(), null),
        PASSWORD("password", Function.identity(), null),
        CONNECT_TIMEOUT("connectTimeout", Integer::parseInt, null),
        SOCKET_TIMEOUT("socketTimeout", Integer::parseInt, null),
        MAX_WARNINGS("maxWarnings", Integer::parseInt, 10);

        private final String propertyName;

        private final Function<String, ?> valueParser;

        private final Object defaultValue;

        Common(String propertyName, Function<String, ?> valueParser, Object defaultValue) {
            this.propertyName = Objects.requireNonNull(propertyName);
            this.valueParser = Objects.requireNonNull(valueParser);
            this.defaultValue = defaultValue;
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        public Function<String, ?> getValueParser() {
            return valueParser;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        @Override
        public String toString() {
            return getPropertyName();
        }
    }
}
