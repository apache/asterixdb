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
package org.apache.asterix.common.config;

import static org.apache.hyracks.control.common.config.OptionTypes.LONG;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER_BYTE_UNIT;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamWriteConstraints;

public class JacksonProperties extends AbstractProperties {

    public enum Option implements IOption {
        JSON_MAX_DEPTH(POSITIVE_INTEGER, StreamReadConstraints.DEFAULT_MAX_DEPTH),
        JSON_MAX_DOC_LENGTH(LONG_BYTE_UNIT, StreamReadConstraints.DEFAULT_MAX_DOC_LEN),
        JSON_MAX_TOKEN_COUNT(LONG, StreamReadConstraints.DEFAULT_MAX_TOKEN_COUNT),
        JSON_MAX_NUMBER_LENGTH(POSITIVE_INTEGER_BYTE_UNIT, StreamReadConstraints.DEFAULT_MAX_NUM_LEN),
        JSON_MAX_STRING_LENGTH(POSITIVE_INTEGER_BYTE_UNIT, Integer.MAX_VALUE),
        JSON_MAX_NAME_LENGTH(POSITIVE_INTEGER_BYTE_UNIT, StreamReadConstraints.DEFAULT_MAX_NAME_LEN);

        private final IOptionType type;
        private final Object defaultValue;

        Option(IOptionType type, Object defaultValue) {
            this.type = type;
            this.defaultValue = defaultValue;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            switch (this) {
                case JSON_MAX_DEPTH:
                    return "The maximum nesting depth for JSON objects. The depth is a count of objects and arrays that have not been closed, `{` and `[` respectively";
                case JSON_MAX_DOC_LENGTH:
                    return "The maximum length of a JSON document in bytes";
                case JSON_MAX_TOKEN_COUNT:
                    return "The maximum number of JSON tokens in a JSON object (<=0 is no limit). A token is a single unit of input, such as a number, a string, an object start or end, or an array start or end";
                case JSON_MAX_NUMBER_LENGTH:
                    return "The maximum length of a JSON number in bytes (<=0 is no limit)";
                case JSON_MAX_STRING_LENGTH:
                    return "The maximum length of a JSON string in bytes (<=0 is no limit)";
                case JSON_MAX_NAME_LENGTH:
                    return "The maximum length of a JSON name in bytes";
                default:
                    throw new IllegalStateException("NYI: " + this);
            }
        }

        @Override
        public IOptionType type() {
            return type;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }
    }

    private static final Logger LOGGER = LogManager.getLogger();

    static void configureJackson(PropertiesAccessor accessor) {
        StreamWriteConstraints writeConstraints =
                StreamWriteConstraints.builder().maxNestingDepth(accessor.getInt(Option.JSON_MAX_DEPTH)).build();
        StreamWriteConstraints.overrideDefaultStreamWriteConstraints(writeConstraints);
        StreamReadConstraints readConstraints =
                StreamReadConstraints.builder().maxNestingDepth(accessor.getInt(Option.JSON_MAX_DEPTH))
                        .maxDocumentLength(accessor.getLong(Option.JSON_MAX_DOC_LENGTH))
                        .maxTokenCount(accessor.getLong(Option.JSON_MAX_TOKEN_COUNT))
                        .maxNameLength(accessor.getInt(Option.JSON_MAX_NAME_LENGTH))
                        .maxStringLength(accessor.getInt(Option.JSON_MAX_STRING_LENGTH))
                        .maxNumberLength(accessor.getInt(Option.JSON_MAX_NUMBER_LENGTH)).build();
        StreamReadConstraints.overrideDefaultStreamReadConstraints(readConstraints);
        LOGGER.info("Configured Jackson read & write constraints: {{}}", Stream.of(Option.values())
                .map(option -> option.camelCase() + "=" + accessor.get(option)).collect(Collectors.joining(", ")));
    }

    public JacksonProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public int getJsonMaxDepth() {
        return accessor.getInt(Option.JSON_MAX_DEPTH);
    }

    public long getJsonMaxDocLength() {
        return accessor.getLong(Option.JSON_MAX_DOC_LENGTH);
    }

    public long getJsonMaxTokenCount() {
        return accessor.getLong(Option.JSON_MAX_TOKEN_COUNT);
    }

    public int getJsonMaxNumberLength() {
        return accessor.getInt(Option.JSON_MAX_NUMBER_LENGTH);
    }

    public int getJsonMaxStringLength() {
        return accessor.getInt(Option.JSON_MAX_STRING_LENGTH);
    }

    public int getJsonMaxNameLength() {
        return accessor.getInt(Option.JSON_MAX_NAME_LENGTH);
    }

}
