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

import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.util.StorageUtil;

public class MessagingProperties extends AbstractProperties {

    public enum Option implements IOption {
        MESSAGING_FRAME_SIZE(
                INTEGER_BYTE_UNIT,
                StorageUtil.getIntSizeInBytes(4, KILOBYTE),
                "The frame size to be used for NC to NC messaging"),
        MESSAGING_FRAME_COUNT(POSITIVE_INTEGER, 512, "Number of reusable frames for NC to NC messaging");

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        Option(IOptionType type, Object defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            return description;
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

    public MessagingProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public int getFrameSize() {
        return accessor.getInt(Option.MESSAGING_FRAME_SIZE);
    }

    public int getFrameCount() {
        return accessor.getInt(Option.MESSAGING_FRAME_COUNT);
    }
}