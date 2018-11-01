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

import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.util.StorageUtil;

public class ActiveProperties extends AbstractProperties {

    public enum Option implements IOption {
        ACTIVE_MEMORY_GLOBAL_BUDGET(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(64L, MEGABYTE),
                "The memory budget (in bytes) for the active runtime"),
        ACTIVE_STOP_TIMEOUT(
                POSITIVE_INTEGER,
                3600,
                "The maximum time to wait for a graceful stop of an active runtime"),
        ACTIVE_SUSPEND_TIMEOUT(
                POSITIVE_INTEGER,
                3600,
                "The maximum time to wait for a graceful suspend of an active runtime");

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

    public ActiveProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public long getMemoryComponentGlobalBudget() {
        return accessor.getLong(Option.ACTIVE_MEMORY_GLOBAL_BUDGET);
    }

    public int getActiveStopTimeout() {
        return accessor.getInt(Option.ACTIVE_STOP_TIMEOUT);
    }

    public int getActiveSuspendTimeout() {
        return accessor.getInt(Option.ACTIVE_SUSPEND_TIMEOUT);
    }
}
