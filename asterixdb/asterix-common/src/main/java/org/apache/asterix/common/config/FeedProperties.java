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

import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.util.StorageUtil;

public class FeedProperties extends AbstractProperties {

    public enum Option implements IOption {
        FEED_PENDING_WORK_THRESHOLD(INTEGER, 50),
        FEED_MEMORY_GLOBAL_BUDGET(LONG_BYTE_UNIT, StorageUtil.getLongSizeInBytes(64L, MEGABYTE)),
        FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT(LONG, 10L),
        FEED_CENTRAL_MANAGER_PORT(INTEGER, 4500),
        FEED_MAX_THRESHOLD_PERIOD(INTEGER, 5);

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
            // TODO(mblow): add missing descriptions
            switch (this) {
                case FEED_CENTRAL_MANAGER_PORT:
                    return "port at which the Central Feed Manager listens for control messages from local Feed " +
                            "Managers";
                case FEED_MAX_THRESHOLD_PERIOD:
                    return "maximum length of input queue before triggering corrective action";
                default:
                    return null;
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

    public FeedProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public int getPendingWorkThreshold() {
        return accessor.getInt(Option.FEED_PENDING_WORK_THRESHOLD);
    }

    public long getMemoryComponentGlobalBudget() {
        return accessor.getLong(Option.FEED_MEMORY_GLOBAL_BUDGET);
    }

    public long getMemoryAvailableWaitTimeout() {
        return accessor.getLong(Option.FEED_MEMORY_AVAILABLE_WAIT_TIMEOUT);
    }

    public int getFeedCentralManagerPort() {
        return accessor.getInt(Option.FEED_CENTRAL_MANAGER_PORT);
    }

    public int getMaxSuccessiveThresholdPeriod() {
        return accessor.getInt(Option.FEED_MAX_THRESHOLD_PERIOD);
    }
}
