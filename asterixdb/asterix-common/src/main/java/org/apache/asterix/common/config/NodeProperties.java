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

import java.util.function.Supplier;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.OptionTypes;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.util.file.FileUtil;

public class NodeProperties extends AbstractProperties {

    public enum Option implements IOption {
        INITIAL_RUN(OptionTypes.BOOLEAN, false, "A flag indicating if it's the first time the NC is started"),
        CORE_DUMP_DIR(
                OptionTypes.STRING,
                (Supplier<String>) () -> FileUtil.joinPath(NCConfig.defaultDir, "coredump"),
                "The directory where node core dumps should be written"),
        TXN_LOG_DIR(
                OptionTypes.STRING,
                (Supplier<String>) () -> FileUtil.joinPath(NCConfig.defaultDir, "txn-log"),
                "The directory where transaction logs should be stored"),
        STORAGE_SUBDIR(OptionTypes.STRING, "storage", "The subdirectory name under each iodevice used for storage"),
        ;

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        <T> Option(IOptionType<T> type, T defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        <T> Option(IOptionType<T> type, Supplier<T> defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            return Section.NC;
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

        @Override
        public boolean hidden() {
            return this == INITIAL_RUN;
        }
    }

    public NodeProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public boolean isInitialRun() {
        return accessor.getBoolean(Option.INITIAL_RUN);
    }

    public boolean isVirtualNc() {
        return accessor.getBoolean(NCConfig.Option.VIRTUAL_NC);
    }
}
