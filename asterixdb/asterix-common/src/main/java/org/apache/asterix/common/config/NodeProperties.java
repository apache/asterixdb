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

import java.util.function.Function;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.OptionTypes;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.util.file.FileUtil;

public class NodeProperties extends AbstractProperties {

    public enum Option implements IOption {
        INITIAL_RUN(OptionTypes.BOOLEAN, false, "A flag indicating if it's the first time the NC is started"),
        CORE_DUMP_DIR(
                OptionTypes.STRING,
                appConfig -> FileUtil.joinPath(appConfig.getString(ControllerConfig.Option.DEFAULT_DIR), "coredump"),
                "The directory where node core dumps should be written",
                "<value of " + ControllerConfig.Option.DEFAULT_DIR.cmdline() + ">/coredump"),
        TXN_LOG_DIR(
                OptionTypes.STRING,
                appConfig -> FileUtil.joinPath(appConfig.getString(ControllerConfig.Option.DEFAULT_DIR), "txn-log"),
                "The directory where transaction logs should be stored",
                "<value of " + ControllerConfig.Option.DEFAULT_DIR.cmdline() + ">/txn-log"),
        STARTING_PARTITION_ID(
                OptionTypes.INTEGER,
                -1,
                "The first partition id to assign to iodevices on this node (-1 == auto-assign)");

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;
        private final String defaultValueDescription;

        <T> Option(IOptionType<T> type, T defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
            this.defaultValueDescription = null;
        }

        <T> Option(IOptionType<T> type, Function<IApplicationConfig, T> defaultValue, String description,
                String defaultValueDescription) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
            this.defaultValueDescription = defaultValueDescription;
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
        public String usageDefaultOverride(IApplicationConfig accessor, Function<IOption, String> optionPrinter) {
            return defaultValueDescription;
        }

        @Override
        public boolean hidden() {
            return this == INITIAL_RUN || this == STARTING_PARTITION_ID;
        }

    }

    public NodeProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public boolean isInitialRun() {
        return accessor.getBoolean(Option.INITIAL_RUN);
    }

    public boolean isVirtualNc() {
        return accessor.getInt(NCConfig.Option.NCSERVICE_PORT) == NCConfig.NCSERVICE_PORT_DISABLED;
    }

    public String getTxnLogDir() {
        return accessor.getString(Option.TXN_LOG_DIR);
    }
}
