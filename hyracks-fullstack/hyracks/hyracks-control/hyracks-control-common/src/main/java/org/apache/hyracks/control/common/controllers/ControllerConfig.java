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
package org.apache.hyracks.control.common.controllers;

import static org.apache.hyracks.control.common.config.OptionTypes.BOOLEAN;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.function.Function;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.config.OptionTypes;
import org.apache.hyracks.control.common.utils.ConfigurationUtil;
import org.apache.hyracks.util.file.FileUtil;

public class ControllerConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Option implements IOption {
        CONFIG_FILE(OptionTypes.STRING, (String) null, "Specify path to master configuration file"),
        CONFIG_FILE_URL(OptionTypes.URL, (URL) null, "Specify URL to master configuration file"),
        DEFAULT_DIR(
                OptionTypes.STRING,
                FileUtil.joinPath(System.getProperty(ConfigurationUtil.JAVA_IO_TMPDIR), "hyracks"),
                "Directory where files are written to by default"),
        LOG_DIR(
                OptionTypes.STRING,
                (Function<IApplicationConfig, String>) appConfig -> FileUtil
                        .joinPath(appConfig.getString(ControllerConfig.Option.DEFAULT_DIR), "logs"),
                "The directory where logs for this node are written"),
        SSL_ENABLED(BOOLEAN, false, "A flag indicating if cluster communications should use secured connections");

        private final IOptionType type;
        private final String description;
        private Object defaultValue;

        <T> Option(IOptionType<T> type, T defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        <T> Option(IOptionType<T> type, Function<IApplicationConfig, T> defaultValue, String description) {
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

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }
    }

    protected final ConfigManager configManager;

    protected ControllerConfig(ConfigManager configManager) {
        this.configManager = configManager;
    }

    public IApplicationConfig getAppConfig() {
        return configManager.getAppConfig();
    }

    public ConfigManager getConfigManager() {
        return configManager;
    }

    public String getConfigFile() {
        return getAppConfig().getString(ControllerConfig.Option.CONFIG_FILE);
    }

    public void setConfigFile(String configFile) {
        configManager.set(ControllerConfig.Option.CONFIG_FILE, configFile);
    }

    public URL getConfigFileUrl() {
        return (URL) getAppConfig().get(ControllerConfig.Option.CONFIG_FILE_URL);
    }

    public void setConfigFileUrl(URL configFileUrl) {
        configManager.set(ControllerConfig.Option.CONFIG_FILE_URL, configFileUrl);
    }

    public String getLogDir() {
        String relPath = configManager.getAppConfig().getString(ControllerConfig.Option.LOG_DIR);
        String fullPath = new File(relPath).getAbsolutePath();
        return fullPath;
    }

    public boolean isSslEnabled() {
        return getAppConfig().getBoolean(Option.SSL_ENABLED);
    }
}
