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

import java.io.Serializable;
import java.net.URL;

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
        CONFIG_FILE(OptionTypes.STRING, "Specify path to master configuration file", null),
        CONFIG_FILE_URL(OptionTypes.URL, "Specify URL to master configuration file", null),
        DEFAULT_DIR(
                OptionTypes.STRING,
                "Directory where files are written to by default",
                FileUtil.joinPath(System.getProperty(ConfigurationUtil.JAVA_IO_TMPDIR), "hyracks")),;

        private final IOptionType type;
        private final String description;
        private String defaultValue;

        Option(IOptionType type, String description, String defaultValue) {
            this.type = type;
            this.description = description;
            this.defaultValue = defaultValue;
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
}
