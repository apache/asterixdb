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
package org.apache.hyracks.control.common.application;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.ConfigManager;

/**
 * An implementation of IApplicationConfig which is backed by the Config Manager.
 */
public class ConfigManagerApplicationConfig implements IApplicationConfig, Serializable {
    private static final long serialVersionUID = 1L;

    private final ConfigManager configManager;

    public ConfigManagerApplicationConfig(ConfigManager configManager) {
        this.configManager = configManager;
    }

    @Override
    public String getString(String section, String key) {
        return (String) get(section, key);
    }

    @Override
    public int getInt(String section, String key) {
        return (int) get(section, key);
    }

    @Override
    public long getLong(String section, String key) {
        return (long) get(section, key);
    }

    @Override
    public Set<String> getSectionNames() {
        return configManager.getSectionNames();
    }

    @Override
    public Set<Section> getSections() {
        return configManager.getSections();
    }

    @Override
    public Set<Section> getSections(Predicate<Section> predicate) {
        return configManager.getSections(predicate);
    }

    @Override
    public Set<String> getKeys(String section) {
        return configManager.getOptionNames(section);
    }

    private Object get(String section, String key) {
        return get(configManager.lookupOption(section, key));
    }

    @Override
    public Object getStatic(IOption option) {
        return configManager.get(option);
    }

    @Override
    public List<String> getNCNames() {
        return configManager.getNodeNames();
    }

    @Override
    public IOption lookupOption(String sectionName, String propertyName) {
        return configManager.lookupOption(sectionName, propertyName);
    }

    @Override
    public Set<IOption> getOptions() {
        return configManager.getOptions();
    }

    @Override
    public Set<IOption> getOptions(Section section) {
        return configManager.getOptions(section);
    }

    @Override
    public IApplicationConfig getNCEffectiveConfig(String nodeId) {
        return configManager.getNodeEffectiveConfig(nodeId);
    }

    public ConfigManager getConfigManager() {
        return configManager;
    }
}
