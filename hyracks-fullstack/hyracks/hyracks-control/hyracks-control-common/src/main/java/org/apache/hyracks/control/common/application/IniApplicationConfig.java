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

import org.apache.hyracks.api.application.IApplicationConfig;
import org.ini4j.Ini;

import java.util.Set;

/**
 * An implementation of IApplicationConfig which is backed by Ini4j.
 */
public class IniApplicationConfig implements IApplicationConfig {
    private final Ini ini;

    public IniApplicationConfig(Ini ini) {
        if (ini != null) {
            this.ini = ini;
        } else {
            this.ini = new Ini();
        }
    }

    private <T> T getIniValue(String section, String key, T default_value, Class<T> clazz) {
        T value = ini.get(section, key, clazz);
        return (value != null) ? value : default_value;
    }

    @Override
    public String getString(String section, String key) {
        return getIniValue(section, key, null, String.class);
    }

    @Override
    public String getString(String section, String key, String defaultValue) {
        return getIniValue(section, key, defaultValue, String.class);
    }

    @Override
    public int getInt(String section, String key) {
        return getIniValue(section, key, 0, Integer.class);
    }

    @Override
    public int getInt(String section, String key, int defaultValue) {
        return getIniValue(section, key, defaultValue, Integer.class);
    }

    @Override
    public long getLong(String section, String key) {
        return getIniValue(section, key, (long) 0, Long.class);
    }

    @Override
    public long getLong(String section, String key, long defaultValue) {
        return getIniValue(section, key, defaultValue, Long.class);
    }

    @Override
    public Set<String> getSections() {
        return ini.keySet();
    }

    @Override
    public Set<String> getKeys(String section) {
        return ini.get(section).keySet();
    }
}
