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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.application.IApplicationConfig;
import org.apache.hyracks.control.common.controllers.IniUtils;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;

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

    @Override
    public String getString(String section, String key) {
        return IniUtils.getString(ini, section, key, null);
    }

    @Override
    public String getString(String section, String key, String defaultValue) {
        return IniUtils.getString(ini, section, key, defaultValue);
    }

    @Override
    public String[] getStringArray(String section, String key) {
        return IniUtils.getStringArray(ini, section, key);
    }

    @Override
    public int getInt(String section, String key) {
        return IniUtils.getInt(ini, section, key, 0);
    }

    @Override
    public int getInt(String section, String key, int defaultValue) {
        return IniUtils.getInt(ini, section, key, defaultValue);
    }

    @Override
    public long getLong(String section, String key) {
        return IniUtils.getLong(ini, section, key, 0);
    }

    @Override
    public long getLong(String section, String key, long defaultValue) {
        return IniUtils.getLong(ini, section, key, defaultValue);
    }

    @Override
    public Set<String> getSections() {
        return ini.keySet();
    }

    @Override
    public Set<String> getKeys(String section) {
        return ini.get(section).keySet();
    }

    @Override
    public List<Set<Map.Entry<String, String>>> getMultiSections(String section) {
        List<Set<Map.Entry<String, String>>> list = new ArrayList<>();
        List<Section> secs = getMulti(section);
        if (secs != null) {
            for (Section sec : secs) {
                list.add(sec.entrySet());
            }
        }
        return list;
    }

    private List<Section> getMulti(String section) {
        return ini.getAll(section);
    }
}
