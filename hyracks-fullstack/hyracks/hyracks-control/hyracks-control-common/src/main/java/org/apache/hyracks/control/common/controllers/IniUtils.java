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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;

import org.ini4j.Ini;
import org.ini4j.Profile.Section;

/**
 * Some utility functions for reading Ini4j objects with default values.
 * For all getXxx() methods: if the 'section' contains a slash, and the 'key'
 * is not found in that section, we will search for the key in the section named
 * by stripping the leaf of the section name (final slash and anything following).
 * eg. getInt(ini, "nc/red", "dir", null) will first look for the key "dir" in
 * the section "nc/red", but if it is not found, will look in the section "nc".
 */
public class IniUtils {

    private IniUtils() {
    }

    private static <T> T getIniValue(Ini ini, String section, String key, T defaultValue, Class<T> clazz) {
        T value;
        while (true) {
            value = ini.get(section, key, clazz);
            if (value == null) {
                int idx = section.lastIndexOf('/');
                if (idx > -1) {
                    section = section.substring(0, idx);
                    continue;
                }
            }
            break;
        }
        return (value != null) ? value : defaultValue;
    }

    @SuppressWarnings("unchecked")
    private static <T> T getIniArray(Ini ini, String section, String key, Class<T> clazz) {
        Section sec = ini.get(section);
        if (clazz.getComponentType() == null) {
            return null;
        }
        if (sec == null) {
            return (T) Array.newInstance(clazz.getComponentType(), 0);
        } else {
            return sec.getAll(key, clazz);
        }
    }

    public static String getString(Ini ini, String section, String key, String defaultValue) {
        return getIniValue(ini, section, key, defaultValue, String.class);
    }

    public static String[] getStringArray(Ini ini, String section, String key) {
        return getIniArray(ini, section, key, String[].class);
    }

    public static int getInt(Ini ini, String section, String key, int defaultValue) {
        return getIniValue(ini, section, key, defaultValue, Integer.class);
    }

    public static long getLong(Ini ini, String section, String key, long defaultValue) {
        return getIniValue(ini, section, key, defaultValue, Long.class);
    }

    public static Ini loadINIFile(String configFile) throws IOException {
        Ini ini = new Ini();
        File conffile = new File(configFile);
        if (!conffile.exists()) {
            throw new FileNotFoundException(configFile);
        }
        ini.load(conffile);
        return ini;
    }
}
