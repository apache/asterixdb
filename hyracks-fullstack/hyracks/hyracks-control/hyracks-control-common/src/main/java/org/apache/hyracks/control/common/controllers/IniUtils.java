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

import org.ini4j.Ini;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Some utility functions for reading Ini4j objects with default values.
 */
public class IniUtils {
    public static String getString(Ini ini, String section, String key, String defaultValue) {
        String value = ini.get(section, key, String.class);
        return (value != null) ? value : defaultValue;
    }

    public static int getInt(Ini ini, String section, String key, int defaultValue) {
        Integer value = ini.get(section, key, Integer.class);
        return (value != null) ? value : defaultValue;
    }

    public static long getLong(Ini ini, String section, String key, long defaultValue) {
        Long value = ini.get(section, key, Long.class);
        return (value != null) ? value : defaultValue;
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
