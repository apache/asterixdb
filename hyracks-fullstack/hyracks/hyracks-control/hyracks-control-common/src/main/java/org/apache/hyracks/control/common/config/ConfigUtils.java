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
package org.apache.hyracks.control.common.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.config.SerializedOption;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.ini4j.Ini;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerFilter;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Some utility functions for reading Ini4j objects with default values.
 * For all getXxx() methods: if the 'section' contains a slash, and the 'key'
 * is not found in that section, we will search for the key in the section named
 * by stripping the leaf of the section name (final slash and anything following).
 * eg. getInt(ini, "nc/red", "dir", null) will first look for the key "dir" in
 * the section "nc/red", but if it is not found, will look in the section "nc".
 */
public class ConfigUtils {
    private static final int USAGE_WIDTH = 120;

    private ConfigUtils() {
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
            return (value != null) ? value : defaultValue;
        }
    }

    public static String getString(Ini ini, String section, String key, String defaultValue) {
        return getIniValue(ini, section, key, defaultValue, String.class);
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

    public static Ini loadINIFile(URL configURL) throws IOException {
        Ini ini = new Ini();
        ini.load(configURL);
        return ini;
    }

    public static Field[] getFields(final Class beanClass, Predicate<Field> predicate) {
        List<Field> fields = new ArrayList<>();
        for (Class clazz = beanClass; clazz != null && clazz.getClassLoader() != null
                && clazz.getClassLoader().getParent() != null; clazz = clazz.getSuperclass()) {
            for (Field f : clazz.getDeclaredFields()) {
                if (predicate.test(f)) {
                    fields.add(f);
                }
            }
        }
        return fields.toArray(new Field[fields.size()]);
    }

    public static void printUsage(CmdLineParser parser, OptionHandlerFilter filter, PrintStream out) {
        parser.getProperties().withUsageWidth(USAGE_WIDTH);
        parser.printUsage(new OutputStreamWriter(out), null, filter);
    }

    public static void printUsage(CmdLineException e, OptionHandlerFilter filter, PrintStream out) {
        out.println("ERROR: " + e.getMessage());
        printUsage(e.getParser(), filter, out);
    }

    private static String getOptionValue(String[] args, String optionName) {
        for (int i = 0; i < (args.length - 1); i++) {
            if (args[i].equals(optionName)) {
                return args[i + 1];
            }
        }
        return null;
    }

    public static String getOptionValue(String[] args, IOption option) throws IOException {
        String value = getOptionValue(args, option.cmdline());
        if (value == null) {
            Ini iniFile = null;
            String configFile = getOptionValue(args, ControllerConfig.Option.CONFIG_FILE.cmdline());
            String configFileUrl = getOptionValue(args, ControllerConfig.Option.CONFIG_FILE_URL.cmdline());
            if (configFile != null) {
                iniFile = loadINIFile(configFile);
            } else if (configFileUrl != null) {
                iniFile = loadINIFile(new URL(configFileUrl));
            }
            if (iniFile != null) {
                value = iniFile.get(option.section().sectionName(), option.ini());
            }
        }
        return value;
    }

    public static String getString(Ini ini, Section section, IOption option, String defaultValue) {
        return getString(ini, section.sectionName(), option.ini(), defaultValue);
    }

    public static void addConfigToJSON(ObjectNode o, IApplicationConfig cfg, Section... sections) {
        ArrayNode configArray = o.putArray("config");
        for (Section section : cfg.getSections(Arrays.asList(sections)::contains)) {
            ObjectNode sectionNode = configArray.addObject();
            Map<String, Object> sectionConfig = getSectionOptionsForJSON(cfg, section, option -> true);
            sectionNode.put("section", section.sectionName()).putPOJO("properties", sectionConfig);
        }
    }

    public static Map<String, Object> getSectionOptionsForJSON(IApplicationConfig cfg, Section section,
            Predicate<IOption> selector) {
        Map<String, Object> sectionConfig = new TreeMap<>();
        for (IOption option : cfg.getOptions(section)) {
            if (selector.test(option)) {
                sectionConfig.put(option.ini(), option.type().serializeToJSON(cfg.get(option)));
            }
        }
        return sectionConfig;
    }

    public static void addConfigToJSON(ObjectNode o, Map<SerializedOption, Object> config, ConfigManager configManager,
            Section... sections) {
        ArrayNode configArray = o.putArray("config");
        for (Section section : sections) {
            ObjectNode sectionNode = configArray.addObject();
            Map<String, Object> sectionConfig = new TreeMap<>();
            config.entrySet().stream().filter(e -> e.getKey().section().equals(section)).forEach(entry -> sectionConfig
                    .put(IOption.toIni(entry.getKey().optionName()), fixupValueForJSON(entry, configManager)));
            if (!sectionConfig.isEmpty()) {
                sectionNode.put("section", section.sectionName()).putPOJO("properties", sectionConfig);
            }
        }
    }

    private static Object fixupValueForJSON(Map.Entry<SerializedOption, Object> entry, ConfigManager configManager) {
        IOption option = configManager.lookupOption(entry.getKey());
        if (option != null) {
            // use the type system for the option to serialize this
            return option.type().serializeToJSON(entry.getValue());
        }
        // not much we can do, let default JSON serialization do its thing
        return entry.getValue();
    }
}
