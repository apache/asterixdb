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
package org.apache.hyracks.api.config;

import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.logging.log4j.Level;

/**
 * Accessor for the data contained in the global application configuration file.
 */
public interface IApplicationConfig {
    String getString(String section, String key);

    int getInt(String section, String key);

    long getLong(String section, String key);

    Set<String> getSectionNames();

    Set<String> getKeys(String section);

    Object getStatic(IOption option);

    List<String> getNCNames();

    IOption lookupOption(String sectionName, String propertyName);

    Set<IOption> getOptions();

    Set<IOption> getOptions(Section section);

    IApplicationConfig getNCEffectiveConfig(String nodeId);

    Set<Section> getSections();

    Set<Section> getSections(Predicate<Section> predicate);

    default Object get(IOption option) {
        return option.get(this);
    }

    default long getLong(IOption option) {
        return (long) get(option);
    }

    default int getInt(IOption option) {
        return (int) get(option);
    }

    default short getShort(IOption option) {
        return (short) get(option);
    }

    default String getString(IOption option) {
        return (String) get(option);
    }

    default boolean getBoolean(IOption option) {
        return (boolean) get(option);
    }

    default Level getLoggingLevel(IOption option) {
        return (Level) get(option);
    }

    default double getDouble(IOption option) {
        return (double) get(option);
    }

    default String[] getStringArray(IOption option) {
        return (String[]) get(option);
    }

    default URL getURL(IOption option) {
        return (URL) get(option);
    }
}
