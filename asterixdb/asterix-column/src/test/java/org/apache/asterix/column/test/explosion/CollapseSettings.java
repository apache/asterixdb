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
package org.apache.asterix.column.test.explosion;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.column.metadata.schema.CollapseDetectorSettings;
import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.control.common.config.ConfigManager;

/** Installs collapse-detector settings for a test, as a node's storage options set them for its indexes. */
public final class CollapseSettings {
    /** The installed option values; null for every option at its default. */
    private static Map<Option, Object> values;

    private CollapseSettings() {
    }

    /** The installed settings, for the schemas a test builds itself, as an index's column manager hands them out. */
    public static CollapseDetectorSettings current() {
        return new CollapseDetectorSettings(CollapseSettings::value);
    }

    /**
     * Sets the installed values on {@code configManager}, registering the storage options as a node does, for an index
     * a test builds through {@code LSMColumnBTreeUtil}, which reads its settings from the node's configuration.
     */
    public static void configure(ConfigManager configManager) {
        configManager.register(Option.values());
        for (Option option : Option.values()) {
            if (option.name().startsWith("STORAGE_COLUMN_COLLAPSE_")) {
                configManager.set(option, value(option));
            }
        }
    }

    /** Collapse enabled, every threshold at its default. */
    public static void install() {
        install(Map.of());
    }

    /** Collapse enabled, {@code overrides} on top of the defaults. */
    public static void install(Map<Option, Object> overrides) {
        Map<Option, Object> installed = new HashMap<>(overrides);
        installed.putIfAbsent(Option.STORAGE_COLUMN_COLLAPSE_ENABLED, true);
        values = installed;
    }

    /** {@code overrides} on top of the defaults, the flag included: at its default unless they set it. */
    public static void installKeepingFlag(Map<Option, Object> overrides) {
        values = new HashMap<>(overrides);
    }

    /**
     * Collapse off, every threshold at its default: tests that do not exercise collapse expect the schema they wrote,
     * whatever the node default.
     */
    public static void reset() {
        values = Map.of(Option.STORAGE_COLUMN_COLLAPSE_ENABLED, false);
    }

    /** The installed values, for {@link #restore}. */
    static Map<Option, Object> saved() {
        return values;
    }

    static void restore(Map<Option, Object> saved) {
        values = saved;
    }

    /**
     * Thresholds low enough for a few dozen small documents to collapse: windows of eight visits, four new names make
     * one fast, and {@code growingWindows} fast windows collapse a node of {@code minChildren} children.
     */
    public static Map<Option, Object> lowered(int minChildren, int growingWindows) {
        return Map.of(Option.STORAGE_COLUMN_COLLAPSE_MIN_CHILDREN, minChildren,
                Option.STORAGE_COLUMN_COLLAPSE_WINDOW_DOCUMENTS, 8, Option.STORAGE_COLUMN_COLLAPSE_WINDOW_NEW_NAMES, 4,
                Option.STORAGE_COLUMN_COLLAPSE_GROWING_WINDOWS, growingWindows);
    }

    private static Object value(IOption option) {
        return values != null && values.containsKey(option) ? values.get(option) : option.defaultValue();
    }
}
