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

/** Installs collapse-detector settings for a test, as a node does from its storage options at startup. */
public final class CollapseSettings {
    /** What the harness hands the schemas it builds, as an index's column manager would. */
    private static CollapseDetectorSettings current = CollapseDetectorSettings.DEFAULT;

    private CollapseSettings() {
    }

    public static CollapseDetectorSettings current() {
        return current;
    }

    public static void install(CollapseDetectorSettings settings) {
        current = settings;
    }

    /** Collapse enabled, every threshold at its default. */
    public static void install() {
        install(Map.of());
    }

    /** Collapse enabled, {@code overrides} on top of the defaults. */
    public static void install(Map<Option, Object> overrides) {
        Map<Option, Object> values = new HashMap<>(overrides);
        values.putIfAbsent(Option.STORAGE_COLUMN_COLLAPSE_ENABLED, true);
        installKeepingFlag(values);
    }

    /** {@code overrides} on top of the defaults, the flag included: at its default unless they set it. */
    public static void installKeepingFlag(Map<Option, Object> overrides) {
        Map<Option, Object> values = Map.copyOf(overrides);
        current = new CollapseDetectorSettings(
                option -> values.containsKey(option) ? values.get(option) : option.defaultValue());
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

    /**
     * Collapse off, every threshold at its default: tests that do not exercise collapse expect the schema they wrote,
     * whatever the node default.
     */
    public static void reset() {
        installKeepingFlag(Map.of(Option.STORAGE_COLUMN_COLLAPSE_ENABLED, false));
    }
}
