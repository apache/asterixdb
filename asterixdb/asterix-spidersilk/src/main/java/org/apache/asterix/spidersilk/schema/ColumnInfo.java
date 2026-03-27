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
package org.apache.asterix.spidersilk.schema;

/**
 * Holds metadata for a single field (column) within a Dataset.
 *
 * <p>Instances are created by {@link SchemaContextBuilder} while traversing the
 * AsterixDB ADM type tree, and consumed by:
 * <ul>
 *   <li>{@link DatasetSchema#toDescriptionString()} — rendered into a prompt-ready string</li>
 *   <li>ColumnPruner (PR-4) — scored for relevance and potentially filtered out</li>
 * </ul>
 */
public class ColumnInfo {

    private final String name;
    private final String type;
    private final boolean primaryKey;

    /**
     * @param name       field name as declared in the Dataset's item type
     * @param type       human-readable type string produced by {@link DatasetSchemaFormatter}
     * @param primaryKey {@code true} if this field is part of the Dataset's primary / partitioning key
     */
    public ColumnInfo(String name, String type, boolean primaryKey) {
        this.name = name;
        this.type = type;
        this.primaryKey = primaryKey;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    /**
     * Returns a compact description suitable for inclusion in an LLM prompt.
     * Example: {@code "tweetid: bigint [PK]"} or {@code "message-text: string"}
     */
    public String toDescriptionString() {
        return primaryKey ? name + ": " + type + " [PK]" : name + ": " + type;
    }

    @Override
    public String toString() {
        return "ColumnInfo{name='" + name + "', type='" + type + "', primaryKey=" + primaryKey + '}';
    }
}
