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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents the schema of a single AsterixDB Dataset, including all field
 * metadata extracted from its ADM item type.
 *
 * <p>This object travels through the three-layer schema injection pipeline:
 * <ol>
 *   <li>Created by {@link SchemaContextBuilder} with the full column list</li>
 *   <li>ColumnPruner (PR-4) calls {@link #setPrunedColumns} to store a filtered subset</li>
 *   <li>ValueHintsSampler (PR-5) calls {@link #setValueHints} to attach sample values</li>
 *   <li>{@link #toDescriptionString()} is called by PromptBuilder to produce the final
 *       prompt fragment for this Dataset</li>
 * </ol>
 *
 * <p>Example output of {@link #toDescriptionString()}:
 * <pre>
 * Dataset TweetMessages (tweetid: bigint [PK], sender-location: point,
 *     send-time: datetime, referred-topics: [string], message-text: string, author-id: bigint)
 * </pre>
 */
public class DatasetSchema {

    private final String datasetName;
    private final List<ColumnInfo> allColumns;

    /** Set by ColumnPruner; null until pruning is performed. */
    private List<ColumnInfo> prunedColumns;

    /** Set by ValueHintsSampler; null until sampling is performed. Key = field name. */
    private Map<String, List<String>> valueHints;

    public DatasetSchema(String datasetName, List<ColumnInfo> allColumns) {
        this.datasetName = datasetName;
        this.allColumns = Collections.unmodifiableList(new java.util.ArrayList<>(allColumns));
    }

    public String getDatasetName() {
        return datasetName;
    }

    /** Returns the complete column list as extracted from the ADM item type. */
    public List<ColumnInfo> getAllColumns() {
        return allColumns;
    }

    /**
     * Returns the pruned column list if {@link #setPrunedColumns} has been called,
     * otherwise falls back to the full list.
     */
    public List<ColumnInfo> getEffectiveColumns() {
        return prunedColumns != null ? prunedColumns : allColumns;
    }

    /** Called by ColumnPruner (PR-4) to store the relevance-filtered subset. */
    public void setPrunedColumns(List<ColumnInfo> prunedColumns) {
        this.prunedColumns = prunedColumns;
    }

    /** Called by ValueHintsSampler (PR-5) to attach sample values per field. */
    public void setValueHints(Map<String, List<String>> valueHints) {
        this.valueHints = valueHints;
    }

    public Map<String, List<String>> getValueHints() {
        return valueHints;
    }

    /**
     * Renders this Dataset's schema as a prompt-ready one-liner.
     * Uses the pruned column list if available, otherwise the full column list.
     *
     * <p>Example:
     * <pre>Dataset TweetMessages (tweetid: bigint [PK], message-text: string, author-id: bigint)</pre>
     */
    public String toDescriptionString() {
        List<ColumnInfo> columns = getEffectiveColumns();
        StringBuilder sb = new StringBuilder("Dataset ").append(datasetName).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).toDescriptionString());
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public String toString() {
        return "DatasetSchema{name='" + datasetName + "', columns=" + allColumns.size() + '}';
    }
}
