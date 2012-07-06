/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.entities;

import java.io.Serializable;
import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;

/**
 * Metadata describing an index.
 */
public class Index implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String datasetName;
    // Enforced to be unique within a dataverse, dataset combination.
    private final String indexName;
    private final IndexType indexType;
    private final List<String> keyFieldNames;
    private final boolean isPrimaryIndex;
    // Specific to NGRAM indexes.
    private final int gramLength;

    public Index(String dataverseName, String datasetName, String indexName, IndexType indexType,
            List<String> keyFieldNames, int gramLength, boolean isPrimaryIndex) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.indexName = indexName;
        this.indexType = indexType;
        this.keyFieldNames = keyFieldNames;
        this.gramLength = gramLength;
        this.isPrimaryIndex = isPrimaryIndex;
    }

    public Index(String dataverseName, String datasetName, String indexName, IndexType indexType,
            List<String> keyFieldNames, boolean isPrimaryIndex) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.indexName = indexName;
        this.indexType = indexType;
        this.keyFieldNames = keyFieldNames;
        this.gramLength = -1;
        this.isPrimaryIndex = isPrimaryIndex;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getIndexName() {
        return indexName;
    }

    public List<String> getKeyFieldNames() {
        return keyFieldNames;
    }

    public int getGramLength() {
        return gramLength;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public boolean isPrimaryIndex() {
        return isPrimaryIndex;
    }

    public boolean isSecondaryIndex() {
        return !isPrimaryIndex();
    }
}
