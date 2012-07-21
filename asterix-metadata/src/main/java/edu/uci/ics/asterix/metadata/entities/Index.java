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
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

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

    public static Pair<IAType, Boolean> getNonNullableKeyFieldType(String expr, ARecordType recType)
            throws AlgebricksException {
        IAType keyType = Index.keyFieldType(expr, recType);
        boolean nullable = false;
        if (keyType.getTypeTag() == ATypeTag.UNION) {
            AUnionType unionType = (AUnionType) keyType;
            if (unionType.isNullableType()) {
                // The non-null type is always at index 1.
                keyType = unionType.getUnionList().get(1);
                nullable = true;
            }
        }
        return new Pair<IAType, Boolean>(keyType, nullable);
    }

    private static IAType keyFieldType(String expr, ARecordType recType) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        for (int i = 0; i < n; i++) {
            if (names[i].equals(expr)) {
                return recType.getFieldTypes()[i];
            }
        }
        throw new AlgebricksException("Could not find field " + expr + " in the schema.");
    }
}
