/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import java.util.Map;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a dataset.
 */
public class Dataset implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String datasetName;
    // Type of items stored in this dataset.
    private final String itemTypeName;
    private final DatasetType datasetType;
    private final IDatasetDetails datasetDetails;
    // Hints related to cardinatlity of dataset, avg size of tuples etc.
    private final Map<String, String> hints;
    private final int datasetId;
    // Type of pending operations with respect to atomic DDL operation
    private int pendingOp;

    public Dataset(String dataverseName, String datasetName, String itemTypeName, IDatasetDetails datasetDetails,
            Map<String, String> hints, DatasetType datasetType, int datasetId, int pendingOp) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.itemTypeName = itemTypeName;
        this.datasetType = datasetType;
        this.datasetDetails = datasetDetails;
        this.datasetId = datasetId;
        this.pendingOp = pendingOp;
        this.hints = hints;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getItemTypeName() {
        return itemTypeName;
    }

    public DatasetType getDatasetType() {
        return datasetType;
    }

    public IDatasetDetails getDatasetDetails() {
        return datasetDetails;
    }

    public Map<String, String> getHints() {
        return hints;
    }

    public int getDatasetId() {
        return datasetId;
    }

    public int getPendingOp() {
        return pendingOp;
    }

    public void setPendingOp(int pendingOp) {
        this.pendingOp = pendingOp;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addDatasetIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropDataset(this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Dataset)) {
            return false;
        }
        Dataset otherDataset = (Dataset) other;
        if (!otherDataset.dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!otherDataset.datasetName.equals(datasetName)) {
            return false;
        }
        return true;
    }
}