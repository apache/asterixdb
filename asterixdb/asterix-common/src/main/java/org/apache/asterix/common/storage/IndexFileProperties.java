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
package org.apache.asterix.common.storage;

import java.io.File;

import org.apache.asterix.common.utils.StoragePathUtil;

/**
 * A holder class for an index file properties.
 */
public class IndexFileProperties {

    private final String fileName;
    private final String idxName;
    private final String dataverseName;
    private final int partitionId;
    private final int datasetId;

    public IndexFileProperties(int partitionId, String dataverseName, String idxName, String fileName, int datasetId) {
        this.partitionId = partitionId;
        this.dataverseName = dataverseName;
        this.idxName = idxName;
        this.fileName = fileName;
        this.datasetId = datasetId;
    }

    public String getFileName() {
        return fileName;
    }

    public String getIdxName() {
        return idxName;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getDatasetId() {
        return datasetId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(StoragePathUtil.PARTITION_DIR_PREFIX + partitionId + File.separator);
        sb.append(dataverseName + File.separator);
        sb.append(idxName + File.separator);
        sb.append(fileName);
        sb.append(" [Dataset ID: " + datasetId + "]");
        return sb.toString();
    }
}