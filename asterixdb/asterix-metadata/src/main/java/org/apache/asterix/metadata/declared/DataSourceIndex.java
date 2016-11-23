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

package org.apache.asterix.metadata.declared;

import org.apache.asterix.metadata.entities.Index;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;

public class DataSourceIndex implements IDataSourceIndex<String, DataSourceId> {

    private final Index index;
    private final String dataset;
    private final String dataverse;
    private final MetadataProvider metadataProvider;

    // Every transactions needs to work with its own instance of an
    // MetadataProvider.
    public DataSourceIndex(Index index, String dataverse, String dataset, MetadataProvider metadataProvider) {
        this.index = index;
        this.dataset = dataset;
        this.dataverse = dataverse;
        this.metadataProvider = metadataProvider;
    }

    // TODO: Maybe Index can directly implement IDataSourceIndex<String, DataSourceId>
    @Override
    public IDataSource<DataSourceId> getDataSource() {
        try {
            DataSourceId sourceId = new DataSourceId(dataverse, dataset);
            return metadataProvider.lookupSourceInMetadata(sourceId);
        } catch (Exception me) {
            return null;
        }
    }

    @Override
    public String getId() {
        return index.getIndexName();
    }

    public Index getIndex() {
        return index;
    }
}
