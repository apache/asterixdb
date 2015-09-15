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

public class AqlIndex implements IDataSourceIndex<String, AqlSourceId> {

    private final Index index;
    private final String dataset;
    private final String dataverse;
    private final AqlMetadataProvider metadataProvider;

    // Every transactions needs to work with its own instance of an
    // AqlMetadataProvider.
    public AqlIndex(Index index, String dataverse, String dataset, AqlMetadataProvider metadatProvider) {
        this.index = index;
        this.dataset = dataset;
        this.dataverse = dataverse;
        this.metadataProvider = metadatProvider;
    }

    // TODO: Maybe Index can directly implement IDataSourceIndex<String, AqlSourceId>
    @Override
    public IDataSource<AqlSourceId> getDataSource() {
        try {
            AqlSourceId asid = new AqlSourceId(dataverse, dataset);
            return metadataProvider.lookupSourceInMetadata(asid);
        } catch (Exception me) {
            return null;
        }
    }

    @Override
    public String getId() {
        return index.getIndexName();
    }

}
