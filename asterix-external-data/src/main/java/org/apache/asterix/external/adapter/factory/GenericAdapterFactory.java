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
package org.apache.asterix.external.adapter.factory;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.feeds.api.IDataSourceAdapter;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IDataFlowController;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IIndexibleExternalDataSource;
import org.apache.asterix.external.api.IIndexingAdapterFactory;
import org.apache.asterix.external.dataset.adapter.GenericAdapter;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.provider.DataflowControllerProvider;
import org.apache.asterix.external.provider.DatasourceFactoryProvider;
import org.apache.asterix.external.provider.ParserFactoryProvider;
import org.apache.asterix.external.util.ExternalDataCompatibilityUtils;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class GenericAdapterFactory implements IIndexingAdapterFactory, IAdapterFactory {

    private static final long serialVersionUID = 1L;
    private IExternalDataSourceFactory dataSourceFactory;
    private IDataParserFactory dataParserFactory;
    private ARecordType recordType;
    private Map<String, String> configuration;
    private List<ExternalFile> files;
    private boolean indexingOp;

    @Override
    public void setSnapshot(List<ExternalFile> files, boolean indexingOp) {
        this.files = files;
        this.indexingOp = indexingOp;
    }

    @Override
    public String getAlias() {
        return ExternalDataConstants.ALIAS_GENERIC_ADAPTER;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return dataSourceFactory.getPartitionConstraint();
    }

    /**
     * Runs on each node controller (after serialization-deserialization)
     */
    @Override
    public IDataSourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        IDataFlowController controller = DataflowControllerProvider.getDataflowController(recordType, ctx, partition,
                dataSourceFactory, dataParserFactory, configuration, indexingOp);
        return new GenericAdapter(controller);
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.recordType = outputType;
        this.configuration = configuration;
        dataSourceFactory = DatasourceFactoryProvider.getExternalDataSourceFactory(configuration);
        dataParserFactory = ParserFactoryProvider.getDataParserFactory(configuration);
        prepare();
        ExternalDataCompatibilityUtils.validateCompatibility(dataSourceFactory, dataParserFactory);
    }

    private void prepare() throws Exception {
        if (dataSourceFactory.isIndexible() && (files != null)) {
            ((IIndexibleExternalDataSource) dataSourceFactory).setSnapshot(files, indexingOp);
        }
        dataSourceFactory.configure(configuration);
        dataParserFactory.setRecordType(recordType);
        dataParserFactory.configure(configuration);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return recordType;
    }
}
