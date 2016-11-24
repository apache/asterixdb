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

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IDataFlowController;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IIndexibleExternalDataSource;
import org.apache.asterix.external.api.IIndexingAdapterFactory;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.asterix.external.dataset.adapter.GenericAdapter;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.provider.DataflowControllerProvider;
import org.apache.asterix.external.provider.DatasourceFactoryProvider;
import org.apache.asterix.external.provider.ParserFactoryProvider;
import org.apache.asterix.external.util.ExternalDataCompatibilityUtils;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;

public class GenericAdapterFactory implements IIndexingAdapterFactory, IAdapterFactory {

    private static final long serialVersionUID = 1L;
    private IExternalDataSourceFactory dataSourceFactory;
    private IDataParserFactory dataParserFactory;
    private ARecordType recordType;
    private Map<String, String> configuration;
    private List<ExternalFile> files;
    private boolean indexingOp;
    private boolean isFeed;
    private FileSplit[] feedLogFileSplits;
    private ARecordType metaType;
    private FeedLogManager feedLogManager = null;

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
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint()
            throws HyracksDataException, AlgebricksException {
        return dataSourceFactory.getPartitionConstraint();
    }

    /**
     * Runs on each node controller (after serialization-deserialization)
     */
    @Override
    public synchronized IDataSourceAdapter createAdapter(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        try {
            restoreExternalObjects(runtimeCtx.getLibraryManager());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        if (isFeed) {
            if (feedLogManager == null) {
                feedLogManager = FeedUtils.getFeedLogManager(ctx, partition, feedLogFileSplits);
            }
            feedLogManager.touch();
        }
        IDataFlowController controller = DataflowControllerProvider.getDataflowController(recordType, ctx, partition,
                dataSourceFactory, dataParserFactory, configuration, indexingOp, isFeed, feedLogManager);
        if (isFeed) {
            return new FeedAdapter((AbstractFeedDataFlowController) controller);
        } else {
            return new GenericAdapter(controller);
        }
    }

    private void restoreExternalObjects(ILibraryManager libraryManager)
            throws HyracksDataException, AlgebricksException {
        if (dataSourceFactory == null) {
            dataSourceFactory = DatasourceFactoryProvider.getExternalDataSourceFactory(libraryManager, configuration);
            // create and configure parser factory
            if (dataSourceFactory.isIndexible() && (files != null)) {
                ((IIndexibleExternalDataSource) dataSourceFactory).setSnapshot(files, indexingOp);
            }
            dataSourceFactory.configure(configuration);
        }
        if (dataParserFactory == null) {
            // create and configure parser factory
            dataParserFactory = ParserFactoryProvider.getDataParserFactory(libraryManager, configuration);
            dataParserFactory.setRecordType(recordType);
            dataParserFactory.setMetaType(metaType);
            dataParserFactory.configure(configuration);
        }
    }

    @Override
    public void configure(ILibraryManager libraryManager, Map<String, String> configuration)
            throws HyracksDataException, AlgebricksException {
        this.configuration = configuration;
        ExternalDataUtils.validateDataSourceParameters(configuration);
        dataSourceFactory = DatasourceFactoryProvider.getExternalDataSourceFactory(libraryManager, configuration);
        if (dataSourceFactory.isIndexible() && (files != null)) {
            ((IIndexibleExternalDataSource) dataSourceFactory).setSnapshot(files, indexingOp);
        }
        dataSourceFactory.configure(configuration);
        ExternalDataUtils.validateDataParserParameters(configuration);
        dataParserFactory = ParserFactoryProvider.getDataParserFactory(libraryManager, configuration);
        dataParserFactory.setRecordType(recordType);
        dataParserFactory.setMetaType(metaType);
        dataParserFactory.configure(configuration);
        ExternalDataCompatibilityUtils.validateCompatibility(dataSourceFactory, dataParserFactory);
        configureFeedLogManager();
        nullifyExternalObjects();
    }

    private void configureFeedLogManager() throws HyracksDataException, AlgebricksException {
        this.isFeed = ExternalDataUtils.isFeed(configuration);
        if (isFeed) {
            feedLogFileSplits = FeedUtils.splitsForAdapter(ExternalDataUtils.getDataverse(configuration),
                    ExternalDataUtils.getFeedName(configuration), dataSourceFactory.getPartitionConstraint());
        }
    }

    private void nullifyExternalObjects() {
        if (ExternalDataUtils.isExternal(configuration.get(ExternalDataConstants.KEY_READER))) {
            dataSourceFactory = null;
        }
        if (ExternalDataUtils.isExternal(configuration.get(ExternalDataConstants.KEY_PARSER))) {
            dataParserFactory = null;
        }
    }

    @Override
    public ARecordType getOutputType() {
        return recordType;
    }

    @Override
    public void setOutputType(ARecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public ARecordType getMetaType() {
        return metaType;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        this.metaType = metaType;
    }

    public IExternalDataSourceFactory getDataSourceFactory() {
        return dataSourceFactory;
    }
}
