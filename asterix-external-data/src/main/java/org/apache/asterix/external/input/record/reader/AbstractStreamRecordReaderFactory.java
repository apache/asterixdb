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
package org.apache.asterix.external.input.record.reader;

import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.api.IIndexibleExternalDataSource;
import org.apache.asterix.external.api.IIndexingDatasource;
import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.api.IInputStreamProviderFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public abstract class AbstractStreamRecordReaderFactory<T>
        implements IRecordReaderFactory<T>, IIndexibleExternalDataSource {

    private static final long serialVersionUID = 1L;
    protected IInputStreamProviderFactory inputStreamFactory;
    protected Map<String, String> configuration;

    public AbstractStreamRecordReaderFactory<T> setInputStreamFactoryProvider(
            IInputStreamProviderFactory inputStreamFactory) {
        this.inputStreamFactory = inputStreamFactory;
        return this;
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return inputStreamFactory.getPartitionConstraint();
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
        inputStreamFactory.configure(configuration);
        configureStreamReaderFactory(configuration);
    }

    protected abstract void configureStreamReaderFactory(Map<String, String> configuration) throws Exception;

    @Override
    public boolean isIndexible() {
        return inputStreamFactory.isIndexible();
    }

    @Override
    public void setSnapshot(List<ExternalFile> files, boolean indexingOp) throws Exception {
        ((IIndexibleExternalDataSource) inputStreamFactory).setSnapshot(files, indexingOp);
    }

    @Override
    public boolean isIndexingOp() {
        if (inputStreamFactory.isIndexible()) {
            return ((IIndexibleExternalDataSource) inputStreamFactory).isIndexingOp();
        }
        return false;
    }

    protected IRecordReader<char[]> configureReader(AbstractStreamRecordReader recordReader, IHyracksTaskContext ctx,
            int partition) throws Exception {
        IInputStreamProvider inputStreamProvider = inputStreamFactory.createInputStreamProvider(ctx, partition);
        IExternalIndexer indexer = null;
        if (inputStreamFactory.isIndexible()) {
            if (((IIndexibleExternalDataSource) inputStreamFactory).isIndexingOp()) {
                indexer = ((IIndexingDatasource) inputStreamProvider).getIndexer();
            }
        }
        recordReader.setInputStream(inputStreamProvider.getInputStream());
        recordReader.setIndexer(indexer);
        recordReader.configure(configuration);
        return recordReader;
    }
}
