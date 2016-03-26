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
package org.apache.asterix.external.input.record.reader.stream;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.api.IIndexibleExternalDataSource;
import org.apache.asterix.external.api.IIndexingDatasource;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractStreamRecordReaderFactory<T>
        implements IRecordReaderFactory<T>, IIndexibleExternalDataSource {

    private static final long serialVersionUID = 1L;
    protected IInputStreamFactory inputStreamFactory;
    protected Map<String, String> configuration;

    public AbstractStreamRecordReaderFactory<T> setInputStreamFactoryProvider(
            IInputStreamFactory inputStreamFactory) {
        this.inputStreamFactory = inputStreamFactory;
        return this;
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AsterixException {
        return inputStreamFactory.getPartitionConstraint();
    }

    @Override
    public void configure(Map<String, String> configuration) throws AsterixException {
        this.configuration = configuration;
        inputStreamFactory.configure(configuration);
    }

    @Override
    public boolean isIndexible() {
        return inputStreamFactory.isIndexible();
    }

    @Override
    public void setSnapshot(List<ExternalFile> files, boolean indexingOp) {
        ((IIndexibleExternalDataSource) inputStreamFactory).setSnapshot(files, indexingOp);
    }

    @Override
    public boolean isIndexingOp() {
        if (inputStreamFactory.isIndexible()) {
            return ((IIndexibleExternalDataSource) inputStreamFactory).isIndexingOp();
        }
        return false;
    }

    protected Pair<AsterixInputStream, IExternalIndexer> getStreamAndIndexer(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        AsterixInputStream inputStream = inputStreamFactory.createInputStream(ctx, partition);
        IExternalIndexer indexer = null;
        if (inputStreamFactory.isIndexible()) {
            if (((IIndexibleExternalDataSource) inputStreamFactory).isIndexingOp()) {
                indexer = ((IIndexingDatasource) inputStream).getIndexer();
            }
        }
        return new Pair<AsterixInputStream, IExternalIndexer>(inputStream, indexer);
    }
}
