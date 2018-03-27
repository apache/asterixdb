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
package org.apache.asterix.external.provider;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IDataFlowController;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IIndexingDatasource;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.api.IRecordWithMetadataParser;
import org.apache.asterix.external.api.IRecordWithPKDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.api.IStreamDataParserFactory;
import org.apache.asterix.external.dataflow.ChangeFeedDataFlowController;
import org.apache.asterix.external.dataflow.ChangeFeedWithMetaDataFlowController;
import org.apache.asterix.external.dataflow.FeedRecordDataFlowController;
import org.apache.asterix.external.dataflow.FeedStreamDataFlowController;
import org.apache.asterix.external.dataflow.FeedWithMetaDataFlowController;
import org.apache.asterix.external.dataflow.IndexingDataFlowController;
import org.apache.asterix.external.dataflow.RecordDataFlowController;
import org.apache.asterix.external.dataflow.StreamDataFlowController;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class DataflowControllerProvider {

    // TODO: Instead, use a factory just like data source and data parser.
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static IDataFlowController getDataflowController(ARecordType recordType, IHyracksTaskContext ctx,
            int partition, IExternalDataSourceFactory dataSourceFactory, IDataParserFactory dataParserFactory,
            Map<String, String> configuration, boolean indexingOp, boolean isFeed, FeedLogManager feedLogManager)
            throws HyracksDataException {
        try {
            switch (dataSourceFactory.getDataSourceType()) {
                case RECORDS:
                    IRecordReaderFactory<?> recordReaderFactory = (IRecordReaderFactory<?>) dataSourceFactory;
                    IRecordReader<?> recordReader = recordReaderFactory.createRecordReader(ctx, partition);
                    IRecordDataParserFactory<?> recordParserFactory = (IRecordDataParserFactory<?>) dataParserFactory;
                    IRecordDataParser<?> dataParser = recordParserFactory.createRecordParser(ctx);
                    if (indexingOp) {
                        return new IndexingDataFlowController(ctx, dataParser, recordReader,
                                ((IIndexingDatasource) recordReader).getIndexer());
                    } else if (isFeed) {
                        boolean isChangeFeed = ExternalDataUtils.isChangeFeed(configuration);
                        boolean isRecordWithMeta = ExternalDataUtils.isRecordWithMeta(configuration);
                        if (isRecordWithMeta) {
                            if (isChangeFeed) {
                                int numOfKeys = ExternalDataUtils.getNumberOfKeys(configuration);
                                return new ChangeFeedWithMetaDataFlowController(ctx, feedLogManager, numOfKeys + 2,
                                        (IRecordWithMetadataParser) dataParser, recordReader);
                            } else {
                                return new FeedWithMetaDataFlowController(ctx, feedLogManager, 2,
                                        (IRecordWithMetadataParser) dataParser, recordReader);
                            }
                        } else if (isChangeFeed) {
                            int numOfKeys = ExternalDataUtils.getNumberOfKeys(configuration);
                            return new ChangeFeedDataFlowController(ctx, feedLogManager, numOfKeys + 1,
                                    (IRecordWithPKDataParser) dataParser, recordReader);
                        } else {
                            return new FeedRecordDataFlowController(ctx, feedLogManager, 1, dataParser, recordReader);
                        }
                    } else {
                        return new RecordDataFlowController(ctx, dataParser, recordReader, 1);
                    }
                case STREAM:
                    IInputStreamFactory streamFactory = (IInputStreamFactory) dataSourceFactory;
                    AsterixInputStream stream = streamFactory.createInputStream(ctx, partition);
                    IStreamDataParserFactory streamParserFactory = (IStreamDataParserFactory) dataParserFactory;
                    IStreamDataParser streamParser = streamParserFactory.createInputStreamParser(ctx, partition);
                    streamParser.setInputStream(stream);
                    if (isFeed) {
                        return new FeedStreamDataFlowController(ctx, feedLogManager, streamParser, stream);
                    } else {
                        return new StreamDataFlowController(ctx, streamParser);
                    }
                default:
                    throw new RuntimeDataException(ErrorCode.PROVIDER_DATAFLOW_CONTROLLER_UNKNOWN_DATA_SOURCE,
                            dataSourceFactory.getDataSourceType());
            }
        } catch (IOException | AsterixException e) {
            throw HyracksDataException.create(e);
        }
    }
}
