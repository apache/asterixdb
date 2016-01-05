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

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IDataFlowController;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.api.IInputStreamProviderFactory;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.api.IStreamDataParserFactory;
import org.apache.asterix.external.dataflow.IndexingDataFlowController;
import org.apache.asterix.external.dataflow.RecordDataFlowController;
import org.apache.asterix.external.dataflow.StreamDataFlowController;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class DataflowControllerProvider {

    /**
     * Order of calls:
     * 1. Constructor()
     * 2. configure(configuration,ctx)
     * 3. setTupleForwarder(forwarder)
     * 4. if record flow controller
     * |-a. Set record reader
     * |-b. Set record parser
     * else
     * |-a. Set stream parser
     * 5. start(writer)
     */

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static IDataFlowController getDataflowController(ARecordType recordType, IHyracksTaskContext ctx,
            int partition, IExternalDataSourceFactory dataSourceFactory, IDataParserFactory dataParserFactory,
            Map<String, String> configuration, boolean indexingOp) throws Exception {
        switch (dataSourceFactory.getDataSourceType()) {
            case RECORDS:
                RecordDataFlowController recordDataFlowController;
                if (indexingOp) {
                    recordDataFlowController = new IndexingDataFlowController();
                } else {
                    recordDataFlowController = new RecordDataFlowController();
                }
                recordDataFlowController.configure(configuration, ctx);
                recordDataFlowController.setTupleForwarder(DataflowUtils.getTupleForwarder(configuration));
                IRecordReaderFactory<?> recordReaderFactory = (IRecordReaderFactory<?>) dataSourceFactory;
                IRecordReader<?> recordReader = recordReaderFactory.createRecordReader(ctx, partition);
                IRecordDataParserFactory<?> recordParserFactory = (IRecordDataParserFactory<?>) dataParserFactory;
                IRecordDataParser<?> dataParser = recordParserFactory.createRecordParser(ctx);
                dataParser.configure(configuration, recordType);
                recordDataFlowController.setRecordReader(recordReader);
                recordDataFlowController.setRecordParser(dataParser);
                return recordDataFlowController;
            case STREAM:
                StreamDataFlowController streamDataFlowController = new StreamDataFlowController();
                streamDataFlowController.configure(configuration, ctx);
                streamDataFlowController.setTupleForwarder(DataflowUtils.getTupleForwarder(configuration));
                IInputStreamProviderFactory streamProviderFactory = (IInputStreamProviderFactory) dataSourceFactory;
                IInputStreamProvider streamProvider = streamProviderFactory.createInputStreamProvider(ctx, partition);
                IStreamDataParserFactory streamParserFactory = (IStreamDataParserFactory) dataParserFactory;
                streamParserFactory.configure(configuration);
                IStreamDataParser streamParser = streamParserFactory.createInputStreamParser(ctx, partition);
                streamParser.configure(configuration, recordType);
                streamParser.setInputStream(streamProvider.getInputStream());
                streamDataFlowController.setStreamParser(streamParser);
                return streamDataFlowController;
            default:
                throw new AsterixException("Unknown data source type: " + dataSourceFactory.getDataSourceType());
        }
    }
}
