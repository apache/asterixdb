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

import java.io.Serializable;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.external.api.ILookupReaderFactory;
import org.apache.asterix.external.api.ILookupRecordReader;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.dataset.adapter.LookupAdapter;
import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.asterix.external.indexing.RecordIdReader;
import org.apache.asterix.external.indexing.RecordIdReaderFactory;
import org.apache.asterix.external.provider.LookupReaderFactoryProvider;
import org.apache.asterix.external.provider.ParserFactoryProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class LookupAdapterFactory<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private IRecordDataParserFactory dataParserFactory;
    private ILookupReaderFactory readerFactory;
    private final ARecordType recordType;
    private final int[] ridFields;
    private Map<String, String> configuration;
    private final boolean retainInput;
    private final boolean retainMissing;
    private final IMissingWriterFactory isMissingWriterFactory;

    public LookupAdapterFactory(ARecordType recordType, int[] ridFields, boolean retainInput, boolean retainNull,
            IMissingWriterFactory iNullWriterFactory) {
        this.recordType = recordType;
        this.ridFields = ridFields;
        this.retainInput = retainInput;
        this.retainMissing = retainNull;
        this.isMissingWriterFactory = iNullWriterFactory;
    }

    public LookupAdapter<T> createAdapter(IHyracksTaskContext ctx, int partition, RecordDescriptor inRecDesc,
            ExternalFileIndexAccessor snapshotAccessor, IFrameWriter writer) throws HyracksDataException {
        try {
            IRecordDataParser<T> dataParser = dataParserFactory.createRecordParser(ctx);
            ILookupRecordReader<? extends T> reader =
                    readerFactory.createRecordReader(ctx, partition, snapshotAccessor);
            reader.configure(configuration);
            RecordIdReader ridReader = RecordIdReaderFactory.create(configuration, ridFields);
            return new LookupAdapter<>(dataParser, reader, inRecDesc, ridReader, retainInput, retainMissing,
                    isMissingWriterFactory, ctx, writer);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    public void configure(IServiceContext serviceContext, Map<String, String> configuration)
            throws HyracksDataException, AlgebricksException {
        this.configuration = configuration;
        IApplicationContext appCtx = (IApplicationContext) serviceContext.getApplicationContext();
        readerFactory = LookupReaderFactoryProvider.getLookupReaderFactory(serviceContext, configuration);
        dataParserFactory = (IRecordDataParserFactory<T>) ParserFactoryProvider
                .getDataParserFactory(appCtx.getLibraryManager(), configuration);
        dataParserFactory.setRecordType(recordType);
        readerFactory.configure(serviceContext, configuration);
        dataParserFactory.configure(configuration);
    }

}
