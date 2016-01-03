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

import org.apache.asterix.external.api.ILookupReaderFactory;
import org.apache.asterix.external.api.ILookupRecordReader;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.dataset.adapter.LookupAdapter;
import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.asterix.external.indexing.RecordIdReader;
import org.apache.asterix.external.indexing.RecordIdReaderFactory;
import org.apache.asterix.external.input.record.reader.LookupReaderFactoryProvider;
import org.apache.asterix.external.provider.ParserFactoryProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class LookupAdapterFactory<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private IRecordDataParserFactory dataParserFactory;
    private ILookupReaderFactory readerFactory;
    private ARecordType recordType;
    private int[] ridFields;
    private Map<String, String> configuration;
    private boolean retainInput;
    private boolean retainNull;
    private int[] propagatedFields;
    private INullWriterFactory iNullWriterFactory;

    public LookupAdapterFactory(ARecordType recordType, int[] ridFields, boolean retainInput, boolean retainNull,
            INullWriterFactory iNullWriterFactory) {
        this.recordType = recordType;
        this.ridFields = ridFields;
        this.retainInput = retainInput;
        this.retainNull = retainNull;
        this.iNullWriterFactory = iNullWriterFactory;
    }

    public LookupAdapter<T> createAdapter(IHyracksTaskContext ctx, int partition, RecordDescriptor inRecDesc,
            ExternalFileIndexAccessor snapshotAccessor, IFrameWriter writer) throws HyracksDataException {
        try {
            IRecordDataParser<T> dataParser = dataParserFactory.createRecordParser(ctx);
            dataParser.configure(configuration, recordType);
            ILookupRecordReader<? extends T> reader = readerFactory.createRecordReader(ctx, partition,
                    snapshotAccessor);
            reader.configure(configuration);
            RecordIdReader ridReader = RecordIdReaderFactory.create(configuration, ridFields);
            configurePropagatedFields(inRecDesc);
            return new LookupAdapter<T>(dataParser, reader, inRecDesc, ridReader, retainInput, propagatedFields,
                    retainNull, iNullWriterFactory, ctx, writer);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
        readerFactory = LookupReaderFactoryProvider.getLookupReaderFactory(configuration);
        dataParserFactory = (IRecordDataParserFactory<T>) ParserFactoryProvider.getDataParserFactory(configuration);
        dataParserFactory.setRecordType(recordType);
        readerFactory.configure(configuration);
        dataParserFactory.configure(configuration);
    }

    private void configurePropagatedFields(RecordDescriptor inRecDesc) {
        int ptr = 0;
        boolean skip = false;
        propagatedFields = new int[inRecDesc.getFieldCount() - ridFields.length];
        for (int i = 0; i < inRecDesc.getFieldCount(); i++) {
            if (ptr < ridFields.length) {
                skip = false;
                for (int j = 0; j < ridFields.length; j++) {
                    if (ridFields[j] == i) {
                        ptr++;
                        skip = true;
                        break;
                    }
                }
                if (!skip)
                    propagatedFields[i - ptr] = i;
            } else {
                propagatedFields[i - ptr] = i;
            }
        }
    }
}
