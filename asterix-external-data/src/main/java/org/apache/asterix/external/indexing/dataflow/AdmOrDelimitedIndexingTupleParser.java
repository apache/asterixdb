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
package org.apache.asterix.external.indexing.dataflow;

import java.io.InputStream;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.indexing.input.AbstractHDFSReader;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.operators.file.IDataParser;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.file.ITupleParser;

public class AdmOrDelimitedIndexingTupleParser implements ITupleParser {

    private ArrayTupleBuilder tb;
    private final FrameTupleAppender appender;
    private final ARecordType recType;
    private final IDataParser parser;
    private final AMutableInt32 aMutableInt = new AMutableInt32(0);
    private final AMutableInt64 aMutableLong = new AMutableInt64(0);

    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer intSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer longSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);

    public AdmOrDelimitedIndexingTupleParser(IHyracksTaskContext ctx, ARecordType recType, IDataParser parser)
            throws HyracksDataException {
        this.parser = parser;
        this.recType = recType;
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        tb = new ArrayTupleBuilder(3);
    }

    @Override
    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
        // Cast the input stream to a record reader
        AbstractHDFSReader inReader = (AbstractHDFSReader) in;
        try {
            parser.initialize(in, recType, true);
            while (true) {
                tb.reset();
                if (!parser.parse(tb.getDataOutput())) {
                    break;
                }
                tb.addFieldEndOffset();
                appendIndexingData(tb, inReader);
                addTupleToFrame(writer);
            }
            appender.flush(writer, true);
        } catch (AsterixException ae) {
            throw new HyracksDataException(ae);
        } catch (Exception ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    // This function is used to append RID to Hyracks tuple
    @SuppressWarnings("unchecked")
    private void appendIndexingData(ArrayTupleBuilder tb, AbstractHDFSReader inReader) throws Exception {
        aMutableInt.setValue(inReader.getFileNumber());
        aMutableLong.setValue(inReader.getReaderPosition());
        tb.addField(intSerde, aMutableInt);
        tb.addField(longSerde, aMutableLong);
    }

    private void addTupleToFrame(IFrameWriter writer) throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            appender.flush(writer, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException("Record is too big to fit in a frame");
            }
        }
    }

}
