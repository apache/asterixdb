/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.indexing.dataflow;

import java.io.InputStream;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.indexing.input.AbstractHDFSReader;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.operators.file.IDataParser;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;

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
