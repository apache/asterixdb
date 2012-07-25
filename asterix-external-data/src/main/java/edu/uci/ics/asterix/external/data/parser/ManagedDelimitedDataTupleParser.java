/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.data.parser;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter;
import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter.OperationState;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataTupleParser;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class ManagedDelimitedDataTupleParser extends DelimitedDataTupleParser implements IManagedTupleParser {

    private List<OperationState> nextState;
    private IManagedFeedAdapter adapter;
    private long tupleInterval;

    public static final String TUPLE_INTERVAL_KEY = "tuple-interval";

    public ManagedDelimitedDataTupleParser(IHyracksTaskContext ctx, ARecordType recType, IManagedFeedAdapter adapter,
            IValueParserFactory[] valueParserFactories, char fieldDelimter) {
        super(ctx, recType, valueParserFactories, fieldDelimter);
        this.adapter = adapter;
        nextState = new ArrayList<OperationState>();
        tupleInterval = adapter.getAdapterProperty(TUPLE_INTERVAL_KEY) == null ? 0 : Long.parseLong(adapter
                .getAdapterProperty(TUPLE_INTERVAL_KEY));
    }

    @Override
    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
        try {
            IValueParser[] valueParsers = new IValueParser[valueParserFactories.length];
            for (int i = 0; i < valueParserFactories.length; ++i) {
                valueParsers[i] = valueParserFactories[i].createValueParser();
            }

            appender.reset(frame, true);
            tb = new ArrayTupleBuilder(1);
            recDos = tb.getDataOutput();

            ArrayBackedValueStorage fieldValueBuffer = new ArrayBackedValueStorage();
            DataOutput fieldValueBufferOutput = fieldValueBuffer.getDataOutput();
            IARecordBuilder recBuilder = new RecordBuilder();
            recBuilder.reset(recType);
            recBuilder.init();

            int n = recType.getFieldNames().length;
            byte[] fieldTypeTags = new byte[n];
            for (int i = 0; i < n; i++) {
                ATypeTag tag = recType.getFieldTypes()[i].getTypeTag();
                fieldTypeTags[i] = tag.serialize();
            }

            int[] fldIds = new int[n];
            ArrayBackedValueStorage[] nameBuffers = new ArrayBackedValueStorage[n];
            AMutableString str = new AMutableString(null);
            for (int i = 0; i < n; i++) {
                String name = recType.getFieldNames()[i];
                fldIds[i] = recBuilder.getFieldId(name);
                if (fldIds[i] < 0) {
                    if (!recType.isOpen()) {
                        throw new HyracksDataException("Illegal field " + name + " in closed type " + recType);
                    } else {
                        nameBuffers[i] = new ArrayBackedValueStorage();
                        fieldNameToBytes(name, str, nameBuffers[i]);
                    }
                }
            }

            FieldCursor cursor = new FieldCursor(new InputStreamReader(in));
            while (cursor.nextRecord()) {
                tb.reset();
                recBuilder.reset(recType);
                recBuilder.init();

                for (int i = 0; i < valueParsers.length; ++i) {
                    if (!cursor.nextField()) {
                        break;
                    }
                    fieldValueBuffer.reset();
                    fieldValueBufferOutput.writeByte(fieldTypeTags[i]);
                    valueParsers[i].parse(cursor.getBuffer(), cursor.getfStart(),
                            cursor.getfEnd() - cursor.getfStart(), fieldValueBufferOutput);
                    if (fldIds[i] < 0) {
                        recBuilder.addField(nameBuffers[i], fieldValueBuffer);
                    } else {
                        recBuilder.addField(fldIds[i], fieldValueBuffer);
                    }
                }
                recBuilder.write(recDos, true);
                processNextTuple(nextState.isEmpty() ? null : nextState.get(0), writer);
                Thread.currentThread().sleep(tupleInterval);
            }
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(frame, writer);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        } catch (InterruptedException ie) {
            throw new HyracksDataException(ie);
        }
    }

    private void addTupleToFrame(IFrameWriter writer, boolean forceFlush) throws HyracksDataException {
        tb.addFieldEndOffset();
        boolean success = appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        if (!success) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
        }

        if (forceFlush) {
            FrameUtils.flushFrame(frame, writer);
        }

    }

    private void processNextTuple(OperationState feedState, IFrameWriter writer) throws HyracksDataException {
        try {
            if (feedState != null) {
                switch (feedState) {
                    case SUSPENDED:
                        suspendOperation(writer);
                        break;
                    case STOPPED:
                        stopOperation(writer);
                        break;
                }
            } else {
                addTupleToFrame(writer, false);
            }
        } catch (HyracksDataException hde) {
            throw hde;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void suspendOperation(IFrameWriter writer) throws HyracksDataException, Exception {
        nextState.remove(0);
        addTupleToFrame(writer, false);
        adapter.beforeSuspend();
        synchronized (this) {
            this.wait();
            adapter.beforeResume();
        }
    }

    private void stopOperation(IFrameWriter writer) throws HyracksDataException, Exception {
        nextState.remove(0);
        addTupleToFrame(writer, false);
        adapter.beforeStop();
        adapter.stop();
    }

    @Override
    public void suspend() throws Exception {
        nextState.add(OperationState.SUSPENDED);
    }

    @Override
    public void resume() throws Exception {
        synchronized (this) {
            this.notifyAll();
        }
    }

    @Override
    public void stop() throws Exception {
        nextState.add(OperationState.STOPPED);
    }

    @Override
    public void alter(Map<String, String> alterParams) throws Exception {
        if (alterParams.get(TUPLE_INTERVAL_KEY) != null) {
            tupleInterval = Long.parseLong(alterParams.get(TUPLE_INTERVAL_KEY));
        }
    }
}
