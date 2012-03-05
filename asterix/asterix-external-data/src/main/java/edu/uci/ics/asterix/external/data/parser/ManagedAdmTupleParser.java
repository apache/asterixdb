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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.adm.parser.nontagged.AdmLexer;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter;
import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter.OperationState;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AdmTupleParser;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class ManagedAdmTupleParser extends AdmTupleParser implements IManagedTupleParser {

    private OperationState state;
    private List<OperationState> nextState;
    private final IManagedFeedAdapter adapter;
    private long tupleInterval;

    public static final String TUPLE_INTERVAL_KEY = "tuple-interval";

    public ManagedAdmTupleParser(IHyracksTaskContext ctx, ARecordType recType, IManagedFeedAdapter adapter) {
        super(ctx, recType);
        nextState = new ArrayList<OperationState>();
        this.adapter = adapter;
        this.tupleInterval = adapter.getAdapterProperty(TUPLE_INTERVAL_KEY) == null ? 0 : Long.parseLong(adapter
                .getAdapterProperty(TUPLE_INTERVAL_KEY));
    }

    public ManagedAdmTupleParser(IHyracksTaskContext ctx, ARecordType recType, long tupleInterval,
            IManagedFeedAdapter adapter) {
        super(ctx, recType);
        nextState = new ArrayList<OperationState>();
        this.adapter = adapter;
        this.tupleInterval = tupleInterval;
    }

    @Override
    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
        admLexer = new AdmLexer(in);
        appender.reset(frame, true);
        int tupleNum = 0;
        try {
            while (true) {
                tb.reset();
                if (!parseAdmInstance(recType, true, dos)) {
                    break;
                }
                tb.addFieldEndOffset();
                processNextTuple(nextState.isEmpty() ? null : nextState.get(0), writer);
                Thread.currentThread().sleep(tupleInterval);
                tupleNum++;
            }
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(frame, writer);
            }
        } catch (AsterixException ae) {
            throw new HyracksDataException(ae);
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        } catch (InterruptedException ie) {
            throw new HyracksDataException(ie);
        }
    }

    private void addTupleToFrame(IFrameWriter writer, boolean forceFlush) throws HyracksDataException {
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
                switch (state) {
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
        addTupleToFrame(writer, true);
        adapter.beforeStop();
        writer.close();
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
