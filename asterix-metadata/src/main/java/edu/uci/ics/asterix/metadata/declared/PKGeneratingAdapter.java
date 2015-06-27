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
package edu.uci.ics.asterix.metadata.declared;

import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.om.base.AMutableUUID;
import edu.uci.ics.asterix.om.base.AUUID;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class PKGeneratingAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = 1L;
    private final RecordDescriptor inRecDesc;
    private final RecordDescriptor outRecDesc;
    private final IDatasourceAdapter wrappedAdapter;
    private final PKGeneratingPushRuntime pkRuntime;
    private final int pkIndex;

    public PKGeneratingAdapter(IHyracksTaskContext ctx, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            ARecordType inRecType, ARecordType outRecType, IDatasourceAdapter wrappedAdapter, int pkIndex) {
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;
        this.wrappedAdapter = wrappedAdapter;
        this.pkRuntime = new PKGeneratingPushRuntime(ctx, inRecType, outRecType);
        this.pkIndex = pkIndex;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        pkRuntime.setInputRecordDescriptor(0, inRecDesc);
        pkRuntime.setFrameWriter(0, writer, outRecDesc);
        pkRuntime.open();
        try {
            wrappedAdapter.start(partition, pkRuntime);
        } catch (Throwable t) {
            pkRuntime.fail();
            throw t;
        } finally {
            pkRuntime.close();
        }
    }

    private class PKGeneratingPushRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {
        private final IHyracksTaskContext ctx;
        private final ARecordType outRecType;
        private final ArrayTupleBuilder tb;
        private final AMutableUUID aUUID = new AMutableUUID(0, 0);
        private final byte AUUIDTag = ATypeTag.UUID.serialize();
        private final byte[] serializedUUID = new byte[16];
        private final PointableAllocator pa = new PointableAllocator();
        private final ARecordPointable recordPointable;
        private final IAType[] outClosedTypes;

        private final RecordBuilder recBuilder;

        public PKGeneratingPushRuntime(IHyracksTaskContext ctx, ARecordType inRecType, ARecordType outRecType) {
            this.ctx = ctx;
            this.outRecType = outRecType;
            this.tb = new ArrayTupleBuilder(2);
            this.recBuilder = new RecordBuilder();
            this.recordPointable = (ARecordPointable) pa.allocateRecordValue(inRecType);
            this.outClosedTypes = outRecType.getFieldTypes();
        }

        /*
         * We write this method in low level instead of using pre-existing libraries since this will be called for each record and to avoid 
         * size validation
         */
        private void serializeUUID(AUUID aUUID, byte[] serializedUUID) {
            long v = aUUID.getLeastSignificantBits();
            serializedUUID[0] = (byte) (v >>> 56);
            serializedUUID[1] = (byte) (v >>> 48);
            serializedUUID[2] = (byte) (v >>> 40);
            serializedUUID[3] = (byte) (v >>> 32);
            serializedUUID[4] = (byte) (v >>> 24);
            serializedUUID[5] = (byte) (v >>> 16);
            serializedUUID[6] = (byte) (v >>> 8);
            serializedUUID[7] = (byte) (v >>> 0);
            v = aUUID.getMostSignificantBits();
            serializedUUID[8] = (byte) (v >>> 56);
            serializedUUID[9] = (byte) (v >>> 48);
            serializedUUID[10] = (byte) (v >>> 40);
            serializedUUID[11] = (byte) (v >>> 32);
            serializedUUID[12] = (byte) (v >>> 24);
            serializedUUID[13] = (byte) (v >>> 16);
            serializedUUID[14] = (byte) (v >>> 8);
            serializedUUID[15] = (byte) (v >>> 0);
        }

        @Override
        public void open() throws HyracksDataException {
            initAccessAppendRef(ctx);
            recBuilder.reset(outRecType);
            recBuilder.init();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            try {
                tAccess.reset(buffer);
                for (int i = 0; i < tAccess.getTupleCount(); ++i) {
                    tb.reset();
                    tRef.reset(tAccess, i);

                    // We need to do the following:
                    // 1. generate a UUID
                    // 2. fill in the first field with the UUID
                    aUUID.nextUUID();
                    tb.getDataOutput().writeByte(AUUIDTag);
                    serializeUUID(aUUID, serializedUUID);
                    tb.getDataOutput().write(serializedUUID);
                    tb.addFieldEndOffset();
                    // 3. fill in the second field with the record after adding to it the UUID
                    recordPointable.set(tRef.getFieldData(0), tRef.getFieldStart(0), tRef.getFieldLength(0));
                    // Start by closed fields
                    int inIndex = 0;
                    for (int f = 0; f < outClosedTypes.length; f++) {
                        if (f == pkIndex) {
                            recBuilder.addField(f, serializedUUID);
                        } else {
                            recBuilder.addField(f, recordPointable.getFieldValues().get(inIndex));
                            inIndex++;
                        }
                    }

                    // Add open fields
                    if (outRecType.isOpen()) {
                        List<IVisitablePointable> fp = recordPointable.getFieldNames();
                        if (fp.size() >= outClosedTypes.length) {
                            int index = outClosedTypes.length - 1;
                            while (index < fp.size()) {
                                recBuilder.addField(fp.get(index), recordPointable.getFieldValues().get(index));
                                index++;
                            }
                        }
                    }
                    //write the record
                    recBuilder.write(tb.getDataOutput(), true);
                    tb.addFieldEndOffset();
                    appendToFrameFromTupleBuilder(tb);
                }
            } catch (Exception e) {
                throw new HyracksDataException("Error in the auto id generation and merge of the record", e);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            flushIfNotFailed();
        }
    }

}
