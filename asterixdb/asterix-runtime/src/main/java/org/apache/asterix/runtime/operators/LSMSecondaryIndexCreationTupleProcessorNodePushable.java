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
package org.apache.asterix.runtime.operators;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

/**
 * This operator node is used for post-processing tuples scanned from the primary index
 * when creating a new secondary index.
 * The incoming tuples could be either a matter-tuple or an anti-matter tuple.
 * For an anti-matter tuple, it outputs a corresponding anti-matter tuple
 * (if the secondary index has a buddy btree, then the result tuple only has primary key;
 * otherwise, the result tuple has both secondary keys and primary key)
 * For each matter-tuple (here old secondary keys refer to the secondary keys of the previous
 * matter tuple with the same primary key)
 * -If old secondary keys == new secondary keys
 * --If with buddy btree
 * ---generate a deleted-tuple
 * --generate a new key
 * -else
 * --If old secondary keys are null?
 * ---do nothing
 * --else
 * ---delete old secondary keys
 * --If new keys are null?
 * ---do nothing
 * --else
 * ---insert new keys
 *
 * Incoming tuple format:
 * [component pos, anti-matter flag, secondary keys, primary keys, filter values]
 */
public class LSMSecondaryIndexCreationTupleProcessorNodePushable extends AbstractLSMSecondaryIndexCreationNodePushable {
    private final FrameTupleReference tuple = new FrameTupleReference();
    // prevSourceTuple stores the previous matter tuple
    private final ArrayTupleBuilder prevMatterTupleBuilder;
    private final ArrayTupleReference prevMatterTuple = new ArrayTupleReference();

    private boolean hasPrevMatterTuple;

    private FrameTupleAppender appender;
    private ArrayTupleBuilder tb;
    private DataOutput dos;
    private final IMissingWriter missingWriter;
    private DeletedTupleCounter deletedTupleCounter;

    public static class DeletedTupleCounter extends AbstractStateObject {
        private final Map<Integer, Integer> map = new HashMap<>();

        public DeletedTupleCounter(JobId jobId, int partition) {
            super(jobId, partition);
        }

        public int get(int componentPos) {
            Integer value = map.get(componentPos);
            return value != null ? value : 0;
        }

        public void inc(int componentPos) {
            map.put(componentPos, get(componentPos) + 1);
        }
    }

    public LSMSecondaryIndexCreationTupleProcessorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, IMissingWriterFactory missingWriterFactory, int numTagFields,
            int numSecondaryKeys, int numPrimaryKeys, boolean hasBuddyBTree) throws HyracksDataException {

        super(ctx, partition, inputRecDesc, numTagFields, numSecondaryKeys, numPrimaryKeys, hasBuddyBTree);

        this.prevMatterTupleBuilder = new ArrayTupleBuilder(inputRecDesc.getFieldCount());

        if (this.hasBuddyBTree) {
            missingWriter = missingWriterFactory.createMissingWriter();
        } else {
            missingWriter = null;
        }

    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        deletedTupleCounter = new DeletedTupleCounter(ctx.getJobletContext().getJobId(), partition);
        ctx.setStateObject(deletedTupleCounter);
        try {
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        HyracksDataException closeException = null;
        try {
            if (appender.getTupleCount() > 0) {
                appender.write(writer, true);
            }
        } catch (HyracksDataException e) {
            closeException = e;
        }
        try {
            // will definitely be called regardless of exceptions
            writer.close();
        } catch (HyracksDataException th) {
            if (closeException == null) {
                closeException = th;
            } else {
                closeException.addSuppressed(th);
            }
        }
        if (closeException != null) {
            throw closeException;
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            try {
                // if both previous value and new value are null, then we skip
                tuple.reset(accessor, i);
                if (isAntiMatterTuple(tuple)) {
                    processAntiMatterTuple(tuple);
                    hasPrevMatterTuple = false;
                } else {
                    processMatterTuple(tuple);
                    // save the matter tuple
                    TupleUtils.copyTuple(prevMatterTupleBuilder, tuple, recordDesc.getFieldCount());
                    prevMatterTuple.reset(prevMatterTupleBuilder.getFieldEndOffsets(),
                            prevMatterTupleBuilder.getByteArray());
                    hasPrevMatterTuple = true;
                }
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    private void processMatterTuple(ITupleReference tuple) throws HyracksDataException {

        boolean isNewValueMissing = isSecondaryKeyMissing(tuple);
        boolean isOldValueMissing = !hasPrevMatterTuple || !equalPrimaryKeys(tuple, prevMatterTuple)
                || isSecondaryKeyMissing(prevMatterTuple);

        if (isNewValueMissing && isOldValueMissing) {
            // if both values are missing, then do nothing
            return;
        }
        // At least one is not null
        if (!isOldValueMissing && equalSecondaryKeys(prevMatterTuple, tuple)) {
            if (hasBuddyBTree) {
                // if the index has buddy btree, then we have to delete the index entry
                // from the older disk components
                writeAntiMatterTuple(prevMatterTuple, getComponentPos(tuple));
            }
            // we need to write the new tuple anyway
            writeMatterTuple(tuple);
            return;
        }
        if (!isOldValueMissing) {
            // we need to delete the previous entry
            writeAntiMatterTuple(prevMatterTuple, getComponentPos(tuple));
        }
        if (!isNewValueMissing) {
            // we need to insert the new entry
            writeMatterTuple(tuple);
        }

    }

    private void processAntiMatterTuple(ITupleReference tuple) throws HyracksDataException {
        boolean isNewValueMissing = isSecondaryKeyMissing(tuple);
        // if the secondary value is missing (which means the secondary value of the previous matter tuple
        // is also missing), we then simply ignore this tuple since there is nothing to delete
        if (!isNewValueMissing) {
            writeAntiMatterTuple(tuple, getComponentPos(tuple));
        }
    }

    private void writeAntiMatterTuple(ITupleReference tuple, int componentPos) throws HyracksDataException {
        deletedTupleCounter.inc(componentPos);
        tb.reset();
        // write tag fields
        tb.addField(IntegerSerializerDeserializer.INSTANCE, componentPos);
        tb.addField(BooleanSerializerDeserializer.INSTANCE, true);
        if (hasBuddyBTree) {
            // the output tuple does not have secondary keys (only primary keys + filter values)
            // write secondary keys (missing)
            for (int i = 0; i < numSecondaryKeys; i++) {
                missingWriter.writeMissing(dos);
                tb.addFieldEndOffset();
            }
        } else {
            for (int i = numTagFields; i < numTagFields + numSecondaryKeys; i++) {
                tb.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            }
        }

        // write all remaining fields
        for (int i = numTagFields + numSecondaryKeys; i < recordDesc.getFieldCount(); i++) {
            tb.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
        }

        FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
    }

    private void writeMatterTuple(ITupleReference tuple) throws HyracksDataException {
        // simply output the original tuple to the writer
        TupleUtils.copyTuple(tb, tuple, recordDesc.getFieldCount());
        FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
    }

    private boolean isSecondaryKeyMissing(ITupleReference tuple) {
        for (int i = numTagFields; i < numTagFields + numSecondaryKeys; i++) {
            if (TypeTagUtil.isType(tuple, i, ATypeTag.SERIALIZED_MISSING_TYPE_TAG)
                    || TypeTagUtil.isType(tuple, i, ATypeTag.SERIALIZED_NULL_TYPE_TAG)) {
                return true;
            }
        }
        return false;
    }

    private boolean equalPrimaryKeys(ITupleReference tuple1, ITupleReference tuple2) {
        for (int i = numTagFields + numSecondaryKeys; i < numTagFields + numPrimaryKeys + numSecondaryKeys; i++) {
            if (!TupleUtils.equalFields(tuple1, tuple2, i)) {
                return false;
            }
        }
        return true;
    }

    private boolean equalSecondaryKeys(ITupleReference tuple1, ITupleReference tuple2) {
        for (int i = numTagFields; i < numTagFields + numSecondaryKeys; i++) {
            if (!TupleUtils.equalFields(tuple1, tuple2, i)) {
                return false;
            }
        }
        return true;
    }
}
