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

package org.apache.hyracks.tests.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class OutputFrameVerifier implements IFrameWriter {

    private final RecordDescriptor inputRecordDescriptor;
    private final List<Object[]> answerList;
    private final FrameTupleAccessor frameAccessor;
    private int offset;
    private boolean failed;

    public OutputFrameVerifier(RecordDescriptor inputRecordDescriptor, List<Object[]> answerList) {
        this.inputRecordDescriptor = inputRecordDescriptor;
        this.frameAccessor = new FrameTupleAccessor(inputRecordDescriptor);
        this.answerList = answerList;
    }

    @Override
    public void open() throws HyracksDataException {
        this.offset = 0;
        this.failed = false;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        frameAccessor.reset(buffer);
        for (int tid = 0; tid < frameAccessor.getTupleCount(); tid++) {
            Object[] objects = new Object[inputRecordDescriptor.getFieldCount()];
            for (int fid = 0; fid < inputRecordDescriptor.getFieldCount(); fid++) {
                ByteArrayInputStream bais = new ByteArrayInputStream(frameAccessor.getBuffer().array(),
                        frameAccessor.getAbsoluteFieldStartOffset(tid, fid), frameAccessor.getFieldLength(tid, fid));
                DataInputStream dis = new DataInputStream(bais);
                objects[fid] = inputRecordDescriptor.getFields()[fid].deserialize(dis);
            }
            if (offset >= answerList.size()) {
                throw new HyracksDataException(
                        "The number of given results is more than expected size:" + answerList.size());
            }
            Object[] expected = answerList.get(offset);
            for (int i = 0; i < expected.length; i++) {
                if (!expected[i].equals(objects[i])) {
                    throw new HyracksDataException(
                            "The result object: " + objects[i] + " is different from the expected one:" + expected[i]);
                }
            }
            offset++;
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        this.failed = true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (offset < answerList.size()) {
            throw new HyracksDataException(
                    "The number of given results:" + offset + " is less than expected size:" + answerList.size());
        }
    }

    public boolean isFailed() {
        return failed;
    }
}
