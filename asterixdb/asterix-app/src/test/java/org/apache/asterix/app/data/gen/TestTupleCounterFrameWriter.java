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
package org.apache.asterix.app.data.gen;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.test.CountAnswer;
import org.apache.hyracks.api.test.TestFrameWriter;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class TestTupleCounterFrameWriter extends TestFrameWriter {

    private final FrameTupleAccessor accessor;
    private int count = 0;

    public TestTupleCounterFrameWriter(RecordDescriptor recordDescriptor, CountAnswer openAnswer,
            CountAnswer nextAnswer, CountAnswer flushAnswer, CountAnswer failAnswer, CountAnswer closeAnswer,
            boolean deepCopyInputFrames) {
        super(openAnswer, nextAnswer, flushAnswer, failAnswer, closeAnswer, deepCopyInputFrames);
        accessor = new FrameTupleAccessor(recordDescriptor);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        super.nextFrame(buffer);
        accessor.reset(buffer);
        count += accessor.getTupleCount();
    }

    public int getCount() {
        return count;
    }
}
