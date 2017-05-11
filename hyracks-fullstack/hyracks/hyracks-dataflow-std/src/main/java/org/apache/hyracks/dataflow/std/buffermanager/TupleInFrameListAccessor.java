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

package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * tuple accessor class for a frame in a frame list using a tuple pointer.
 */
public class TupleInFrameListAccessor extends AbstractTuplePointerAccessor {

    private IFrameTupleAccessor bufferAccessor;
    private List<ByteBuffer> bufferFrames;

    public TupleInFrameListAccessor(RecordDescriptor rd, List<ByteBuffer> bufferFrames) {
        bufferAccessor = new FrameTupleAccessor(rd);
        this.bufferFrames = bufferFrames;
    }

    @Override
    IFrameTupleAccessor getInnerAccessor() {
        return bufferAccessor;
    }

    @Override
    void resetInnerAccessor(TuplePointer tuplePointer) {
        bufferAccessor.reset(bufferFrames.get(tuplePointer.getFrameIndex()));
    }
}
