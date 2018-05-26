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

package org.apache.hyracks.algebricks.runtime.operators.union;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractPushRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MicroUnionAllRuntimeFactory extends AbstractPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int inputArity;

    public MicroUnionAllRuntimeFactory(int inputArity) {
        this.inputArity = inputArity;
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) {
        Mutable<Boolean> failedShared = new MutableObject<>(Boolean.FALSE);
        IPushRuntime[] result = new IPushRuntime[inputArity];
        for (int i = 0; i < inputArity; i++) {
            result[i] = new MicroUnionAllPushRuntime(i, failedShared);
        }
        return result;
    }

    @Override
    public String toString() {
        return "union-all";
    }

    private final class MicroUnionAllPushRuntime implements IPushRuntime {

        private final int idx;

        private final Mutable<Boolean> failedShared;

        private IFrameWriter writer;

        MicroUnionAllPushRuntime(int idx, Mutable<Boolean> failedShared) {
            this.idx = idx;
            this.failedShared = failedShared;
        }

        @Override
        public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
            if (index != 0) {
                throw new IllegalArgumentException(String.valueOf(index));
            }
            this.writer = writer;
        }

        @Override
        public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
            // input is not accessed
        }

        @Override
        public void open() throws HyracksDataException {
            if (idx == 0) {
                writer.open();
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            writer.nextFrame(buffer);
        }

        @Override
        public void fail() throws HyracksDataException {
            boolean failed = failedShared.getValue();
            failedShared.setValue(Boolean.TRUE);
            if (!failed) {
                writer.fail();
            }
        }

        @Override
        public void close() throws HyracksDataException {
            if (idx == 0) {
                writer.close();
            }
        }
    }
}
