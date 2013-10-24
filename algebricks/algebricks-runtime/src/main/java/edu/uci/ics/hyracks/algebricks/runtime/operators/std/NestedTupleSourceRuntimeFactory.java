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
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class NestedTupleSourceRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    public NestedTupleSourceRuntimeFactory() {
    }

    @Override
    public String toString() {
        return "nts";
    }

    @Override
    public IPushRuntime createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        return new NestedTupleSourceRuntime(ctx);
    }

    public static class NestedTupleSourceRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {

        public NestedTupleSourceRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
            initAccessAppend(ctx);
        }

        @Override
        public void open() throws HyracksDataException {
            writer.open();
        }

        public void writeTuple(ByteBuffer inputBuffer, int tIndex) throws HyracksDataException {
            tAccess.reset(inputBuffer);
            appendTupleToFrame(tIndex);
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            throw new IllegalStateException();
        }

        @Override
        public void fail() throws HyracksDataException {
            writer.fail();
        }

        public void forceFlush() throws HyracksDataException {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
        }
    }
}