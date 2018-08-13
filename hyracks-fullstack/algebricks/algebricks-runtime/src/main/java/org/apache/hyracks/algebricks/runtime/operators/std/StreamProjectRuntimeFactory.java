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
package org.apache.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class StreamProjectRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    private final boolean flushFramesRapidly;

    public StreamProjectRuntimeFactory(int[] projectionList, boolean flushFramesRapidly) {
        super(projectionList);
        this.flushFramesRapidly = flushFramesRapidly;
    }

    public StreamProjectRuntimeFactory(int[] projectionList) {
        this(projectionList, false);
    }

    @Override
    public String toString() {
        return "stream-project " + Arrays.toString(projectionList);
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private boolean first = true;

            @Override
            public void open() throws HyracksDataException {
                super.open();
                if (first) {
                    first = false;
                    initAccessAppend(ctx);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // what if numOfTuples is 0?
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                if (nTuple == 0) {
                    appender.flush(writer);
                } else {
                    int t = 0;
                    if (nTuple > 1) {
                        for (; t < nTuple - 1; t++) {
                            appendProjectionToFrame(t, projectionList);
                        }
                    }
                    if (flushFramesRapidly) {
                        // Whenever all the tuples in the incoming frame have been consumed, the project operator
                        // will push its frame to the next operator; i.e., it won't wait until the frame gets full.
                        appendProjectionToFrame(t, projectionList, true);
                    } else {
                        appendProjectionToFrame(t, projectionList);
                    }
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                appender.flush(writer);
            }

        };
    }
}
