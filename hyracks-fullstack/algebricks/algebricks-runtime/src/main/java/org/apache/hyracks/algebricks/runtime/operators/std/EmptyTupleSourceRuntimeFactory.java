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

import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputSourcePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractPushRuntimeFactory;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class EmptyTupleSourceRuntimeFactory extends AbstractPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    public EmptyTupleSourceRuntimeFactory() {
    }

    @Override
    public String toString() {
        return "ets";
    }

    @Override
    public IPushRuntime[] createPushRuntime(final IHyracksTaskContext ctx) throws HyracksDataException {
        return new IPushRuntime[] { new AbstractOneInputSourcePushRuntime() {

            private final ArrayTupleBuilder tb = new ArrayTupleBuilder(0);
            private final FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));

            @Override
            public void open() throws HyracksDataException {
                super.open();
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new IllegalStateException();
                }
                appender.write(writer, true);
            }

            @Override
            public void close() throws HyracksDataException {
                if (isOpen) {
                    writer.close();
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                appender.flush(writer);
            }
        } };
    }
}
