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

package org.apache.hyracks.dataflow.std.misc;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class ConstantTupleSourceOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private IHyracksTaskContext ctx;

    private int[] fieldSlots;
    private byte[] tupleData;
    private int tupleSize;

    public ConstantTupleSourceOperatorNodePushable(IHyracksTaskContext ctx, int[] fieldSlots, byte[] tupleData,
            int tupleSize) {
        super();
        this.fieldSlots = fieldSlots;
        this.tupleData = tupleData;
        this.tupleSize = tupleSize;
        this.ctx = ctx;
    }

    @Override
    public void initialize() throws HyracksDataException {
        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
        if (fieldSlots != null && tupleData != null && tupleSize > 0)
            appender.append(fieldSlots, tupleData, 0, tupleSize);
        writer.open();
        try {
            appender.write(writer, false);
        } catch (Throwable th) {
            writer.fail();
            throw HyracksDataException.create(th);
        } finally {
            writer.close();
        }
    }
}
