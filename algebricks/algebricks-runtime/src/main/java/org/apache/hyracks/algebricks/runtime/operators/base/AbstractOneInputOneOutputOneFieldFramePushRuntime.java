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
package org.apache.hyracks.algebricks.runtime.operators.base;

import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public abstract class AbstractOneInputOneOutputOneFieldFramePushRuntime
        extends AbstractOneInputOneOutputOneFramePushRuntime {

    @Override
    protected IFrameTupleAppender getTupleAppender() {
        return (FrameFixedFieldTupleAppender) appender;
    }

    protected IFrameFieldAppender getFieldAppender() {
        return (FrameFixedFieldTupleAppender) appender;
    }

    protected final void initAccessAppendFieldRef(IHyracksTaskContext ctx) throws HyracksDataException {
        frame = new VSizeFrame(ctx);
        appender = new FrameFixedFieldTupleAppender(inputRecordDesc.getFieldCount());
        appender.reset(frame, true);
        tAccess = new FrameTupleAccessor(inputRecordDesc);
        tRef = new FrameTupleReference();
    }

    protected void appendField(byte[] array, int start, int length) throws HyracksDataException {
        FrameUtils.appendFieldToWriter(writer, getFieldAppender(), array, start, length);
    }

    protected void appendField(IFrameTupleAccessor accessor, int tid, int fid) throws HyracksDataException {
        FrameUtils.appendFieldToWriter(writer, getFieldAppender(), accessor, tid, fid);
    }
}
