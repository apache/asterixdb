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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

public class MaterializerTaskState extends AbstractStateObject {
    private RunFileWriter out;
    private final AtomicInteger numConsumers = new AtomicInteger(1);

    public MaterializerTaskState(JobId jobId, TaskId taskId) {
        super(jobId, taskId);
    }

    public MaterializerTaskState(JobId jobId, TaskId taskId, int numConsumers) {
        super(jobId, taskId);
        this.numConsumers.set(numConsumers);
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {

    }

    @Override
    public void fromBytes(DataInput in) throws IOException {

    }

    public void open(IHyracksTaskContext ctx) throws HyracksDataException {
        FileReference file =
                ctx.getJobletContext().createManagedWorkspaceFile(MaterializerTaskState.class.getSimpleName());
        out = new RunFileWriter(file, ctx.getIoManager());
        out.open();
    }

    public void close() throws HyracksDataException {
        if (out != null) {
            out.close();
        }
    }

    public void appendFrame(ByteBuffer buffer) throws HyracksDataException {
        out.nextFrame(buffer);
    }

    public void writeOut(IFrameWriter writer, IFrame frame, boolean failed) throws HyracksDataException {
        RunFileReader in = null;
        if (out != null) {
            in = out.createReader();
        }
        writer.open();
        try {
            if (failed) {
                writer.fail();
                return;
            }
            if (in != null) {
                in.open();
                try {
                    while (in.nextFrame(frame)) {
                        writer.nextFrame(frame.getBuffer());
                    }
                } finally {
                    in.close();
                }
            }
        } catch (Exception e) {
            writer.fail();
            throw e;
        } finally {
            try {
                writer.close();
            } finally {
                if (numConsumers.decrementAndGet() == 0 && out != null) {
                    out.getFileReference().delete();
                }
            }
        }
    }
}
