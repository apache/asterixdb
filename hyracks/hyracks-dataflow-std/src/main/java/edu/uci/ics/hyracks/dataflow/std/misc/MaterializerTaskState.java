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
package edu.uci.ics.hyracks.dataflow.std.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;

public class MaterializerTaskState extends AbstractStateObject {
    private RunFileWriter out;

    public MaterializerTaskState(JobId jobId, TaskId taskId) {
        super(jobId, taskId);
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {

    }

    @Override
    public void fromBytes(DataInput in) throws IOException {

    }

    public void open(IHyracksTaskContext ctx) throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                MaterializerTaskState.class.getSimpleName());
        out = new RunFileWriter(file, ctx.getIOManager());
        out.open();
    }

    public void close() throws HyracksDataException {
        out.close();
    }

    public void appendFrame(ByteBuffer buffer) throws HyracksDataException {
        out.nextFrame(buffer);
    }

    public void writeOut(IFrameWriter writer, ByteBuffer frame) throws HyracksDataException {
        RunFileReader in = out.createReader();
        writer.open();
        try {
            in.open();
            while (in.nextFrame(frame)) {
                frame.flip();
                writer.nextFrame(frame);
                frame.clear();
            }
            in.close();
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }
    
    public void deleteFile() {
        out.getFileReference().delete();
    }
}