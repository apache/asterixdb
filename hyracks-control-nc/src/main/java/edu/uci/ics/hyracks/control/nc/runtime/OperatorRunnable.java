/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.nc.runtime;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class OperatorRunnable implements Runnable {
    private final IOperatorNodePushable opNode;
    private IFrameReader reader;
    private ByteBuffer buffer;
    private volatile boolean abort;

    public OperatorRunnable(IHyracksContext ctx, IOperatorNodePushable opNode) {
        this.opNode = opNode;
        buffer = ctx.getResourceManager().allocateFrame();
    }

    public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        opNode.setOutputFrameWriter(index, writer, recordDesc);
    }

    public void setFrameReader(IFrameReader reader) {
        this.reader = reader;
    }

    public void abort() {
        abort = true;
    }

    @Override
    public void run() {
        try {
            opNode.initialize();
            if (reader != null) {
                IFrameWriter writer = opNode.getInputFrameWriter(0);
                writer.open();
                reader.open();
                while (readFrame()) {
                    if (abort) {
                        break;
                    }
                    buffer.flip();
                    writer.nextFrame(buffer);
                    buffer.compact();
                }
                reader.close();
                writer.close();
            }
            opNode.deinitialize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean readFrame() throws HyracksDataException {
        return reader.nextFrame(buffer);
    }

    @Override
    public String toString() {
        return "OperatorRunnable[" + opNode.getDisplayName() + "]";
    }
}