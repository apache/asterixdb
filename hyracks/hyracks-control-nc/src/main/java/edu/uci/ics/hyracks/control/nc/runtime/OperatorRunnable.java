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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class OperatorRunnable implements Runnable {
    private final IHyracksStageletContext ctx;
    private final IOperatorNodePushable opNode;
    private final int nInputs;
    private final Executor executor;
    private IFrameReader[] readers;
    private boolean abort;
    private Set<Thread> opThreads;

    public OperatorRunnable(IHyracksStageletContext ctx, IOperatorNodePushable opNode, int nInputs, Executor executor) {
        this.ctx = ctx;
        this.opNode = opNode;
        this.nInputs = nInputs;
        this.executor = executor;
        readers = new IFrameReader[nInputs];
        opThreads = new HashSet<Thread>();
    }

    public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        opNode.setOutputFrameWriter(index, writer, recordDesc);
    }

    public void setFrameReader(int inputIdx, IFrameReader reader) {
        this.readers[inputIdx] = reader;
    }

    public synchronized void abort() {
        if (abort) {
            return;
        }
        abort = true;
        for (Thread t : opThreads) {
            t.interrupt();
        }
    }

    private synchronized boolean aborted() {
        return abort;
    }

    private synchronized void addOperatorThread(Thread t) {
        if (abort) {
            t.interrupt();
            return;
        }
        opThreads.add(t);
    }

    private synchronized void removeOperatorThread(Thread t) {
        opThreads.add(t);
    }

    @Override
    public void run() {
        try {
            opNode.initialize();
            if (nInputs > 0) {
                final Semaphore sem = new Semaphore(nInputs - 1);
                for (int i = 1; i < nInputs; ++i) {
                    final IFrameReader reader = readers[i];
                    final IFrameWriter writer = opNode.getInputFrameWriter(i);
                    sem.acquire();
                    executor.execute(new Runnable() {
                        public void run() {
                            try {
                                if (!aborted()) {
                                    pushFrames(reader, writer);
                                }
                            } catch (HyracksDataException e) {
                            } finally {
                                sem.release();
                            }
                        }
                    });
                }
                try {
                    pushFrames(readers[0], opNode.getInputFrameWriter(0));
                } finally {
                    sem.acquire(nInputs - 1);
                }
            }
            opNode.deinitialize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void pushFrames(IFrameReader reader, IFrameWriter writer) throws HyracksDataException {
        addOperatorThread(Thread.currentThread());
        try {
            ByteBuffer buffer = ctx.allocateFrame();
            writer.open();
            reader.open();
            while (reader.nextFrame(buffer)) {
                if (aborted()) {
                    break;
                }
                buffer.flip();
                writer.nextFrame(buffer);
                buffer.compact();
            }
            reader.close();
            writer.close();
        } finally {
            removeOperatorThread(Thread.currentThread());
            if (Thread.interrupted()) {
                throw new HyracksDataException("Thread interrupted");
            }
        }
    }

    @Override
    public String toString() {
        return "OperatorRunnable[" + opNode.getDisplayName() + "]";
    }
}