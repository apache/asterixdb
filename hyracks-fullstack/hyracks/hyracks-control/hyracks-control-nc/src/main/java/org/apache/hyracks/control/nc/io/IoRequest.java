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
package org.apache.hyracks.control.nc.io;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IAsyncRequest;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.util.InterruptibleAction;

public class IoRequest implements IAsyncRequest, InterruptibleAction {

    public enum State {
        INITIAL,
        READ_REQUESTED,
        WRITE_REQUESTED,
        OPERATION_FAILED,
        OPERATION_SUCCEEDED
    }

    private final IOManager ioManager;
    private final BlockingQueue<IoRequest> submittedRequests;
    private final BlockingQueue<IoRequest> freeRequests;
    private State state;
    private IFileHandle fHandle;
    private long offset;
    private ByteBuffer data;
    private ByteBuffer[] dataArray;
    private Throwable failure;
    private int read;
    private int write;
    private long writes;

    public IoRequest(IOManager ioManager, BlockingQueue<IoRequest> submittedRequests,
            BlockingQueue<IoRequest> freeRequests) {
        this.ioManager = ioManager;
        this.submittedRequests = submittedRequests;
        this.freeRequests = freeRequests;
        reset();
    }

    public void reset() {
        state = State.INITIAL;
        fHandle = null;
        data = null;
        dataArray = null;
        failure = null;
    }

    public void read(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        if (state != State.INITIAL) {
            throw new IllegalStateException("Can't request a read operation through a " + state + " request");
        }
        state = State.READ_REQUESTED;
        this.fHandle = fHandle;
        this.offset = offset;
        this.data = data;
        queue();
    }

    public void write(IFileHandle fHandle, long offset, ByteBuffer[] dataArray) throws HyracksDataException {
        if (state != State.INITIAL) {
            throw new IllegalStateException("Can't request a write operation through a " + state + " request");
        }
        state = State.WRITE_REQUESTED;
        this.fHandle = fHandle;
        this.offset = offset;
        this.dataArray = dataArray;
        queue();
    }

    public void write(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        if (state != State.INITIAL) {
            throw new IllegalStateException("Can't request a write operation through a " + state + " request");
        }
        state = State.WRITE_REQUESTED;
        this.fHandle = fHandle;
        this.offset = offset;
        this.data = data;
        queue();
    }

    private void queue() throws HyracksDataException {
        try {
            submittedRequests.put(this);
        } catch (InterruptedException e) { // NOSONAR: The call below will re-interrupt
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void await() throws InterruptedException {
        synchronized (this) {
            while (state != State.OPERATION_FAILED && state != State.OPERATION_SUCCEEDED) {
                wait();
            }
        }
    }

    synchronized void handle() {
        try {
            if (state == State.READ_REQUESTED) {
                read = ioManager.doSyncRead(fHandle, offset, data);
            } else if (state == State.WRITE_REQUESTED) {
                if (data != null) {
                    // single buffer
                    write = ioManager.doSyncWrite(fHandle, offset, data);
                } else {
                    // multiple buffers
                    writes = ioManager.doSyncWrite(fHandle, offset, dataArray);
                }
            } else {
                throw new IllegalStateException("IO Request with state = " + state);
            }
            state = State.OPERATION_SUCCEEDED;
        } catch (Throwable th) { // NOSONAR: This method must never throw anything
            state = State.OPERATION_FAILED;
            failure = th;
        } finally {
            notifyAll();
        }
    }

    public State getState() {
        return state;
    }

    @SuppressWarnings("squid:S899") // Offer failing means we're over capacity and this should be garbage collected
    void recycle() {
        reset();
        freeRequests.offer(this);
    }

    public int getRead() {
        return read;
    }

    public int getWrite() {
        return write;
    }

    public long getWrites() {
        return writes;
    }

    @Override
    public void run() throws InterruptedException {
        await();
    }

    public HyracksDataException getFailure() {
        return HyracksDataException.create(failure);
    }
}
