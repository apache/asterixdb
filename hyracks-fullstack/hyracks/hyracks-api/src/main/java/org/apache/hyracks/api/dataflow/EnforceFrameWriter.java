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
package org.apache.hyracks.api.dataflow;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class EnforceFrameWriter implements IFrameWriter {

    // The downstream data consumer of this writer.
    private final IFrameWriter writer;

    // A flag that indicates whether the data consumer of this writer has failed.
    private boolean downstreamFailed = false;

    // A flag that indicates whether the the data producer of this writer has called fail() for this writer.
    // There could be two cases:
    // CASE 1: the downstream of this writer fails and the exception is propagated to the source operator, which
    //         cascades to the fail() of this writer;
    // CASE 2: the failure happens in the upstream of this writer and the source operator cascades to the fail()
    //         of this writer.
    private boolean failCalledByUpstream = false;

    // A flag that indicates whether the downstream of this writer is open.
    private boolean downstreamOpen = false;

    protected EnforceFrameWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    @Override
    public final void open() throws HyracksDataException {
        try {
            if (downstreamOpen) {
                throw HyracksDataException.create(ErrorCode.OPEN_ON_OPEN_WRITER);
            }
            if (downstreamFailed || failCalledByUpstream) {
                throw HyracksDataException.create(ErrorCode.OPEN_ON_FAILED_WRITER);
            }
            writer.open();
            downstreamOpen = true;
        } catch (Throwable th) {
            downstreamFailed = true;
            throw th;
        }
    }

    @Override
    public final void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (!downstreamOpen) {
            throw HyracksDataException.create(ErrorCode.NEXT_FRAME_ON_CLOSED_WRITER);
        }
        if (downstreamFailed || failCalledByUpstream) {
            throw HyracksDataException.create(ErrorCode.NEXT_FRAME_ON_FAILED_WRITER);
        }
        try {
            writer.nextFrame(buffer);
        } catch (Throwable th) {
            downstreamFailed = true;
            throw th;
        }
    }

    @Override
    public final void flush() throws HyracksDataException {
        if (!downstreamOpen) {
            throw HyracksDataException.create(ErrorCode.FLUSH_ON_CLOSED_WRITER);
        }
        if (downstreamFailed || failCalledByUpstream) {
            throw HyracksDataException.create(ErrorCode.FLUSH_ON_FAILED_WRITER);
        }
        try {
            writer.flush();
        } catch (Throwable th) {
            downstreamFailed = true;
            throw th;
        }
    }

    @Override
    public final void fail() throws HyracksDataException {
        writer.fail();
        if (failCalledByUpstream) {
            throw HyracksDataException.create(ErrorCode.FAIL_ON_FAILED_WRITER);
        }
        failCalledByUpstream = true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (downstreamFailed && !failCalledByUpstream) {
            throw HyracksDataException.create(ErrorCode.MISSED_FAIL_CALL);
        }
        writer.close();
        downstreamOpen = false;
    }

    public static IFrameWriter enforce(IFrameWriter writer) {
        return writer instanceof EnforceFrameWriter ? writer : new EnforceFrameWriter(writer);
    }
}
