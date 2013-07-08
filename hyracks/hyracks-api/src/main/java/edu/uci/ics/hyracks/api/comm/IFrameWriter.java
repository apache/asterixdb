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
package edu.uci.ics.hyracks.api.comm;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * {@link IFrameWriter} is the interface implemented by a stream consumer. An
 * {@link IFrameWriter} could be in one of the following states:
 * <ul>
 * <li>INITIAL</li>
 * <li>OPENED</li>
 * <li>CLOSED</li>
 * <li>FAILED</li>
 * </ul>
 * A producer follows the following protocol when using an {@link IFrameWriter}.
 * Initially, the {@link IFrameWriter} is in the INITIAL state.
 * The first valid call to an {@link IFrameWriter} is always the
 * {@link IFrameWriter#open()}. This call provides the opportunity for the
 * {@link IFrameWriter} implementation to allocate any resources for its
 * processing. Once this call returns, the {@link IFrameWriter} is in the OPENED
 * state. If an error occurs
 * during the {@link IFrameWriter#open()} call, a {@link HyracksDataException}
 * is thrown and it stays in the INITIAL state.
 * While the {@link IFrameWriter} is in the OPENED state, the producer can call
 * one of:
 * <ul>
 * <li> {@link IFrameWriter#close()} to give up any resources owned by the
 * {@link IFrameWriter} and enter the CLOSED state.</li>
 * <li> {@link IFrameWriter#nextFrame(ByteBuffer)} to provide data to the
 * {@link IFrameWriter}. The call returns normally on success and the
 * {@link IFrameWriter} remains in the OPENED state. On failure, the call throws
 * a {@link HyracksDataException}, and the {@link IFrameWriter} enters the ERROR
 * state.</li>
 * <li> {@link IFrameWriter#fail()} to indicate that stream is to be aborted. The
 * {@link IFrameWriter} enters the FAILED state.</li>
 * </ul>
 * In the FAILED state, the only call allowed is the
 * {@link IFrameWriter#close()} to move the {@link IFrameWriter} into the CLOSED
 * state and give up all resources.
 * No calls are allowed when the {@link IFrameWriter} is in the CLOSED state.
 * 
 * Note: If the call to {@link IFrameWriter#open()} failed, the
 * {@link IFrameWriter#close()} is not called by the producer. So an exceptional
 * return from the {@link IFrameWriter#open()} call must clean up all partially
 * allocated resources.
 * 
 * @author vinayakb
 */
public interface IFrameWriter {
    /**
     * First call to allocate any resources.
     */
    public void open() throws HyracksDataException;

    /**
     * Provide data to the stream of this {@link IFrameWriter}.
     * 
     * @param buffer
     *            - Buffer containing data.
     * @throws HyracksDataException
     */
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException;

    /**
     * Indicate that a failure was encountered and the current stream is to be
     * aborted.
     * 
     * @throws HyracksDataException
     */
    public void fail() throws HyracksDataException;

    /**
     * Close this {@link IFrameWriter} and give up all resources.
     * 
     * @throws HyracksDataException
     */
    public void close() throws HyracksDataException;
}