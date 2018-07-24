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
package org.apache.hyracks.net.protocols.muxdemux;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IBufferAcceptor;
import org.apache.hyracks.api.comm.IBufferFactory;
import org.apache.hyracks.api.comm.IChannelReadInterface;
import org.apache.hyracks.api.comm.ICloseableBufferAcceptor;

public abstract class AbstractChannelReadInterface implements IChannelReadInterface {

    protected ICloseableBufferAcceptor fba;
    protected IBufferAcceptor emptyBufferAcceptor;
    protected ByteBuffer currentReadBuffer;
    protected IBufferFactory bufferFactory;
    protected volatile int credits;

    @Override
    public void flush() {
        if (currentReadBuffer != null) {
            currentReadBuffer.flip();
            fba.accept(currentReadBuffer);
            currentReadBuffer = null;
        }
    }

    @Override
    public void setFullBufferAcceptor(ICloseableBufferAcceptor fullBufferAcceptor) {
        fba = fullBufferAcceptor;
    }

    @Override
    public IBufferAcceptor getEmptyBufferAcceptor() {
        return emptyBufferAcceptor;
    }

    @Override
    public ICloseableBufferAcceptor getFullBufferAcceptor() {
        return fba;
    }

    @Override
    public int getCredits() {
        return credits;
    }

    @Override
    public void setReadCredits(int credits) {
        this.credits = credits;
    }

    @Override
    public IBufferFactory getBufferFactory() {
        return bufferFactory;
    }

    @Override
    public void setBufferFactory(IBufferFactory bufferFactory, int limit, int frameSize) {
        this.bufferFactory = bufferFactory;
    }
}
