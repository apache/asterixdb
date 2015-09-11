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

import org.apache.hyracks.net.buffers.IBufferAcceptor;
import org.apache.hyracks.net.buffers.ICloseableBufferAcceptor;

/**
 * Represents the read interface of a {@link ChannelControlBlock}.
 * 
 * @author vinayakb
 */
public interface IChannelReadInterface {
    /**
     * Set the callback that will be invoked by the network layer when a buffer has been
     * filled with data received from the remote side.
     * 
     * @param fullBufferAcceptor
     *            - the full buffer acceptor.
     */
    public void setFullBufferAcceptor(ICloseableBufferAcceptor fullBufferAcceptor);

    /**
     * Get the acceptor that collects empty buffers when the client has finished consuming
     * a previously full buffer.
     * 
     * @return the empty buffer acceptor.
     */
    public IBufferAcceptor getEmptyBufferAcceptor();

    /**
     * Set the buffer factory which is in charge of creating buffers if the request does not
     * make the number of allocated buffers goes beyond limit
     * 
     * @param bufferFactory
     *            - the buffer factory
     * @param limit
     *            - the limit of buffers
     * @param frameSize
     *            - the size of each buffer
     */
    public void setBufferFactory(IBufferFactory bufferFactory, int limit, int frameSize);
}