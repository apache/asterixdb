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
 * Represents the write interface of a {@link ChannelControlBlock}.
 * 
 * @author vinayakb
 */
public interface IChannelWriteInterface {
    /**
     * Set the callback interface that must be invoked when a full buffer has been emptied by
     * writing the data to the remote end.
     * 
     * @param emptyBufferAcceptor
     *            - the empty buffer acceptor.
     */
    public void setEmptyBufferAcceptor(IBufferAcceptor emptyBufferAcceptor);

    /**
     * Get the full buffer acceptor that accepts buffers filled with data that need to be written
     * to the remote end.
     * 
     * @return the full buffer acceptor.
     */
    public ICloseableBufferAcceptor getFullBufferAcceptor();

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