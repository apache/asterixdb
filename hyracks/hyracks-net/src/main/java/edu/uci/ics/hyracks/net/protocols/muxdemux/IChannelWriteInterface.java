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
package edu.uci.ics.hyracks.net.protocols.muxdemux;

import edu.uci.ics.hyracks.net.buffers.IBufferAcceptor;
import edu.uci.ics.hyracks.net.buffers.ICloseableBufferAcceptor;

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
}