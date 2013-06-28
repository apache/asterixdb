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
}