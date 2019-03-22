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
package org.apache.asterix.messaging;

import java.io.IOException;

import org.apache.hyracks.api.comm.IBufferAcceptor;
import org.apache.hyracks.api.exceptions.NetException;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.net.protocols.muxdemux.AbstractChannelReadInterface;

public class MessagingChannelReadInterface extends AbstractChannelReadInterface {

    MessagingChannelReadInterface(IBufferAcceptor emptyBufferAcceptor) {
        this.emptyBufferAcceptor = emptyBufferAcceptor;
    }

    @Override
    public int read(ISocketChannel sc, int size) throws IOException, NetException {
        while (true) {
            if (size <= 0) {
                return size;
            }
            if (currentReadBuffer == null) {
                currentReadBuffer = bufferFactory.createBuffer();
            }
            int rSize = Math.min(size, currentReadBuffer.remaining());
            if (rSize > 0) {
                currentReadBuffer.limit(currentReadBuffer.position() + rSize);
                int len;
                len = sc.read(currentReadBuffer);
                if (len < 0) {
                    throw new NetException("Socket Closed");
                }
                size -= len;
                if (len < rSize) {
                    return size;
                }
            } else {
                return size;
            }

            if (currentReadBuffer.remaining() <= 0) {
                flush();
            }
        }
    }
}
