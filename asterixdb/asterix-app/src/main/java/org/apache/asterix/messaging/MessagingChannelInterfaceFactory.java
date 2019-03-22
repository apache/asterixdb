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
import java.nio.ByteBuffer;

import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.asterix.common.memory.FrameAction;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.hyracks.api.comm.IBufferAcceptor;
import org.apache.hyracks.api.comm.IBufferFactory;
import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.comm.IChannelReadInterface;
import org.apache.hyracks.api.comm.IChannelWriteInterface;
import org.apache.hyracks.api.comm.ICloseableBufferAcceptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MessagingChannelInterfaceFactory implements IChannelInterfaceFactory {

    private static final Logger LOGGER = LogManager.getLogger();

    private final NCMessageBroker messageBroker;
    private final ConcurrentFramePool messagingFramePool;
    /* A single buffer factory used by all messaging channels */
    private final IBufferFactory appMessagingBufferFactor = new AppMessagingBufferFactory();
    private final int msgFrameSize;
    private final int channelFrameCount;

    public MessagingChannelInterfaceFactory(NCMessageBroker messageBroker, MessagingProperties messagingProperties) {
        this.messageBroker = messageBroker;
        messagingFramePool = messageBroker.getMessagingFramePool();
        msgFrameSize = messagingProperties.getFrameSize();
        channelFrameCount = messagingProperties.getFrameCount();
    }

    @Override
    public IChannelReadInterface createReadInterface(IChannelControlBlock ccb) {
        AppMessagingEmptyBufferAcceptor readEmptyBufferAcceptor = new AppMessagingEmptyBufferAcceptor();
        MessagingChannelReadInterface readInterface = new MessagingChannelReadInterface(readEmptyBufferAcceptor);
        readInterface.setBufferFactory(appMessagingBufferFactor, channelFrameCount, msgFrameSize);
        readInterface.setFullBufferAcceptor(new AppMessagingReadFullBufferAcceptor(readEmptyBufferAcceptor));
        return readInterface;
    }

    @Override
    public IChannelWriteInterface createWriteInterface(IChannelControlBlock ccb) {
        MessagingChannelWriteInterface writeInterface = new MessagingChannelWriteInterface(ccb);
        writeInterface.setBufferFactory(appMessagingBufferFactor, channelFrameCount, msgFrameSize);
        writeInterface.setEmptyBufferAcceptor(new AppMessagingEmptyBufferAcceptor());
        return writeInterface;
    }

    /**
     * A buffer factory based on {@link ConcurrentFramePool}. Used
     * for messaging channels buffers.
     */
    private final class AppMessagingBufferFactory implements IBufferFactory {
        private final FrameAction frameAction = new FrameAction();

        @Override
        public ByteBuffer createBuffer() throws HyracksDataException {
            ByteBuffer buffer = messagingFramePool.get();
            if (buffer == null) {
                try {
                    messagingFramePool.subscribe(frameAction);
                    buffer = frameAction.retrieve();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return buffer;
        }
    }

    /**
     * A buffer acceptor that receives the read buffers containing messages from
     * other nodes.
     */
    private class AppMessagingReadFullBufferAcceptor implements ICloseableBufferAcceptor {
        private final IBufferAcceptor recycle;

        private AppMessagingReadFullBufferAcceptor(IBufferAcceptor recycle) {
            this.recycle = recycle;
        }

        @Override
        public void accept(ByteBuffer buffer) {
            try {
                INcAddressedMessage receivedMsg =
                        (INcAddressedMessage) JavaSerializationUtils.deserialize(buffer.array());
                // Queue the received message and free the network IO thread
                messageBroker.queueReceivedMessage(receivedMsg);
            } catch (ClassNotFoundException | IOException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.log(Level.WARN, e.getMessage(), e);
                }
            } finally {
                recycle.accept(buffer);
            }
        }

        @Override
        public void close() {
            // Nothing to close
        }

        @Override
        public void error(int ecode) {
            // Errors are handled via messages
        }
    }

    /**
     * Empty buffer acceptor used to return the used buffers in app messaging
     * to the buffer pool.
     */
    private class AppMessagingEmptyBufferAcceptor implements IBufferAcceptor {

        @Override
        public void accept(ByteBuffer buffer) {
            try {
                messagingFramePool.release(buffer);
            } catch (HyracksDataException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.log(Level.WARN, e.getMessage(), e);
                }
            }
        }
    }
}
