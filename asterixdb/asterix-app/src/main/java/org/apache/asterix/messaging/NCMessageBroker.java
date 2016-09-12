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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.asterix.common.messaging.api.IApplicationMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.messages.IMessage;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;

public class NCMessageBroker implements INCMessageBroker {
    private static final Logger LOGGER = Logger.getLogger(NCMessageBroker.class.getName());

    private final NodeControllerService ncs;
    private final IAsterixAppRuntimeContext appContext;
    private final LinkedBlockingQueue<IApplicationMessage> receivedMsgsQ;
    private final ConcurrentFramePool messagingFramePool;
    private final int maxMsgSize;

    public NCMessageBroker(NodeControllerService ncs, MessagingProperties messagingProperties) {
        this.ncs = ncs;
        appContext = (IAsterixAppRuntimeContext) ncs.getApplicationContext().getApplicationObject();
        maxMsgSize = messagingProperties.getFrameSize();
        int messagingMemoryBudget = messagingProperties.getFrameSize() * messagingProperties.getFrameCount();
        messagingFramePool = new ConcurrentFramePool(ncs.getId(), messagingMemoryBudget,
                messagingProperties.getFrameSize());
        receivedMsgsQ = new LinkedBlockingQueue<>();
        MessageDeliveryService msgDeliverySvc = new MessageDeliveryService();
        appContext.getThreadExecutor().execute(msgDeliverySvc);
    }

    @Override
    public void sendMessageToCC(IApplicationMessage message) throws Exception {
        ncs.sendApplicationMessageToCC(JavaSerializationUtils.serialize(message), null);
    }

    @Override
    public void sendMessageToNC(String nodeId, IApplicationMessage message)
            throws Exception {
        IChannelControlBlock messagingChannel = ncs.getMessagingNetworkManager().getMessagingChannel(nodeId);
        sendMessageToChannel(messagingChannel, message);
    }

    @Override
    public void queueReceivedMessage(IApplicationMessage msg) {
        receivedMsgsQ.offer(msg);
    }

    @Override
    public void receivedMessage(IMessage message, String nodeId) throws Exception {
        IApplicationMessage absMessage = (IApplicationMessage) message;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Received message: " + absMessage);
        }
        absMessage.handle(ncs);
    }

    public ConcurrentFramePool getMessagingFramePool() {
        return messagingFramePool;
    }

    private void sendMessageToChannel(IChannelControlBlock ccb, IApplicationMessage msg) throws IOException {
        byte[] serializedMsg = JavaSerializationUtils.serialize(msg);
        if (serializedMsg.length > maxMsgSize) {
            throw new HyracksDataException("Message exceded maximum size");
        }
        // Prepare the message buffer
        ByteBuffer msgBuffer = messagingFramePool.get();
        if (msgBuffer == null) {
            throw new HyracksDataException("Could not get an empty buffer");
        }
        msgBuffer.clear();
        msgBuffer.put(serializedMsg);
        msgBuffer.flip();
        // Give the buffer to the channel write interface for writing
        ccb.getWriteInterface().getFullBufferAcceptor().accept(msgBuffer);
    }

    private class MessageDeliveryService implements Runnable {
        /*
         * TODO Currently this thread is not stopped when it is interrupted because
         * NC2NC messaging might be used during nodes shutdown coordination and the
         * JVM shutdown hook might interrupt while it is still needed. If NC2NC
         * messaging wont be used during shutdown, then this thread needs to be
         * gracefully stopped using a POSION_PILL or when interrupted during the
         * shutdown.
         */
        @Override
        public void run() {
            while (true) {
                IApplicationMessage msg = null;
                try {
                    msg = receivedMsgsQ.take();
                    //TODO add nodeId to IApplicationMessage and pass it
                    receivedMessage(msg, null);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING) && msg != null) {
                        LOGGER.log(Level.WARNING, "Could not process message : "
                                + msg, e);
                    } else {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.log(Level.WARNING, "Could not process message", e);
                        }
                    }
                }
            }
        }
    }
}
