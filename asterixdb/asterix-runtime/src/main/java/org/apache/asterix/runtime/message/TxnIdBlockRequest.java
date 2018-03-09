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
package org.apache.asterix.runtime.message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TxnIdBlockRequest implements ICcAddressedMessage {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int BLOCK_SIZE = 512;
    private static final long serialVersionUID = 1L;

    private static BlockingQueue<TxnIdBlockResponse> blockQueue = new LinkedBlockingQueue<>();
    private final String nodeId;
    private final int blockSizeRequested;

    public TxnIdBlockRequest(String nodeId, int blockSizeRequested) {
        this.nodeId = nodeId;
        this.blockSizeRequested = blockSizeRequested;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException {
        try {
            ICCMessageBroker broker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
            long startingId = appCtx.getTxnIdFactory().getIdBlock(blockSizeRequested);
            TxnIdBlockResponse response = new TxnIdBlockResponse(startingId, blockSizeRequested);
            broker.sendApplicationMessageToNC(response, nodeId);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    public static Block send(INcApplicationContext ncs) throws HyracksDataException {
        TxnIdBlockRequest blockRequestMessage = new TxnIdBlockRequest(ncs.getServiceContext().getNodeId(), BLOCK_SIZE);
        try {
            ((INCMessageBroker) ncs.getServiceContext().getMessageBroker()).sendMessageToPrimaryCC(blockRequestMessage);
            TxnIdBlockResponse response = blockQueue.take();
            return new Block(response.getStartingId(), response.getBlockSize());
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Unable to request transaction id block", e);
            throw HyracksDataException.create(e);
        }
    }

    static void addResponse(TxnIdBlockResponse response) {
        blockQueue.offer(response);
    }

    @Override
    public String toString() {
        return TxnIdBlockRequest.class.getSimpleName();
    }

    public static class Block {

        private final long startingId;
        private final int blockSize;

        public Block(long startingId, int blockSize) {
            this.startingId = startingId;
            this.blockSize = blockSize;
        }

        public long getStartingId() {
            return startingId;
        }

        public int getBlockSize() {
            return blockSize;
        }
    }
}
