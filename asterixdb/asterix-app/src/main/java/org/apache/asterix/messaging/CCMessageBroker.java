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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.ICcIdentifiedMessage;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.messaging.api.INcResponse;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.messages.IMessage;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CCMessageBroker implements ICCMessageBroker {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ClusterControllerService ccs;
    private final Map<Long, MutablePair<MutableInt, MutablePair<ResponseState, Object>>> handles =
            new ConcurrentHashMap<>();
    private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong(0);
    private static final Object UNINITIALIZED = new Object();

    public CCMessageBroker(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public void receivedMessage(IMessage message, String nodeId) throws Exception {
        ICcAddressedMessage msg = (ICcAddressedMessage) message;
        IMessage.logMessage(LOGGER, message);
        ICcApplicationContext appCtx = (ICcApplicationContext) ccs.getApplicationContext();
        msg.handle(appCtx);
    }

    @Override
    public void sendApplicationMessageToNC(INcAddressedMessage msg, String nodeId) throws Exception {
        INodeManager nodeManager = ccs.getNodeManager();
        NodeControllerState state = nodeManager.getNodeControllerState(nodeId);
        if (msg instanceof ICcIdentifiedMessage) {
            ((ICcIdentifiedMessage) msg).setCcId(ccs.getCcId());
        }
        if (state != null) {
            state.getNodeController().sendApplicationMessageToNC(JavaSerializationUtils.serialize(msg), null, nodeId);
        } else {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Couldn't send message to unregistered node (" + nodeId + ")");
            }
        }
    }

    @Override
    public long newRequestId() {
        return REQUEST_ID_GENERATOR.incrementAndGet();
    }

    @Override
    public Object sendSyncRequestToNCs(long reqId, List<String> ncs, List<? extends INcAddressedMessage> requests,
            long timeout) throws Exception {
        MutableInt numRequired = new MutableInt(0);
        MutablePair<MutableInt, MutablePair<ResponseState, Object>> pair =
                MutablePair.of(numRequired, MutablePair.of(ResponseState.UNINITIALIZED, UNINITIALIZED));
        pair.getKey().setValue(ncs.size());
        handles.put(reqId, pair);
        try {
            synchronized (pair) {
                for (int i = 0; i < ncs.size(); i++) {
                    String nc = ncs.get(i);
                    INcAddressedMessage message = requests.get(i);
                    if (!(message instanceof ICcIdentifiedMessage)) {
                        throw new IllegalStateException("sync request message not cc identified: " + message);
                    }
                    sendApplicationMessageToNC(message, nc);
                }
                long time = System.currentTimeMillis();
                while (pair.getLeft().getValue() > 0) {
                    try {
                        pair.wait(timeout);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw HyracksDataException.create(e);
                    }
                    if (System.currentTimeMillis() - time > timeout && pair.getLeft().getValue() > 0) {
                        throw new RuntimeDataException(ErrorCode.NC_REQUEST_TIMEOUT, timeout / 1000);
                    }
                }
            }
            MutablePair<ResponseState, Object> right = pair.getRight();
            switch (right.getKey()) {
                case FAILURE:
                    throw HyracksDataException.create((Throwable) right.getValue());
                case SUCCESS:
                    return right.getRight();
                default:
                    throw new RuntimeDataException(ErrorCode.COMPILATION_ILLEGAL_STATE, String.valueOf(right.getKey()));
            }
        } finally {
            handles.remove(reqId);
        }
    }

    @Override
    public void respond(Long reqId, INcResponse response) {
        Pair<MutableInt, MutablePair<ResponseState, Object>> pair = handles.get(reqId);
        if (pair != null) {
            synchronized (pair) {
                try {
                    MutablePair<ResponseState, Object> result = pair.getValue();
                    switch (result.getKey()) {
                        case SUCCESS:
                        case UNINITIALIZED:
                            response.setResult(result);
                            break;
                        default:
                            break;
                    }
                } finally {
                    // Decrement the response counter
                    MutableInt remainingResponses = pair.getKey();
                    remainingResponses.setValue(remainingResponses.getValue() - 1);
                    pair.notifyAll();
                }
            }
        }
    }
}
