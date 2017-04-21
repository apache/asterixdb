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
package org.apache.asterix.app.replication.message;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class TakeoverMetadataNodeRequestMessage implements INCLifecycleMessage, INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(TakeoverMetadataNodeRequestMessage.class.getName());

    @Override
    public void handle(INcApplicationContext appContext) throws HyracksDataException, InterruptedException {
        INCMessageBroker broker = (INCMessageBroker) appContext.getServiceContext().getMessageBroker();
        HyracksDataException hde = null;
        try {
            appContext.initializeMetadata(false);
            appContext.exportMetadataNodeStub();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed taking over metadata", e);
            hde = HyracksDataException.create(e);
        } finally {
            TakeoverMetadataNodeResponseMessage reponse =
                    new TakeoverMetadataNodeResponseMessage(appContext.getTransactionSubsystem().getId());
            try {
                broker.sendMessageToCC(reponse);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed taking over metadata", e);
                hde = HyracksDataException.suppress(hde, e);
            }
        }
        if (hde != null) {
            throw hde;
        }
    }

    @Override
    public String toString() {
        return TakeoverMetadataNodeRequestMessage.class.getSimpleName();
    }

    @Override
    public MessageType getType() {
        return MessageType.TAKEOVER_METADATA_NODE_REQUEST;
    }
}