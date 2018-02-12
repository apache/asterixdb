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

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.CcIdentifiedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetadataNodeRequestMessage extends CcIdentifiedMessage
        implements INCLifecycleMessage, INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final boolean export;
    private final int partitionId;

    public MetadataNodeRequestMessage(boolean export, int partitionId) {
        this.export = export;
        this.partitionId = partitionId;
    }

    @Override
    public void handle(INcApplicationContext appContext) throws HyracksDataException, InterruptedException {
        INCMessageBroker broker = (INCMessageBroker) appContext.getServiceContext().getMessageBroker();
        Throwable hde = null;
        try {
            if (export) {
                appContext.initializeMetadata(false, partitionId);
                appContext.exportMetadataNodeStub();
                appContext.bindMetadataNodeStub(getCcId());
            } else {
                appContext.unexportMetadataNodeStub();
            }
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Failed taking over metadata", e);
            hde = HyracksDataException.create(e);
        } finally {
            MetadataNodeResponseMessage reponse =
                    new MetadataNodeResponseMessage(appContext.getTransactionSubsystem().getId(), export);
            try {
                broker.sendMessageToCC(getCcId(), reponse);
            } catch (Exception e) {
                LOGGER.log(Level.ERROR, "Failed taking over metadata", e);
                hde = ExceptionUtils.suppress(hde, e);
            }
        }
        if (hde != null) {
            throw HyracksDataException.create(hde);
        }
    }

    @Override
    public String toString() {
        return MetadataNodeRequestMessage.class.getSimpleName();
    }

    @Override
    public MessageType getType() {
        return MessageType.METADATA_NODE_REQUEST;
    }
}