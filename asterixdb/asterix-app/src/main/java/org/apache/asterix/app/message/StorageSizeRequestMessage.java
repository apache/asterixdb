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
package org.apache.asterix.app.message;

import java.util.function.Predicate;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.CcIdentifiedMessage;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StorageSizeRequestMessage extends CcIdentifiedMessage implements INcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final long reqId;
    private final String database;
    private final String dataverse;
    private final String collection;
    private final String index;

    public StorageSizeRequestMessage(long reqId, String database, String dataverse, String collection, String index) {
        this.reqId = reqId;
        this.database = database;
        this.dataverse = dataverse;
        this.collection = collection;
        this.index = index;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException {
        try {
            Predicate<String> predicate = getPredicate();
            IIOManager ioManager = appCtx.getPersistenceIoManager();
            StorageSizeResponseMessage response =
                    new StorageSizeResponseMessage(reqId, ioManager.getSize(predicate), null);
            respond(appCtx, response);
        } catch (Exception e) {
            LOGGER.info("failed to get collection size", e);
            StorageSizeResponseMessage response = new StorageSizeResponseMessage(reqId, 0, e);
            respond(appCtx, response);
        }
    }

    private Predicate<String> getPredicate() {
        return path -> {
            ResourceReference resourceReference = ResourceReference.of(path);
            if (resourceReference.getDatabase().equals(database)
                    && resourceReference.getDataverse().getCanonicalForm().equals(dataverse)
                    && resourceReference.getDataset().equals(collection)) {
                if (index != null) {
                    return resourceReference.getIndex().equals(index);
                }
                return true;
            }
            return false;
        };
    }

    private void respond(INcApplicationContext appCtx, StorageSizeResponseMessage response)
            throws HyracksDataException {
        NCMessageBroker messageBroker = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        try {
            messageBroker.sendMessageToPrimaryCC(response);
        } catch (Exception e) {
            LOGGER.info("failed to send collection size to cc", e);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public boolean isWhispered() {
        return true;
    }
}
